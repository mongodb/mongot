package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/**
 * Covers which document a {@code first}/{@code last} bucket picks -- under a sort, under no sort,
 * and across a merge -- and what value it reads from that document's stored source.
 */
public class FirstLastAccumulatorTest extends AggregationTestSupport {

  private static final FieldPath COLOR_PATH = FieldPath.parse(COLOR_FIELD);
  private static final FieldPath PRICE_PATH = FieldPath.parse(PRICE_FIELD);

  private static final ResolvedTokenField COLOR_TOKEN = new ResolvedTokenField(COLOR_PATH);
  private static final SearchIndexCapabilities INDEX_CAPABILITIES = SearchIndexCapabilities.CURRENT;

  private static final ResolvedTokenField PRICE_INT64 = new ResolvedTokenField(PRICE_PATH);

  private static final String RATING_FIELD = "rating";
  private static final FieldPath RATING_PATH = FieldPath.parse(RATING_FIELD);
  private static final String RATING_LUCENE_FIELD =
      TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(RATING_PATH, Optional.empty());
  private static final ResolvedTokenField RATING_DOUBLE = new ResolvedTokenField(RATING_PATH);

  private static final String SIZE_FIELD = "size";
  private static final FieldPath SIZE_PATH = FieldPath.parse(SIZE_FIELD);
  private static final String SIZE_LUCENE_FIELD =
      TypeField.TOKEN.getLuceneFieldName(SIZE_PATH, Optional.empty());

  private static final String CREATED_FIELD = "created";
  private static final FieldPath CREATED_PATH = FieldPath.parse(CREATED_FIELD);
  private static final String CREATED_LUCENE_FIELD =
      TypeField.DATE_V2.getLuceneFieldName(CREATED_PATH, Optional.empty());
  private static final ResolvedTokenField CREATED_DATE = new ResolvedTokenField(CREATED_PATH);

  /** The sortable type backing each field these tests sort on. */
  private static final Map<FieldPath, TypeField> SORTABLE_TYPES =
      Map.of(
          PRICE_PATH,
          TypeField.NUMBER_INT64_V2,
          RATING_PATH,
          TypeField.NUMBER_DOUBLE_V2,
          CREATED_PATH,
          TypeField.DATE_V2);

  /** Sort spec on the int64 price field. */
  private static List<MongotSortField> priceSpec(UserFieldSortOptions options) {
    return List.of(new MongotSortField(PRICE_PATH, options));
  }

  /** Sort spec on the date field, to exercise inversion over a non-int64 sort component. */
  private static List<MongotSortField> createdSpec(UserFieldSortOptions options) {
    return List.of(new MongotSortField(CREATED_PATH, options));
  }

  /** Compound sort spec: price ascending, then rating ascending. */
  private static List<MongotSortField> priceThenRatingSpec() {
    return List.of(
        new MongotSortField(PRICE_PATH, UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(RATING_PATH, UserFieldSortOptions.DEFAULT_ASC));
  }

  /**
   * The Lucene sort for {@code spec}, built through the same factory the query layer uses. Empty
   * for an empty spec, standing in for an unsorted query.
   */
  private static Optional<Sort> luceneSort(List<MongotSortField> spec) {
    if (spec.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new Sort(
            spec.stream()
                .map(FirstLastAccumulatorTest::optimizedSortField)
                .toArray(SortField[]::new)));
  }

  /**
   * The inverted Lucene sort for {@code spec}, taken the way production does: invert each spec
   * field and rebuild, rather than turning the finished {@link Sort} around.
   */
  private static Optional<Sort> invertedLuceneSort(List<MongotSortField> spec) {
    return luceneSort(spec.stream().map(MongotSortField::invert).toList());
  }

  private static SortField optimizedSortField(MongotSortField sortField) {
    return LuceneSortFactory.createOptimizedSortField(
            sortField,
            ImmutableSet.of(SORTABLE_TYPES.get(sortField.field())),
            Optional.empty(),
            Optional.empty(),
            INDEX_CAPABILITIES,
            false)
        .orElseThrow();
  }

  /** Terms grouping on the city token field, using the production spec. */
  private static AggregationSpec byCity(List<AggregationSpec> children) {
    return new TermsGroupAggregationSpec(
        "by_city", FieldPath.parse(CITY_FIELD), children, new TermsArgs(true, TypeField.TOKEN, 10));
  }

  /** Terms grouping on a token field at a nested (non-root) level. */
  private static AggregationSpec nestedTerms(
      String name, FieldPath path, List<AggregationSpec> children) {
    return new TermsGroupAggregationSpec(
        name, path, children, new TermsArgs(false, TypeField.TOKEN, 10));
  }

  private AggregationContext contextWith(
      Map<FieldPath, ResolvedTokenField> fields, List<MongotSortField> sortSpec) {
    return contextWith(fields, sortSpec, 0);
  }

  private AggregationContext contextWith(
      Map<FieldPath, ResolvedTokenField> fields, List<MongotSortField> sortSpec, int partitionId) {
    AggregationFieldResolver resolver =
        new AggregationFieldResolver() {
          @Override
          public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
            throw new IllegalStateException("unexpected path: " + path);
          }

          @Override
          public ResolvedTokenField resolveToken(FieldPath path) {
            return Optional.ofNullable(fields.get(path))
                .orElseThrow(() -> new IllegalStateException("unexpected path: " + path));
          }
        };
    return new TestContext(
        this.reader,
        Optional.of(resolver),
        luceneSort(sortSpec),
        invertedLuceneSort(sortSpec),
        partitionId);
  }

  /** Groups by city with a {@code first} and a {@code last} of the color token. */
  private TopAggregationSpec firstAndLastColorByCity() {
    return new TopAggregationSpec(
        List.of(
            byCity(
                List.of(
                    new FirstAggregationSpec("first_color", COLOR_PATH),
                    new LastAggregationSpec("last_color", COLOR_PATH)))));
  }

  /** Groups by city with a {@code first} and a {@code last} of an arbitrary metric path. */
  private TopAggregationSpec firstAndLastMetricByCity(FieldPath path) {
    return new TopAggregationSpec(
        List.of(
            byCity(
                List.of(
                    new FirstAggregationSpec("first_metric", path),
                    new LastAggregationSpec("last_metric", path)))));
  }

  private static BsonValue metric(TermsAggregationResult.TermsBucket bucket, String name) {
    return bucket.getChildResults().get(name).toStandaloneBson();
  }

  private TermsAggregationResult runByCity(TopAggregationSpec spec, AggregationContext context)
      throws Exception {
    TopLevelAggregationResult top =
        (TopLevelAggregationResult) collectAll(spec, context).buildTopLevelResult();
    return (TermsAggregationResult) top.childResults().get(0);
  }

  @Test
  public void underAscendingSort_picksLowestAndHighestSortedDocuments() throws Exception {
    // Price order within NYC is green(5) < red(10) < blue(30), which is not document order.
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5),
            new TestDoc("LA", "black", 7)),
        Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
    assertEquals(new BsonString("black"), metric(bucket(byCity, "LA"), "first_color"));
    assertEquals(new BsonString("black"), metric(bucket(byCity, "LA"), "last_color"));
  }

  @Test
  public void underDescendingSort_firstAndLastSwap() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5)),
        Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_DESC)));

    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  @Test
  public void withoutSort_usesEncounteredOrder() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5)),
        Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(), contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), List.of()));

    assertEquals(new BsonString("red"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  @Test
  public void acrossSegments_winnerIsGlobalNotPerSegment() throws Exception {
    // One document per segment, so the winner must be chosen across segment boundaries.
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5)),
        1);

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  @Test
  public void numericField_decodesToInt64() throws Exception {
    buildIndex(List.of(new TestDoc("NYC", "red", 10), new TestDoc("NYC", "blue", 30)), 1);

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(byCity(List.of(new LastAggregationSpec("last_price", PRICE_PATH)))));

    TermsAggregationResult byCity =
        runByCity(
            spec,
            contextWith(
                Map.of(PRICE_PATH, PRICE_INT64), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(30L, metric(bucket(byCity, "NYC"), "last_price").asInt64().getValue());
  }

  @Test
  public void mergingSliceResults_keepsTheGlobalWinner() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5)),
        Integer.MAX_VALUE);

    AggregationContext context =
        contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC));

    // Two independent collections stand in for two search slices over the same reader.
    TermsAggregationResult left = runByCity(firstAndLastColorByCity(), context);
    TermsAggregationResult right = runByCity(firstAndLastColorByCity(), context);

    AggregationResult mergedFirst =
        merge(
            bucket(left, "NYC").getChildResults().get("first_color"),
            bucket(right, "NYC").getChildResults().get("first_color"));
    AggregationResult mergedLast =
        merge(
            bucket(left, "NYC").getChildResults().get("last_color"),
            bucket(right, "NYC").getChildResults().get("last_color"));

    assertEquals(new BsonString("green"), mergedFirst.toStandaloneBson());
    assertEquals(new BsonString("blue"), mergedLast.toStandaloneBson());
  }

  @Test
  public void winningDocumentMissingTheField_reportsNull() throws Exception {
    buildIndexWithOptionalColor();

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    // The cheapest NYC document has no color; the priciest does.
    assertEquals(BsonNull.VALUE, metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /**
   * Stored source keeps the value exactly as indexed, so an array-valued field is returned whole --
   * what MQL {@code $first} returns. A doc-values column could only ever hand back one entry.
   */
  @Test
  public void multiValuedField_readsTheWholeArray() throws Exception {
    writeDocuments(
        writer ->
            writer.accept(
                document(
                    "NYC",
                    Optional.of("red"),
                    10,
                    Optional.of(
                        new BsonDocument(
                            COLOR_FIELD,
                            new BsonArray(
                                List.of(new BsonString("red"), new BsonString("blue"))))))));

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(), contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), List.of()));

    assertEquals(
        new BsonArray(List.of(new BsonString("red"), new BsonString("blue"))),
        metric(bucket(byCity, "NYC"), "first_color"));
  }

  /**
   * A fanned-out document belongs to each bucket it lands in, so it competes in each -- as MQL does
   * after {@code $unwind}. Also pins the repeated read: it is collected twice, so its key is read
   * twice with an unchanged doc id.
   */
  @Test
  public void multiValuedAncestorGroup_competesInEveryBucket() throws Exception {
    writeDocuments(
        writer -> {
          // Shared by NYC and LA.
          Document shared = document("NYC", Optional.of("red"), 10, Optional.empty());
          shared.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef("LA")));
          writer.accept(shared);
          writer.accept(document("NYC", Optional.of("blue"), 20, Optional.empty()));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    // NYC holds both documents; LA holds only the shared one.
    assertEquals(new BsonString("red"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
    assertEquals(new BsonString("red"), metric(bucket(byCity, "LA"), "first_color"));
    assertEquals(new BsonString("red"), metric(bucket(byCity, "LA"), "last_color"));
  }

  /** A later sort field breaks ties in an earlier one. */
  @Test
  public void multiValuedAncestorGroup_withoutSort_competesInEveryBucket() throws Exception {
    writeDocuments(
        writer -> {
          Document shared = document("NYC", Optional.of("red"), 10, Optional.empty());
          shared.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef("LA")));
          writer.accept(shared);
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(), contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), List.of()));

    for (String key : bucketKeys(byCity)) {
      assertEquals(new BsonString("red"), metric(bucket(byCity, key), "first_color"));
    }
  }

  @Test
  public void storedSource_returnsNestedDocumentsAndArrays() throws Exception {
    FieldPath nested = FieldPath.parse("meta.tags");
    buildIndexWithStoredSource();

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                byCity(
                    List.of(
                        new FirstAggregationSpec("first_tags", nested),
                        new LastAggregationSpec("last_tags", nested)))));

    TermsAggregationResult byCity =
        runByCity(
            spec,
            contextWith(
                Map.of(nested, new ResolvedTokenField(nested)),
                priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    // Arrays round-trip through stored source, unlike the doc-values path.
    assertEquals(
        new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))),
        metric(bucket(byCity, "NYC"), "first_tags"));
    assertEquals(
        new BsonArray(List.of(new BsonInt32(9))), metric(bucket(byCity, "NYC"), "last_tags"));
  }

  @Test
  public void storedSource_pathAbsentFromDocument_reportsNull() throws Exception {
    FieldPath absent = FieldPath.parse("meta.missing");
    buildIndexWithStoredSource();

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(byCity(List.of(new FirstAggregationSpec("first_missing", absent)))));

    TermsAggregationResult byCity =
        runByCity(spec, contextWith(Map.of(absent, new ResolvedTokenField(absent)), List.of()));

    assertEquals(BsonNull.VALUE, metric(bucket(byCity, "NYC"), "first_missing"));
  }

  @Test
  public void tiedSortKeys_breakTowardEncounteredOrder() throws Exception {
    // Both NYC docs share a price, so only the doc-id tiebreak can separate them.
    buildIndex(
        List.of(new TestDoc("NYC", "red", 10), new TestDoc("NYC", "blue", 10)), Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(new BsonString("red"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  @Test
  public void compoundSort_laterFieldBreaksTiesInTheEarlierOne() throws Exception {
    // red and blue tie on price, so rating decides: blue(1.0) < red(2.0) < green(price 20).
    writeDocuments(
        writer -> {
          writer.accept(row("NYC", "red", 10, 2.0, 100L));
          writer.accept(row("NYC", "blue", 10, 1.0, 200L));
          writer.accept(row("NYC", "green", 20, 5.0, 300L));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), priceThenRatingSpec()));

    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  @Test
  public void doubleField_decodesFromSortableLong() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(row("NYC", "red", 10, 2.5, 100L));
          writer.accept(row("NYC", "blue", 20, -1.75, 200L));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastMetricByCity(RATING_PATH),
            contextWith(
                Map.of(RATING_PATH, RATING_DOUBLE), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(2.5, metric(bucket(byCity, "NYC"), "first_metric").asDouble().getValue(), 0.0);
    assertEquals(-1.75, metric(bucket(byCity, "NYC"), "last_metric").asDouble().getValue(), 0.0);
  }

  @Test
  public void dateField_decodesToDateTime() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(row("NYC", "red", 10, 2.5, 1_700_000_000_000L));
          writer.accept(row("NYC", "blue", 20, 1.0, 1_800_000_000_000L));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastMetricByCity(CREATED_PATH),
            contextWith(
                Map.of(CREATED_PATH, CREATED_DATE), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(
        new BsonDateTime(1_700_000_000_000L), metric(bucket(byCity, "NYC"), "first_metric"));
    assertEquals(
        new BsonDateTime(1_800_000_000_000L), metric(bucket(byCity, "NYC"), "last_metric"));
  }

  /**
   * A multi-valued sort field is read through a selector -- MIN under an ascending sort -- and
   * inverting the spec keeps that selector while flipping only the direction.
   *
   * <p>This is the case that separates a real inversion from "what the user would have typed as
   * -1": {@code {price: -1}} would read MAX, so an inversion that re-derived the selector from the
   * flipped order would compare wide(100) against narrow(60) and hand {@code last} the wrong
   * document.
   */
  @Test
  public void multiValuedSortField_ascendingInversionKeepsTheMinSelector() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(multiPricedDocument("NYC", "wide", 1, 100));
          writer.accept(multiPricedDocument("NYC", "narrow", 50, 60));
          writer.accept(multiPricedDocument("NYC", "mid", 40, 45));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    // MIN keys: wide=1 < mid=40 < narrow=50.
    assertEquals(new BsonString("wide"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("narrow"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /** The mirror image: a descending sort reads MAX, and inversion has to keep reading MAX. */
  @Test
  public void multiValuedSortField_descendingInversionKeepsTheMaxSelector() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(multiPricedDocument("NYC", "wide", 1, 100));
          writer.accept(multiPricedDocument("NYC", "narrow", 50, 60));
          writer.accept(multiPricedDocument("NYC", "mid", 40, 45));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_DESC)));

    // MAX keys, descending: wide=100 > narrow=60 > mid=45. Note last differs from the ascending
    // case above -- the selector, not just the direction, decides which document wins.
    assertEquals(new BsonString("wide"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("mid"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /**
   * A document missing the sort field sorts as the lowest value under the default ascending spec,
   * so inversion has to move it to the far end rather than leave it winning.
   */
  @Test
  public void missingSortValues_inversionMovesThemToTheOppositeEnd() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(document("NYC", Optional.of("red"), 10, Optional.empty()));
          writer.accept(document("NYC", Optional.of("blue"), 30, Optional.empty()));
          writer.accept(unpricedDocument("NYC", "grey"));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(new BsonString("grey"), metric(bucket(byCity, "NYC"), "first_color"));
    // Not grey: the missing value is lowest, so under the inverted sort it comes last.
    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /**
   * Every component of a compound sort has to be inverted, not just the leading one. The two
   * highest-priced documents tie, so only an inverted rating can separate them.
   */
  @Test
  public void compoundSort_inversionReversesEveryComponent() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(row("NYC", "blue", 10, 1.0, 100L));
          writer.accept(row("NYC", "red", 10, 2.0, 200L));
          writer.accept(row("NYC", "green", 20, 5.0, 300L));
          writer.accept(row("NYC", "yellow", 20, 7.0, 400L));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), priceThenRatingSpec()));

    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "first_color"));
    // yellow, not green: inverting price alone would take price 20 with the lowest rating.
    assertEquals(new BsonString("yellow"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /** Inversion is not int64-specific: the same spec-level flip drives a date sort component. */
  @Test
  public void sortOnDateField_lastPicksTheLatestDocument() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(row("NYC", "red", 10, 2.0, 1_700_000_000_000L));
          writer.accept(row("NYC", "blue", 20, 1.0, 1_500_000_000_000L));
          writer.accept(row("NYC", "green", 30, 5.0, 1_900_000_000_000L));
        });

    TermsAggregationResult byCity =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), createdSpec(UserFieldSortOptions.DEFAULT_ASC)));

    assertEquals(new BsonString("blue"), metric(bucket(byCity, "NYC"), "first_color"));
    assertEquals(new BsonString("green"), metric(bucket(byCity, "NYC"), "last_color"));
  }

  /**
   * Ties must fall to the lower partition for {@code first} and the higher for {@code last}, never
   * to the raw doc id and never to fold order. The later partition deliberately holds the lower doc
   * id.
   */
  @Test
  public void mergeAcrossPartitions_tiesResolveByPartitionNotDocId() {
    AggregationResult first0 = firstResult(MERGE_SORT, 0, 500, "red");
    AggregationResult first1 = firstResult(MERGE_SORT, 1, 1, "blue");
    assertEquals(new BsonString("red"), merge(first0, first1).toStandaloneBson());
    assertEquals(new BsonString("red"), merge(first1, first0).toStandaloneBson());

    AggregationResult last0 = lastResult(INVERTED_MERGE_SORT, 0, 500, "red");
    AggregationResult last1 = lastResult(INVERTED_MERGE_SORT, 1, 1, "blue");
    assertEquals(new BsonString("blue"), merge(last0, last1).toStandaloneBson());
    assertEquals(new BsonString("blue"), merge(last1, last0).toStandaloneBson());
  }

  /** With no sort the whole ordering is encounter order, so partition must lead there too. */
  @Test
  public void mergeAcrossPartitions_withoutSort_ordersByPartitionThenDocId() {
    AggregationResult first0 = firstResultWithoutSort(0, 500, "red");
    AggregationResult first1 = firstResultWithoutSort(1, 1, "blue");
    assertEquals(new BsonString("red"), merge(first1, first0).toStandaloneBson());

    AggregationResult last0 = lastResultWithoutSort(0, 500, "red");
    AggregationResult last1 = lastResultWithoutSort(1, 1, "blue");
    assertEquals(new BsonString("blue"), merge(last0, last1).toStandaloneBson());
  }

  /**
   * A plain long sort for the merge tests, which supply sort keys directly rather than collecting
   * them, so nothing here depends on which typed Lucene field the query layer would have picked.
   */
  private static final Sort MERGE_SORT = new Sort(new SortField("price", SortField.Type.LONG));

  /** What {@code last} merges under: the same sort inverted, as its accumulator collected with. */
  private static final Sort INVERTED_MERGE_SORT =
      new Sort(new SortField("price", SortField.Type.LONG, true));

  /** The same key on both sides, so only the tiebreak decides. */
  private static Object[] tiedKey() {
    return new Object[] {10L};
  }

  private static AggregationResult firstResult(
      Sort collectorSort, int partitionId, int docId, String value) {
    return new FirstAggregationResult(
        "metric", Optional.of(collectorSort), candidate(partitionId, docId, tiedKey(), value));
  }

  private static AggregationResult lastResult(
      Sort collectorSort, int partitionId, int docId, String value) {
    return new LastAggregationResult(
        "metric", Optional.of(collectorSort), candidate(partitionId, docId, tiedKey(), value));
  }

  private static AggregationResult firstResultWithoutSort(
      int partitionId, int docId, String value) {
    return new FirstAggregationResult(
        "metric", Optional.empty(), candidate(partitionId, docId, new Object[0], value));
  }

  private static AggregationResult lastResultWithoutSort(int partitionId, int docId, String value) {
    return new LastAggregationResult(
        "metric", Optional.empty(), candidate(partitionId, docId, new Object[0], value));
  }

  private static Optional<FirstLastAggregationResultBase.Candidate> candidate(
      int partitionId, int docId, Object[] sortKey, String value) {
    return Optional.of(
        new FirstLastAggregationResultBase.Candidate(
            partitionId, new FieldDoc(docId, 0f, sortKey), Optional.of(new BsonString(value))));
  }

  /**
   * Three grouping levels -- city, then color, then size -- with first/last at the leaf. Grouping
   * is expressed by nesting one field per level rather than a composite key, so a metric leaf sees
   * the innermost bucket ordinal and each level narrows the set of documents competing.
   */
  @Test
  public void nestedGroupingByThreeFields_picksWinnerWithinEachLeafBucket() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(sizedDocument("NYC", "red", "S", 10));
          writer.accept(sizedDocument("NYC", "red", "S", 20));
          writer.accept(sizedDocument("NYC", "red", "L", 30));
          writer.accept(sizedDocument("NYC", "blue", "S", 40));
          writer.accept(sizedDocument("LA", "red", "S", 50));
        });

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                byCity(
                    List.of(
                        nestedTerms(
                            "by_color",
                            COLOR_PATH,
                            List.of(
                                nestedTerms(
                                    "by_size",
                                    SIZE_PATH,
                                    List.of(
                                        new FirstAggregationSpec("first_price", PRICE_PATH),
                                        new LastAggregationSpec("last_price", PRICE_PATH)))))))));

    TermsAggregationResult byCity =
        runByCity(
            spec,
            contextWith(
                Map.of(PRICE_PATH, PRICE_INT64), priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    TermsAggregationResult nycColors = childTerms(bucket(byCity, "NYC"), "by_color");
    TermsAggregationResult nycRedSizes = childTerms(bucket(nycColors, "red"), "by_size");

    // NYC > red > S holds the 10 and 20 documents; the other leaves hold one each.
    assertEquals(10L, metric(bucket(nycRedSizes, "S"), "first_price").asInt64().getValue());
    assertEquals(20L, metric(bucket(nycRedSizes, "S"), "last_price").asInt64().getValue());
    assertEquals(30L, metric(bucket(nycRedSizes, "L"), "first_price").asInt64().getValue());

    TermsAggregationResult nycBlueSizes = childTerms(bucket(nycColors, "blue"), "by_size");
    assertEquals(40L, metric(bucket(nycBlueSizes, "S"), "first_price").asInt64().getValue());

    TermsAggregationResult laSizes =
        childTerms(bucket(childTerms(bucket(byCity, "LA"), "by_color"), "red"), "by_size");
    assertEquals(50L, metric(bucket(laSizes, "S"), "first_price").asInt64().getValue());
  }

  /**
   * Metrics in one bucket reading different paths.
   *
   * <p>Each metric owns its own collectors and its own resolved field, so a bucket holding several
   * of them must not let one metric's path or winner leak into another's. They agree on the winning
   * document here -- one sort orders them all -- but read different values out of it.
   */
  @Test
  public void siblingMetricsOnDifferentFields_eachReadsItsOwnPath() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("NYC", "green", 5)),
        Integer.MAX_VALUE);

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                byCity(
                    List.of(
                        new FirstAggregationSpec("first_color", COLOR_PATH),
                        new LastAggregationSpec("last_color", COLOR_PATH),
                        new FirstAggregationSpec("first_price", PRICE_PATH),
                        new LastAggregationSpec("last_price", PRICE_PATH)))));

    TermsAggregationResult byCity =
        runByCity(
            spec,
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN, PRICE_PATH, PRICE_INT64),
                priceSpec(UserFieldSortOptions.DEFAULT_ASC)));

    // green(5) sorts first and blue(30) last; both metrics of each pair see the same document.
    TermsAggregationResult.TermsBucket nyc = bucket(byCity, "NYC");
    assertEquals(new BsonString("green"), metric(nyc, "first_color"));
    assertEquals(5L, metric(nyc, "first_price").asInt64().getValue());
    assertEquals(new BsonString("blue"), metric(nyc, "last_color"));
    assertEquals(30L, metric(nyc, "last_price").asInt64().getValue());
  }

  /**
   * Two grouping branches side by side, each with its own first/last.
   *
   * <p>Sibling groups number their buckets independently, so ordinal 0 means a different bucket in
   * each branch. A metric indexes its per-bucket state by owning ordinal, so this is what would
   * catch two branches sharing one ordinal space.
   */
  @Test
  public void siblingGroups_eachBranchKeepsItsOwnBucketWinners() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "blue", 30),
            new TestDoc("LA", "red", 5),
            new TestDoc("LA", "green", 40)),
        Integer.MAX_VALUE);

    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                byCity(
                    List.of(
                        new FirstAggregationSpec("first_price", PRICE_PATH),
                        new LastAggregationSpec("last_price", PRICE_PATH))),
                nestedTerms(
                    "by_color",
                    COLOR_PATH,
                    List.of(
                        new FirstAggregationSpec("first_price", PRICE_PATH),
                        new LastAggregationSpec("last_price", PRICE_PATH)))));

    TopLevelAggregationResult top =
        (TopLevelAggregationResult)
            collectAll(
                    spec,
                    contextWith(
                        Map.of(PRICE_PATH, PRICE_INT64),
                        priceSpec(UserFieldSortOptions.DEFAULT_ASC)))
                .buildTopLevelResult();

    TermsAggregationResult byCity = (TermsAggregationResult) top.childResults().get(0);
    TermsAggregationResult byColor = (TermsAggregationResult) top.childResults().get(1);

    // Grouped by city: NYC holds 10 and 30, LA holds 5 and 40.
    assertEquals(10L, metric(bucket(byCity, "NYC"), "first_price").asInt64().getValue());
    assertEquals(30L, metric(bucket(byCity, "NYC"), "last_price").asInt64().getValue());
    assertEquals(5L, metric(bucket(byCity, "LA"), "first_price").asInt64().getValue());
    assertEquals(40L, metric(bucket(byCity, "LA"), "last_price").asInt64().getValue());

    // Grouped by color instead: red spans both cities at 5 and 10.
    assertEquals(5L, metric(bucket(byColor, "red"), "first_price").asInt64().getValue());
    assertEquals(10L, metric(bucket(byColor, "red"), "last_price").asInt64().getValue());
    assertEquals(30L, metric(bucket(byColor, "blue"), "first_price").asInt64().getValue());
    assertEquals(40L, metric(bucket(byColor, "green"), "last_price").asInt64().getValue());
  }

  /**
   * The same spec collected against two partitions and merged, as a partitioned index does.
   *
   * <p>The merge tests above hand-build their candidates; this one runs real accumulators, so it
   * also covers the partition id reaching the winners from the context.
   */
  @Test
  public void acrossPartitions_mergesRealAccumulatorResults() throws Exception {
    buildIndex(
        List.of(new TestDoc("NYC", "red", 10), new TestDoc("NYC", "blue", 30)), Integer.MAX_VALUE);

    TermsAggregationResult partition0 =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC), 0));
    TermsAggregationResult partition1 =
        runByCity(
            firstAndLastColorByCity(),
            contextWith(
                Map.of(COLOR_PATH, COLOR_TOKEN), priceSpec(UserFieldSortOptions.DEFAULT_ASC), 1));

    // Both partitions see the same documents here, so the sort decides and the partition id only
    // has to keep the merge from preferring one arbitrarily.
    assertEquals(
        new BsonString("red"),
        merge(
                bucket(partition0, "NYC").getChildResults().get("first_color"),
                bucket(partition1, "NYC").getChildResults().get("first_color"))
            .toStandaloneBson());
    assertEquals(
        new BsonString("blue"),
        merge(
                bucket(partition0, "NYC").getChildResults().get("last_color"),
                bucket(partition1, "NYC").getChildResults().get("last_color"))
            .toStandaloneBson());
  }

  /** Without a sort the merge falls to partition order, so the lower partition supplies first. */
  @Test
  public void acrossPartitions_withoutSort_lowerPartitionSuppliesFirst() throws Exception {
    buildIndex(
        List.of(new TestDoc("NYC", "red", 10), new TestDoc("NYC", "blue", 30)), Integer.MAX_VALUE);

    TermsAggregationResult partition0 =
        runByCity(
            firstAndLastColorByCity(), contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), List.of(), 0));
    TermsAggregationResult partition1 =
        runByCity(
            firstAndLastColorByCity(), contextWith(Map.of(COLOR_PATH, COLOR_TOKEN), List.of(), 1));

    assertEquals(
        new BsonString("red"),
        merge(
                bucket(partition1, "NYC").getChildResults().get("first_color"),
                bucket(partition0, "NYC").getChildResults().get("first_color"))
            .toStandaloneBson());
    assertEquals(
        new BsonString("blue"),
        merge(
                bucket(partition0, "NYC").getChildResults().get("last_color"),
                bucket(partition1, "NYC").getChildResults().get("last_color"))
            .toStandaloneBson());
  }

  // ===== index fixtures =====

  /** NYC documents where the cheapest one has no color at all. */
  private void buildIndexWithOptionalColor() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(document("NYC", Optional.of("red"), 10, Optional.empty()));
          writer.accept(document("NYC", Optional.of("blue"), 30, Optional.empty()));
          writer.accept(document("NYC", Optional.empty(), 5, Optional.empty()));
        });
  }

  /** NYC documents carrying a stored source with a nested array field. */
  private void buildIndexWithStoredSource() throws Exception {
    writeDocuments(
        writer -> {
          writer.accept(
              document(
                  "NYC",
                  Optional.of("red"),
                  10,
                  Optional.of(
                      storedSource(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)))))));
          writer.accept(
              document(
                  "NYC",
                  Optional.of("blue"),
                  30,
                  Optional.of(storedSource(new BsonArray(List.of(new BsonInt32(9)))))));
        });
  }

  /** A document carrying every value type the tests read: token, int64, double and date. */
  private static Document row(String city, String color, long price, double rating, long created) {
    Document doc =
        document(
            city,
            Optional.of(color),
            price,
            Optional.of(
                new BsonDocument()
                    .append(RATING_FIELD, new BsonDouble(rating))
                    .append(CREATED_FIELD, new BsonDateTime(created))));
    // Doc values for the sorts the tests build over rating and created.
    doc.add(
        new SortedNumericDocValuesField(
            RATING_LUCENE_FIELD, LuceneDoubleConversionUtils.toMqlSortableLong(rating)));
    doc.add(new SortedNumericDocValuesField(CREATED_LUCENE_FIELD, created));
    return doc;
  }

  /**
   * A document whose sort field holds several values, so the sort's selector decides which one the
   * comparison sees. The stored source keeps the first, which no assertion reads.
   */
  private static Document multiPricedDocument(String city, String color, long... prices) {
    Document doc = document(city, Optional.of(color), prices[0], Optional.empty());
    for (int i = 1; i < prices.length; i++) {
      doc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, prices[i]));
    }
    return doc;
  }

  /** A document with no value for the sort field at all. */
  private static Document unpricedDocument(String city, String color) {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef(city)));
    doc.add(new SortedSetDocValuesField(COLOR_LUCENE_FIELD, new BytesRef(color)));
    addStoredSource(
        doc,
        new BsonDocument()
            .append(CITY_FIELD, new BsonString(city))
            .append(COLOR_FIELD, new BsonString(color)));
    return doc;
  }

  private static BsonDocument storedSource(BsonValue tags) {
    return new BsonDocument("meta", new BsonDocument("tags", tags));
  }

  /** A document carrying a third token field, for nested grouping. */
  private static Document sizedDocument(String city, String color, String size, long price) {
    Document doc =
        document(
            city,
            Optional.of(color),
            price,
            Optional.of(new BsonDocument(SIZE_FIELD, new BsonString(size))));
    doc.add(new SortedSetDocValuesField(SIZE_LUCENE_FIELD, new BytesRef(size)));
    return doc;
  }

  /**
   * A document with city/color/price in both doc values (for grouping and sorting) and stored
   * source (where the metrics read their values). {@code extraSource} is merged over that base
   * source, so a test can add fields or override one with a different shape.
   */
  private static Document document(
      String city, Optional<String> color, long price, Optional<BsonDocument> extraSource) {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef(city)));
    color.ifPresent(
        value -> doc.add(new SortedSetDocValuesField(COLOR_LUCENE_FIELD, new BytesRef(value))));
    doc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, price));

    BsonDocument source =
        new BsonDocument()
            .append(CITY_FIELD, new BsonString(city))
            .append(PRICE_FIELD, new BsonInt64(price));
    color.ifPresent(value -> source.append(COLOR_FIELD, new BsonString(value)));
    extraSource.ifPresent(source::putAll);
    addStoredSource(doc, source);
    return doc;
  }

  private interface DocumentWriter {
    void write(java.util.function.Consumer<Document> writer);
  }

  private void writeDocuments(DocumentWriter... segments) throws Exception {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (DocumentWriter segment : segments) {
        segment.write(
            doc -> {
              try {
                writer.addDocument(doc);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
        writer.commit();
      }
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }
}
