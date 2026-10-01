package com.xgen.mongot.index.query.sort;

import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.sort.SortOptionsBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SortOptionsTest.DeserializationTest.class,
      SortOptionsTest.SerializationTest.class,
      SortOptionsTest.ClassTest.class,
      SortOptionsTest.EmbeddedScopeTest.class
    })
public class SortOptionsTest {

  @RunWith(Theories.class)
  public static class ClassTest {

    @DataPoints public static final SortSelector[] selectors = SortSelector.values();

    @DataPoints public static final SortOrder[] sortOrders = SortOrder.values();

    @DataPoints
    public static final NullEmptySortPosition[] nullEmptySortPositions =
        NullEmptySortPosition.values();

    @DataPoints public static final MetaSortField[] meta = MetaSortField.values();

    @Theory
    public void testInvertMeta(
        @FromDataPoints("meta") MetaSortField meta,
        @FromDataPoints("sortOrders") SortOrder order,
        @FromDataPoints("selectors") SortSelector selector) {
      MetaSortOptions original = new MetaSortOptions(order, selector, meta);
      MetaSortOptions inverse = original.invert();

      assertEquals(original.meta(), inverse.meta());
      assertNotEquals(original.order(), inverse.order());
      assertEquals(original.order().invert(), inverse.order());
      assertEquals(original.selector(), inverse.selector());

      var originalSort = original.getMetaSort();
      var inverseSort = inverse.getMetaSort();
      assertEquals(originalSort.getType(), inverseSort.getType());
      assertEquals(originalSort.getMissingValue(), inverseSort.getMissingValue());
      assertEquals(!originalSort.getReverse(), inverseSort.getReverse());
      assertEquals(originalSort.getField(), inverseSort.getField());
    }

    @Theory
    public void testInvertNoData(
        @FromDataPoints("sortOrders") SortOrder order,
        @FromDataPoints("selectors") SortSelector selector,
        @FromDataPoints("nullEmptySortPositions") NullEmptySortPosition nullEmptySortPosition) {
      UserFieldSortOptions original =
          new UserFieldSortOptions(order, selector, nullEmptySortPosition);
      UserFieldSortOptions inverse = original.invert();

      assertNotEquals(original.order(), inverse.order());
      assertEquals(original.order().invert(), inverse.order());
      assertEquals(original.selector(), inverse.selector());
      assertEquals(original.nullEmptySortPosition(), inverse.nullEmptySortPosition());
    }

    @Test
    public void testAscending() throws BsonParseException {
      SortOptions options = SortOptions.fromBson(BsonParseContext.root(), new BsonInt32(1));

      assertEquals(SortOrder.ASC, options.order());
      assertEquals(SortedNumericSelector.Type.MIN, options.selector().numericSelector);
      assertEquals(SortedSetSelector.Type.MIN, options.selector().sortedSetSelector);
    }

    @Test
    public void testDescending() throws BsonParseException {
      SortOptions options = SortOptions.fromBson(BsonParseContext.root(), new BsonInt32(-1));

      assertEquals(SortOrder.DESC, options.order());
      assertEquals(SortedNumericSelector.Type.MAX, options.selector().numericSelector);
      assertEquals(SortedSetSelector.Type.MAX, options.selector().sortedSetSelector);
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "sort-options-deserialization";
    private static final BsonDeserializationTestSuite<SortOptions> TEST_SUITE =
        fromValue("src/test/unit/resources/index/query/sort", SUITE_NAME, SortOptions::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SortOptions> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<SortOptions> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SortOptions>> data() {
      return TEST_SUITE.withExamples(
          simpleAsc(),
          simpleDesc(),
          scoreAsc(),
          scoreDesc(),
          scoreDefault(),
          noDataLowestOrderAsc(),
          noDataHighestOrderAsc(),
          noDataLowestOrderDesc(),
          noDataHighestOrderDesc(),
          orderWithoutMetaOrNoData(),
          noDataHighestOrderAsc());
    }

    @org.junit.Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> simpleAsc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple asc", UserFieldSortOptions.DEFAULT_ASC);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> simpleDesc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple desc", UserFieldSortOptions.DEFAULT_DESC);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> scoreAsc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "searchScore asc",
          SortOptionsBuilder.meta()
              .meta(MetaSortField.SEARCH_SCORE)
              .sortOrder(SortOrder.ASC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> scoreDesc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "searchScore desc",
          SortOptionsBuilder.meta()
              .meta(MetaSortField.SEARCH_SCORE)
              .sortOrder(SortOrder.DESC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> scoreDefault() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "searchScore default",
          SortOptionsBuilder.meta()
              .meta(MetaSortField.SEARCH_SCORE)
              .sortOrder(SortOrder.DESC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> noDataLowestOrderAsc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "noData lowest order asc",
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.ASC)
              .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> noDataHighestOrderAsc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "noData highest order asc",
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.ASC)
              .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> noDataLowestOrderDesc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "noData lowest order desc",
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.DESC)
              .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> noDataHighestOrderDesc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "noData highest order desc",
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.DESC)
              .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortOptions> orderWithoutMetaOrNoData() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "order-without-meta-or-noData",
          SortOptionsBuilder.user().sortOrder(SortOrder.DESC).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "sort-options-serialization";
    private static final BsonSerializationTestSuite<SortOptions> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/query/sort", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SortOptions> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<SortOptions> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SortOptions>> data() {
      return List.of(simple(), meta(), noData());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SortOptions> simple() {
      return BsonSerializationTestSuite.TestSpec.create("simple", UserFieldSortOptions.DEFAULT_ASC);
    }

    private static BsonSerializationTestSuite.TestSpec<SortOptions> meta() {
      return BsonSerializationTestSuite.TestSpec.create(
          "meta",
          SortOptionsBuilder.meta()
              .meta(MetaSortField.SEARCH_SCORE)
              .sortOrder(SortOrder.DESC)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SortOptions> noData() {
      return BsonSerializationTestSuite.TestSpec.create(
          "noData",
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.DESC)
              .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
              .build());
    }
  }

  /**
   * Parsing of the {@code embeddedDocument} block on a scoped sort key. The parsed {@code path} and
   * raw operator are retained; interpreting the {@code operator} is a query-layer concern.
   */
  @RunWith(JUnit4.class)
  public static class EmbeddedScopeTest {

    private static BsonDocument operatorDoc() {
      return new BsonDocument(
          "equals",
          new BsonDocument("path", new BsonString("variants.inStock"))
              .append("value", org.bson.BsonBoolean.TRUE));
    }

    private static BsonDocument scopedSortValue(int order, BsonDocument embeddedDocument) {
      return new BsonDocument()
          .append("order", new BsonInt32(order))
          .append("embeddedDocument", embeddedDocument);
    }

    private static BsonDocument fullEmbeddedDocument() {
      return new BsonDocument()
          .append("path", new BsonString("variants"))
          .append("operator", operatorDoc());
    }

    @Test
    public void fromBson_embeddedDocument_retainsScope() throws BsonParseException {
      SortOptions options =
          SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, fullEmbeddedDocument()));

      UserFieldSortOptions userOptions = (UserFieldSortOptions) options;
      assertEquals(SortOrder.ASC, userOptions.order());
      assertEquals(
          Optional.of(new EmbeddedSortScope(FieldPath.parse("variants"), operatorDoc())),
          userOptions.embeddedScope());
    }

    @Test
    public void fromBson_unscopedSort_hasEmptyScope() throws BsonParseException {
      SortOptions options = SortOptions.fromBson(BsonParseContext.root(), new BsonInt32(1));

      assertEquals(Optional.empty(), ((UserFieldSortOptions) options).embeddedScope());
    }

    @Test
    public void testMissingOperatorRejected() {
      BsonDocument embeddedDocument = new BsonDocument("path", new BsonString("variants"));

      assertThrows(
          BsonParseException.class,
          () ->
              SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, embeddedDocument)));
    }

    @Test
    public void testMissingPathRejected() {
      BsonDocument embeddedDocument = new BsonDocument("operator", operatorDoc());

      assertThrows(
          BsonParseException.class,
          () ->
              SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, embeddedDocument)));
    }

    @Test
    public void fromBson_nonDocumentOperator_rejected() {
      BsonDocument embeddedDocument = fullEmbeddedDocument().append("operator", new BsonInt32(1));

      assertThrows(
          BsonParseException.class,
          () ->
              SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, embeddedDocument)));
    }

    @Test
    public void fromBson_unknownScopeField_rejected() {
      BsonDocument embeddedDocument = fullEmbeddedDocument().append("unknown", new BsonInt32(1));

      assertThrows(
          BsonParseException.class,
          () ->
              SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, embeddedDocument)));
    }

    @Test
    public void testEmbeddedDocumentWithMetaRejected() {
      BsonDocument value =
          new BsonDocument()
              .append("$meta", new BsonString("searchScore"))
              .append("embeddedDocument", fullEmbeddedDocument());

      assertThrows(
          BsonParseException.class, () -> SortOptions.fromBson(BsonParseContext.root(), value));
    }

    @Test
    public void invert_scopedSort_preservesScope() throws BsonParseException {
      UserFieldSortOptions original =
          (UserFieldSortOptions)
              SortOptions.fromBson(
                  BsonParseContext.root(), scopedSortValue(1, fullEmbeddedDocument()));

      UserFieldSortOptions inverted = original.invert();

      assertEquals(SortOrder.DESC, inverted.order());
      assertEquals(original.embeddedScope(), inverted.embeddedScope());
      assertEquals(fullEmbeddedDocument(), inverted.toBson().getDocument("embeddedDocument"));
    }

    @Test
    public void testToBsonRoundTripPreservesScope() throws BsonParseException {
      // A scoped sort must survive parse -> toBson -> parse, otherwise SortSpec.toBson() /
      // SearchQuery.toBson() would silently drop the scope and turn it into an unscoped sort.
      UserFieldSortOptions original =
          (UserFieldSortOptions)
              SortOptions.fromBson(
                  BsonParseContext.root(), scopedSortValue(1, fullEmbeddedDocument()));

      BsonDocument serialized = original.toBson();
      assertTrue(serialized.containsKey("embeddedDocument"));
      assertEquals(fullEmbeddedDocument(), serialized.getDocument("embeddedDocument"));

      UserFieldSortOptions reparsed =
          (UserFieldSortOptions) SortOptions.fromBson(BsonParseContext.root(), serialized);
      assertEquals(original.embeddedScope(), reparsed.embeddedScope());
      assertEquals(FieldPath.parse("variants"), reparsed.embeddedScope().orElseThrow().path());
    }

    @Test
    public void toBson_constructedScope_roundTrips() throws BsonParseException {
      UserFieldSortOptions options =
          SortOptionsBuilder.user()
              .sortOrder(SortOrder.ASC)
              .embeddedScope(new EmbeddedSortScope(FieldPath.parse("variants"), operatorDoc()))
              .build();

      BsonDocument serialized = options.toBson();

      assertEquals(fullEmbeddedDocument(), serialized.getDocument("embeddedDocument"));
      assertEquals(options, SortOptions.fromBson(BsonParseContext.root(), serialized));
    }

    @Test
    public void toBson_inputScopePathChanged_preservesParsedPath() throws BsonParseException {
      BsonDocument embeddedDocument = fullEmbeddedDocument();
      SortOptions options =
          SortOptions.fromBson(BsonParseContext.root(), scopedSortValue(1, embeddedDocument));

      embeddedDocument.put("path", new BsonString("other"));

      assertEquals(fullEmbeddedDocument(), options.toBson().getDocument("embeddedDocument"));
    }

    @Test
    public void testToBsonUnscopedOmitsEmbeddedDocument() throws BsonParseException {
      UserFieldSortOptions unscoped =
          (UserFieldSortOptions) SortOptions.fromBson(BsonParseContext.root(), new BsonInt32(1));

      assertFalse(unscoped.toBson().containsKey("embeddedDocument"));
    }
  }
}
