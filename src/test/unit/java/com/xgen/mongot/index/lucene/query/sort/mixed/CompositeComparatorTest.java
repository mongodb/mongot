package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils.toMqlSortableLong;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * This test compares results of a lucene sort using {@link CompositeComparator} to sorting
 * equivalent values in memory with Java's {@code stream.sorted(MQL_COMPARATOR)}.
 *
 * <p>This class does not test that the implementation of {@link SortUtil#mqlMixedCompare(BsonValue,
 * BsonValue, NullEmptySortPosition)} is correct, but instead tests that any bracket-based pruning
 * implemented by {@link CompositeComparator} does not change the order of the sort results.
 *
 * <p>Although the test data is non-deterministic, this test should never appear flakey.
 */
@RunWith(Theories.class)
public class CompositeComparatorTest {

  /**
   * The maximum value of an unsigned 3 byte integer, which is the maximum value for ObjectId's
   * counter field.
   */
  private static final int MAX_MEDIUM_INTEGER = 0x00FF_FFFF;

  /**
   * {@link RandomStringUtils} doesn't produce valid UTF-16 if it is allowed to include private high
   * surrogate chars.
   */
  private static final int MAX_PUBLIC_SURROGATE = 0xD800;

  /**
   * The maximum magnitude double that can be precisely represented as a long. We currently do not
   * handle double-long comparisons outside this range correctly.
   */
  private static final long MAX_PRECISE_LONG = 1L << 52L;

  private static final FieldPath path = FieldPath.newRoot("root");

  private static final String dateField =
      FieldName.TypeField.DATE_V2.getLuceneFieldName(path, Optional.empty());
  private static final String doubleField =
      TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(path, Optional.empty());
  private static final String longField =
      TypeField.NUMBER_INT64_V2.getLuceneFieldName(path, Optional.empty());
  private static final String tokenField =
      TypeField.TOKEN.getLuceneFieldName(path, Optional.empty());
  private static final String uuidField = TypeField.UUID.getLuceneFieldName(path, Optional.empty());
  private static final String nullField = TypeField.NULL.getLuceneFieldName(path, Optional.empty());
  private static final String objectIdField =
      TypeField.OBJECT_ID.getLuceneFieldName(path, Optional.empty());
  private static final String booleanField =
      TypeField.BOOLEAN.getLuceneFieldName(path, Optional.empty());

  private static final Comparator<BsonValue> MQL_COMPARATOR_NULLS_HIGHEST =
      (first, second) -> SortUtil.mqlMixedCompare(first, second, NullEmptySortPosition.HIGHEST);

  private static ByteBuffersDirectory directory;
  private static IndexSearcher searcher;
  private static ImmutableList<BsonValue> sortedPerDocumentMinimumsNullsLowest;
  private static ImmutableList<BsonValue> sortedPerDocumentMaximumsNullsLowest;
  private static ImmutableList<BsonValue> sortedPerDocumentMinimumsNullsHighest;
  private static ImmutableList<BsonValue> sortedPerDocumentMaximumsNullsHighest;

  /** Note bottom is only set if limit < |dataset|. */
  @DataPoints public static final int[] limits = {10, Integer.MAX_VALUE};

  /**
   * For tests to be deterministic, top values must not compare exactly equal to anything in the
   * dataset.
   */
  @DataPoints
  public static final BsonValue[] tops = {
    new BsonString("Q"),
    new BsonDouble(Double.NaN),
    new BsonInt64(MAX_PRECISE_LONG + 1),
    new BsonBinary(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")),
    new BsonObjectId(new ObjectId("ffffffffffffffffffffffff"))
  };

  @DataPoints public static final SortOrder[] orders = {SortOrder.ASC, SortOrder.DESC};

  @DataPoints public static final SortSelector[] selectors = {SortSelector.MIN, SortSelector.MAX};

  @DataPoints
  public static final NullEmptySortPosition[] nullPositions = {
    NullEmptySortPosition.LOWEST, NullEmptySortPosition.HIGHEST
  };

  /**
   * Creates a pair of {@link Document} and {@code List<BsonValue>} such that:
   *
   * <ul>
   *   <li>the key is random document corresponding to a (possibly empty) heterogeneous array.
   *   <li>the value is a list of all raw BsonValues that appear in that document.
   * </ul>
   */
  private static Pair<Document, ImmutableList<BsonValue>> createDoc() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    List<BsonValue> values = new ArrayList<>();
    Document d = new Document();

    // Add long(s)
    if (rng.nextBoolean()) {
      long value = rng.nextLong(-MAX_PRECISE_LONG, MAX_PRECISE_LONG);
      d.add(new SortedNumericDocValuesField(longField, value));
      values.add(new BsonInt64(value));

      if (rng.nextBoolean()) {
        long second = rng.nextLong(-MAX_PRECISE_LONG, MAX_PRECISE_LONG);
        d.add(new SortedNumericDocValuesField(longField, second));
        values.add(new BsonInt64(second));
      }
    }

    // Add date(s)
    if (rng.nextBoolean()) {
      long value = rng.nextLong();
      d.add(new SortedNumericDocValuesField(dateField, value));
      values.add(new BsonDateTime(value));

      if (rng.nextBoolean()) {
        long second = rng.nextLong();
        d.add(new SortedNumericDocValuesField(dateField, second));
        values.add(new BsonDateTime(second));
      }
    }

    // Add double(s)
    if (rng.nextBoolean()) {
      double v = rng.nextDouble(-Double.MAX_VALUE, Double.MAX_VALUE);
      d.add(new SortedNumericDocValuesField(doubleField, toMqlSortableLong(v)));
      values.add(new BsonDouble(v));

      if (rng.nextBoolean()) {
        double sv = rng.nextDouble(-Double.MAX_VALUE, Double.MAX_VALUE);
        d.add(new SortedNumericDocValuesField(doubleField, toMqlSortableLong(sv)));
        values.add(new BsonDouble(sv));
      }
    }

    // Add token(s)
    if (rng.nextBoolean()) {
      String value = RandomStringUtils.random(6, 0, MAX_PUBLIC_SURROGATE, false, false);
      d.add(new SortedSetDocValuesField(tokenField, new BytesRef(value)));
      values.add(new BsonString(value));

      if (rng.nextBoolean()) {
        String second = RandomStringUtils.random(6, 0, MAX_PUBLIC_SURROGATE, false, false);
        d.add(new SortedSetDocValuesField(tokenField, new BytesRef(second)));
        values.add(new BsonString(second));
      }
    }

    // Add uuid(s)
    if (rng.nextBoolean()) {
      // Have the max UUID be less than UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"), since it's
      // reserved for the top value
      UUID uuid = new UUID(rng.nextLong(), rng.nextLong() - 1);
      d.add(new SortedSetDocValuesField(uuidField, new BytesRef(uuid.toString())));
      values.add(new BsonBinary(uuid, UuidRepresentation.STANDARD));

      if (rng.nextBoolean()) {
        UUID second = new UUID(rng.nextLong(), rng.nextLong() - 1);
        d.add(new SortedSetDocValuesField(uuidField, new BytesRef(second.toString())));
        values.add(new BsonBinary(second, UuidRepresentation.STANDARD));
      }
    }

    // Add null
    if (rng.nextBoolean()) {
      d.add(new SortedDocValuesField(nullField, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
      values.add(BsonNull.VALUE);
    }

    // Add objectId(s)
    if (rng.nextBoolean()) {
      // Have the max ObjectId be less than ObjectId("ffffffffffffffffffffffff"), since it's
      // reserved for the top value
      ObjectId objectId = new ObjectId(rng.nextInt(), rng.nextInt(MAX_MEDIUM_INTEGER - 1));
      d.add(new SortedSetDocValuesField(objectIdField, new BytesRef(objectId.toByteArray())));
      values.add(new BsonObjectId(objectId));

      if (rng.nextBoolean()) {
        ObjectId second = new ObjectId(rng.nextInt(), rng.nextInt(MAX_MEDIUM_INTEGER - 1));
        d.add(new SortedSetDocValuesField(objectIdField, new BytesRef(second.toByteArray())));
        values.add(new BsonObjectId(second));
      }
    }

    // Add boolean(s)
    if (rng.nextBoolean()) {
      boolean b1 = rng.nextBoolean();
      d.add(new SortedSetDocValuesField(booleanField, new BytesRef(FieldValue.fromBoolean(b1))));
      values.add(BsonBoolean.valueOf(b1));

      if (rng.nextBoolean()) {
        boolean b2 = rng.nextBoolean();
        d.add(new SortedSetDocValuesField(booleanField, new BytesRef(FieldValue.fromBoolean(b2))));
        values.add(BsonBoolean.valueOf(b2));
      }
    }

    return Pair.of(d, ImmutableList.copyOf(values));
  }

  @BeforeClass
  public static void setup() throws IOException {
    directory = new ByteBuffersDirectory();

    ImmutableList<Pair<Document, ImmutableList<BsonValue>>> dataset =
        Stream.generate(CompositeComparatorTest::createDoc)
            .limit(10_000)
            .collect(toImmutableList());
    ImmutableList<Document> documents =
        dataset.stream().map(Pair::getKey).collect(toImmutableList());
    ImmutableList<ImmutableList<BsonValue>> values =
        dataset.stream().map(Pair::getValue).collect(toImmutableList());

    sortedPerDocumentMinimumsNullsLowest =
        getSortedPerDocumentValues(values, SortSelector.MIN, NullEmptySortPosition.LOWEST);
    sortedPerDocumentMaximumsNullsLowest =
        getSortedPerDocumentValues(values, SortSelector.MAX, NullEmptySortPosition.LOWEST);

    sortedPerDocumentMinimumsNullsHighest =
        getSortedPerDocumentValues(values, SortSelector.MIN, NullEmptySortPosition.HIGHEST);
    sortedPerDocumentMaximumsNullsHighest =
        getSortedPerDocumentValues(values, SortSelector.MAX, NullEmptySortPosition.HIGHEST);

    try (var writer = new RandomSegmentingIndexWriter(directory)) {
      writer.addDocuments(documents);
    }

    // Lucene 10.4's AssertingLeafCollector calls LuceneTestCase.rarely() during bulk scoring,
    // which requires a RandomizedRunner context. Use a plain searcher/reader to sidestep that.
    DirectoryReader reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  private static ImmutableList<BsonValue> getSortedPerDocumentValues(
      ImmutableList<ImmutableList<BsonValue>> values,
      SortSelector selector,
      NullEmptySortPosition nullPosition) {
    Comparator<BsonValue> comparator = getComparator(nullPosition);
    return values.stream()
        .map(
            l ->
                l.stream()
                    .min(selector == SortSelector.MIN ? comparator : comparator.reversed())
                    .orElse(BsonNull.VALUE))
        .sorted(comparator)
        .map(
            bsonValue ->
                bsonValue.isNull()
                    ? (nullPosition == NullEmptySortPosition.HIGHEST
                        ? BsonUtils.MAX_KEY
                        : BsonUtils.MIN_KEY)
                    : bsonValue)
        .collect(toImmutableList());
  }

  private static Comparator<BsonValue> getComparator(NullEmptySortPosition nullPosition) {
    return nullPosition == NullEmptySortPosition.HIGHEST
        ? MQL_COMPARATOR_NULLS_HIGHEST
        : SortUtil.MQL_COMPARATOR_NULLS_LOWEST;
  }

  @AfterClass
  public static void tearDown() throws IOException {
    directory.close();
  }

  @Theory
  public void testSortWithNullPosition(
      int limit, SortOrder order, SortSelector selector, NullEmptySortPosition nullPosition)
      throws IOException {
    Query query = new MatchAllDocsQuery();
    var options = new UserFieldSortOptions(order, selector, nullPosition);
    Sort sort = createSort(options);
    ImmutableList<BsonValue> expectation = getGroundTruth(options, limit);

    TopDocs topDocs = searcher.search(query, limit, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
    assertThat(values).asList().containsExactlyElementsIn(expectation).inOrder();
  }

  @Theory
  public void testAscendingWithTop(
      SortSelector selector, int limit, BsonValue top, NullEmptySortPosition nullPosition)
      throws IOException {
    Query query = new MatchAllDocsQuery();
    var options = new UserFieldSortOptions(SortOrder.ASC, selector, nullPosition);
    Sort sort = createSort(options);
    FieldDoc fieldDoc = new FieldDoc(0, 0f, new Object[] {top});
    ImmutableList<BsonValue> expectation =
        getGroundTruth(options, Integer.MAX_VALUE).stream()
            .dropWhile(e -> SortUtil.mqlMixedCompare(e, top, nullPosition) <= 0)
            .limit(limit)
            .collect(toImmutableList());

    TopDocs topDocs = searcher.searchAfter(fieldDoc, query, limit, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
    assertThat(values).asList().isInOrder(getComparator(nullPosition));
    assertThat(values).asList().containsExactlyElementsIn(expectation).inOrder();
  }

  @Theory
  public void testDescendingWithTop(
      SortSelector selector, int limit, BsonValue top, NullEmptySortPosition nullPosition)
      throws IOException {
    Query query = new MatchAllDocsQuery();
    var options = new UserFieldSortOptions(SortOrder.DESC, selector, nullPosition);
    Sort sort = createSort(options);
    FieldDoc fieldDoc = new FieldDoc(0, 0f, new Object[] {top});
    ImmutableList<BsonValue> expectation =
        getGroundTruth(options, Integer.MAX_VALUE).stream()
            .dropWhile(e -> SortUtil.mqlMixedCompare(e, top, nullPosition) >= 0)
            .limit(limit)
            .collect(toImmutableList());

    TopDocs topDocs = searcher.searchAfter(fieldDoc, query, limit, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
    assertThat(values).asList().isInOrder(getComparator(nullPosition).reversed());
    assertThat(values).asList().containsExactlyElementsIn(expectation).inOrder();
  }

  private static Sort createSort(SortOptions options) {
    SortField[] sortFields = new SortField[1];
    sortFields[0] = new MqlMixedSort(new MongotSortField(path, options), Optional.empty());
    return new Sort(sortFields);
  }

  private static ImmutableList<BsonValue> getGroundTruth(UserFieldSortOptions options, int limit) {
    int cappedLimit = Math.min(limit, sortedPerDocumentMaximumsNullsLowest.size());
    ImmutableList<BsonValue> values;

    if (options.nullEmptySortPosition() == NullEmptySortPosition.HIGHEST) {
      values =
          options.selector() == SortSelector.MIN
              ? sortedPerDocumentMinimumsNullsHighest
              : sortedPerDocumentMaximumsNullsHighest;
    } else {
      values =
          options.selector() == SortSelector.MIN
              ? sortedPerDocumentMinimumsNullsLowest
              : sortedPerDocumentMaximumsNullsLowest;
    }

    Comparator<BsonValue> comparator = getComparator(options.nullEmptySortPosition());

    return values.stream()
        .sorted(options.isReverse() ? comparator.reversed() : comparator)
        .limit(cappedLimit)
        .collect(toImmutableList());
  }
}
