package com.xgen.mongot.index.lucene.query.sort;

import static org.apache.lucene.search.SortField.STRING_FIRST;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.util.FieldTypeBuilder;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

class SortPruningTestUtils {
  public static final FieldPath FIELD_NAME = FieldPath.newRoot("foo");
  public static final Object MISSING_VALUE_ORDER = STRING_FIRST;
  public static final SortOptions DEFAULT_ASC_NULLS_HIGHEST =
      new UserFieldSortOptions(SortOrder.ASC, NullEmptySortPosition.HIGHEST);
  public static final SortOptions DEFAULT_DESC_NULLS_HIGHEST =
      new UserFieldSortOptions(SortOrder.DESC, NullEmptySortPosition.HIGHEST);

  public static final Function<Object, Comparable<?>> UUID_STRING_COMPARABLE =
      o -> ((BsonBinary) o).asUuid().toString();

  /**
   * Creates a Lucene Document optionally containing values for all the sortable datatypes of Mongot
   * stored in DocValues & Points for testing purposes.
   */
  public static Document createDoc(
      long value, boolean indexPoints, Function<Long, Boolean> addFields) {
    Document doc = new Document();
    if (!addFields.apply(value)) {
      return doc;
    }

    // index docValues
    doc.add(
        new NumericDocValuesField(
            FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            value));
    doc.add(
        new NumericDocValuesField(
            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            LuceneDoubleConversionUtils.toMqlSortableLong(value)));
    doc.add(
        new SortedDocValuesField(
            FieldName.TypeField.NULL.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            new BytesRef(FieldValue.NULL_FIELD_VALUE)));
    doc.add(
        new SortedSetDocValuesField(
            FieldName.TypeField.TOKEN.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            intToBytesRef(value)));

    String uuidStr = UUID.randomUUID().toString();
    doc.add(
        new SortedSetDocValuesField(
            FieldName.TypeField.UUID.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            new BytesRef(uuidStr)));

    var objectId = new ObjectId();
    doc.add(
        new SortedSetDocValuesField(
            FieldName.TypeField.OBJECT_ID.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            new BytesRef(objectId.toByteArray())));

    boolean bool = value % 2 == 0;
    doc.add(
        new SortedSetDocValuesField(
            FieldName.TypeField.BOOLEAN.getLuceneFieldName(FIELD_NAME, Optional.empty()),
            new BytesRef(FieldValue.fromBoolean(bool))));

    if (indexPoints) {
      doc.add(
          new LongPoint(
              FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              value));
      doc.add(
          new LongPoint(
              FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              LuceneDoubleConversionUtils.toMqlSortableLong(value)));
      doc.add(
          new StringField(
              FieldName.TypeField.NULL.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              new BytesRef(FieldValue.NULL_FIELD_VALUE),
              Field.Store.NO));
      doc.add(
          new StringField(
              FieldName.TypeField.TOKEN.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              intToBytesRef(value),
              Field.Store.NO));
      doc.add(
          new StringField(
              FieldName.TypeField.UUID.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              uuidStr,
              Field.Store.NO));
      doc.add(
          new Field(
              FieldName.TypeField.OBJECT_ID.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              objectId.toByteArray(),
              new FieldTypeBuilder()
                  .withIndexOptions(IndexOptions.DOCS)
                  .tokenized(false)
                  .stored(false)
                  .build()));
      doc.add(
          new StringField(
              FieldName.TypeField.BOOLEAN.getLuceneFieldName(FIELD_NAME, Optional.empty()),
              new BytesRef(FieldValue.fromBoolean(bool)),
              Field.Store.NO));
    }

    return doc;
  }

  public static IndexSearcher indexDocs(
      List<Document> docs, IndexWriter indexWriter, int maxDocsPerSegment) throws IOException {
    for (int i = 0; i < docs.size(); i++) {
      if (i != 0 && i % maxDocsPerSegment == 0) {
        indexWriter.flush();
      }
      indexWriter.addDocument(docs.get(i));
    }
    indexWriter.flush(); // flush any remaining docs into last segment

    IndexReader reader = DirectoryReader.open(indexWriter);
    // single threaded so totalhits is deterministic
    return new IndexSearcher(reader);
  }

  private static BytesRef intToBytesRef(long value) {
    return new BytesRef(Character.toString((char) value));
  }

  /**
   * The match-all query pruning tests should use. Lucene 10.4 scores a bare {@code
   * MatchAllDocsQuery} via {@code DenseConjunctionBulkScorer}, which only consults the comparator's
   * competitive iterator between fixed 4096-doc windows, defeating per-doc pruning. Production
   * mitigates this by wrapping sorted queries in {@link DisableBulkScorerQuery} (see {@code
   * LuceneSearchIndexReader#maybeDisableBulkScorerForSort}, flag default on), which forces {@code
   * Weight.DefaultBulkScorer} and restores per-doc pruning. Tests wrap the same way so they
   * exercise the shipped configuration and hit counts stay exact.
   */
  public static Query matchAllDocsQuery() {
    return new DisableBulkScorerQuery(new MatchAllDocsQuery());
  }

  public static void assertNonCompetitiveHitsAreSkipped(long collectedHits, long numDocs) {
    Truth.assertWithMessage("Pruning did not occur").that(collectedHits).isLessThan(numDocs);
  }

  static void assertExplain(TopDocs topDocs, long numDocs, SortStats sortStats) {
    long collectedHits =
        sortStats
            .stats()
            .get()
            .prunedResultIterator()
            .invocationCounts()
            .get()
            .getOrDefault(ExplainTimings.Type.NEXT_DOC.getName(), Long.MAX_VALUE);
    Truth.assertWithMessage("PruningIterator.nextDoc cannot be less than totalHits.value")
        .that(collectedHits)
        .isAtLeast(topDocs.totalHits.value());

    Truth.assertWithMessage("PruningIterator.nextDoc cannot be greater than numDocs")
        .that(collectedHits)
        .isLessThan(numDocs);
  }

  /**
   * This method should be used to verify the different conditions of TopDocs indicating that
   * pruning has not occurred.
   */
  static void assertUnoptimizedSort(TopDocs topDocs, int numHits, int numDocs) {
    Truth.assertThat(topDocs.scoreDocs.length).isEqualTo(numHits);
    Truth.assertThat(topDocs.totalHits.value())
        .isEqualTo(numDocs); // assert that all documents were collected => optimization was not run
  }

  /**
   * This method should be used to verify the different conditions of TopDocs indicating that
   * pruning has occurred when an ascending sort is specified. Some assumptions this method makes
   * are:
   *
   * <ul>
   *   <li>They must be generated by a sorted query
   *   <li>The result of field[0] must be homogenous
   *   <li>Other sort fields are not checked by this method and should be verified externally
   * </ul>
   */
  static void assertOptimizedSort(TopDocs topDocs, int numHits, int numDocs) {
    assertOptimizedSort(topDocs, numHits, numDocs, c -> (Comparable<?>) c);
  }

  static void assertOptimizedSort(
      TopDocs topDocs, int numHits, int numDocs, Function<Object, Comparable<?>> toComparable) {
    Truth.assertThat(topDocs.scoreDocs.length).isEqualTo(numHits);
    Truth.assertThat(topDocs.totalHits.relation())
        .isEqualTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

    Truth.assertThat(
            Arrays.stream(topDocs.scoreDocs)
                .map(s -> ((FieldDoc) s).fields[0])
                .map(toComparable)
                .toList())
        .isInOrder();

    assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value(), numDocs);
  }

  /**
   * This method should be used to verify the different conditions of TopDocs indicating that
   * pruning has occurred when descending sort is specified. Some assumptions this method makes are:
   *
   * <ul>
   *   <li>TopDocs must be generated by a sorted query
   *   <li>The result of field[0] must be homogenous
   *   <li>Other sort fields are not checked by this method and should be verified externally
   * </ul>
   */
  static void assertReversedOptimizedSort(TopDocs topDocs, int numHits, int numDocs) {
    assertReversedOptimizedSort(topDocs, numHits, numDocs, c -> (Comparable<?>) c);
  }

  static void assertReversedOptimizedSort(
      TopDocs topDocs, int numHits, int numDocs, Function<Object, Comparable<?>> toComparable) {
    Truth.assertThat(topDocs.scoreDocs.length).isEqualTo(numHits);
    Truth.assertThat(topDocs.totalHits.relation())
        .isEqualTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

    Truth.assertThat(
            Arrays.stream(topDocs.scoreDocs)
                .map(s -> ((FieldDoc) s).fields[0])
                .map(toComparable)
                .toList())
        .isInOrder(Comparator.reverseOrder());

    assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value(), numDocs);
  }

  static void assertNullOptimizedSort(
      TopDocs topDocs, int numHits, int numDocs, NullEmptySortPosition nullEmptySortPosition) {
    Truth.assertThat(topDocs.scoreDocs.length).isEqualTo(numHits);
    Truth.assertThat(topDocs.totalHits.relation())
        .isEqualTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

    List<Object> values =
        Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toList();

    BsonValue uniformValue =
        nullEmptySortPosition == NullEmptySortPosition.HIGHEST
            ? BsonUtils.MAX_KEY
            : BsonUtils.MIN_KEY;
    Truth.assertThat(values).isEqualTo(Collections.nCopies(numHits, uniformValue));

    assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value(), numDocs);
  }

  static void assertOptimizedSortForNullFieldType(
      TopDocs topDocs, int numHits, int numDocs, NullEmptySortPosition nullEmptySortPosition) {
    assertNullOptimizedSort(topDocs, numHits, numDocs, nullEmptySortPosition);
    // All values are equal so verify that Lucene sorted all the doc ids in asc order
    Truth.assertThat(Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList())
        .isInOrder();
  }

  /**
   * This method should be used to for validation when a SortField of Type.SCORE is used in tests.
   * It accepts the expected score for all the documents and the index at which the SortField of
   * Type.SCORE is specified.
   */
  static void assertScore(TopDocs docs, int scoreIdx, float expectedScore) {
    List<Float> expected = Collections.nCopies(docs.scoreDocs.length, expectedScore);
    List<Float> actual =
        Arrays.stream(docs.scoreDocs)
            .map(scoreDoc -> (FieldDoc) scoreDoc)
            .map(fieldDoc -> fieldDoc.fields[scoreIdx])
            .map(x -> (Float) x)
            .collect(Collectors.toList());
    Truth.assertThat(actual).isEqualTo(expected);
  }

  static class TestRunner {
    private Directory directory;
    private IndexWriter writer;
    private final CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException> testFunc;

    TestRunner(CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException> testFunc) {
      this.testFunc = testFunc;
    }

    private void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();
      IndexWriterConfig config = new IndexWriterConfig();
      config.setMergePolicy(NoMergePolicy.INSTANCE);
      this.writer = new IndexWriter(this.directory, config);
    }

    private void cleanup() throws IOException {
      this.writer.close();
      this.directory.close();
    }

    void runTest() throws IOException {
      setup();

      // NB: lucene ScoreDoc does not implement equals() so compare the docs as strings instead.
      var pruningEnabledResults =
          this.testFunc.apply(true, this.writer).stream()
              .map(topDocs -> Arrays.stream(topDocs.scoreDocs).map(ScoreDoc::toString).toList())
              .toList();

      // cleanup and setup again before running test without pruning
      cleanup();
      setup();

      var pruningDisabledResults =
          this.testFunc.apply(false, this.writer).stream()
              .map(topDocs -> Arrays.stream(topDocs.scoreDocs).map(ScoreDoc::toString).toList())
              .toList();
      Truth.assertThat(pruningDisabledResults).isEqualTo(pruningEnabledResults);
    }
  }
}
