package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.query.sort.SortPruningTestUtils.FIELD_NAME;
import static org.apache.lucene.search.SortField.FIELD_SCORE;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Streams;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MqlDoubleSortTest.ClassTest.class,
      MqlDoubleSortTest.PruningTest.class,
    })
public class MqlDoubleSortTest {
  public static class ClassTest {
    private Directory directory;
    private IndexSearcher searcher;
    private static final String doubleField =
        FieldName.TypeField.SORTABLE_NUMBER_BETA_V1.getLuceneFieldName(
            FieldPath.newRoot("field"), Optional.empty());

    private static Document createDoc(double d) {
      Document doc = new Document();
      long sortableLong = LuceneDoubleConversionUtils.toMqlSortableLong(d);
      doc.add(new SortedNumericDocValuesField(doubleField, sortableLong));
      return doc;
    }

    @Before
    public void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();

      ImmutableList<Document> docs =
          ImmutableList.of(
              createDoc(Double.POSITIVE_INFINITY),
              createDoc(Double.NEGATIVE_INFINITY),
              createDoc(Double.NaN),
              createDoc(Double.MAX_VALUE),
              createDoc(-Double.MAX_VALUE),
              createDoc(-Double.MIN_VALUE),
              createDoc(Double.MIN_VALUE),
              createDoc(Double.MIN_NORMAL),
              createDoc(-0.0),
              createDoc(0.0),
              new Document());

      try (var writer = new RandomSegmentingIndexWriter(this.directory)) {
        writer.addDocuments(docs);
      }

      DirectoryReader reader = DirectoryReader.open(this.directory);
      this.searcher = new IndexSearcher(reader);
    }

    @After
    public void cleanup() throws IOException {
      this.searcher.getIndexReader().close();
      this.directory.close();
    }

    @Test
    public void sortAscNullsLowest() throws IOException {
      SortField sortField = createSortField(SortOrder.ASC, NullEmptySortPosition.LOWEST);
      Object[] values = runSortQuery(sortField);
      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY,
              new BsonDouble(Double.NaN),
              new BsonDouble(Double.NEGATIVE_INFINITY),
              new BsonDouble(-Double.MAX_VALUE),
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.POSITIVE_INFINITY))
          .inOrder();
    }

    @Test
    public void sortAscNullsHighest() throws IOException {
      SortField sortField = createSortField(SortOrder.ASC, NullEmptySortPosition.HIGHEST);
      Object[] values = runSortQuery(sortField);
      assertThat(values)
          .asList()
          .containsExactly(
              new BsonDouble(Double.NaN),
              new BsonDouble(Double.NEGATIVE_INFINITY),
              new BsonDouble(-Double.MAX_VALUE),
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.POSITIVE_INFINITY),
              BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortDescNullsLowest() throws IOException {
      SortField sortField = createSortField(SortOrder.DESC, NullEmptySortPosition.LOWEST);
      Object[] values = runSortQuery(sortField);
      assertThat(values)
          .asList()
          .containsExactly(
              new BsonDouble(Double.POSITIVE_INFINITY),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(-Double.MAX_VALUE),
              new BsonDouble(Double.NEGATIVE_INFINITY),
              new BsonDouble(Double.NaN),
              BsonUtils.MIN_KEY)
          .inOrder();
    }

    @Test
    public void sortDescNullsHighest() throws IOException {
      SortField sortField = createSortField(SortOrder.DESC, NullEmptySortPosition.HIGHEST);
      Object[] values = runSortQuery(sortField);
      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MAX_KEY,
              new BsonDouble(Double.POSITIVE_INFINITY),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(-Double.MAX_VALUE),
              new BsonDouble(Double.NEGATIVE_INFINITY),
              new BsonDouble(Double.NaN))
          .inOrder();
    }

    @Test
    public void sortAscSearchAfterNullsLowest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          new MqlDoubleSort(
              FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
              new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
              true,
              Optional.empty(),
                  false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 3, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY,
              new BsonDouble(Double.NaN),
              new BsonDouble(Double.NEGATIVE_INFINITY))
          .inOrder();

      TopDocs afterDocs =
          this.searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              query,
              Integer.MAX_VALUE,
              sort,
              false);

      Object[] afterValues =
          Arrays.stream(afterDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(afterValues)
          .asList()
          .containsExactly(
              new BsonDouble(-Double.MAX_VALUE),
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.POSITIVE_INFINITY))
          .inOrder();
    }

    @Test
    public void sortAscSearchAfterNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          new MqlDoubleSort(
              FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
              new MongotSortField(
                  FieldPath.newRoot("field"), SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              true,
              Optional.empty(),
                  false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 3, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(
              new BsonDouble(Double.NaN),
              new BsonDouble(Double.NEGATIVE_INFINITY),
              new BsonDouble(-Double.MAX_VALUE))
          .inOrder();

      TopDocs afterDocs =
          this.searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              query,
              Integer.MAX_VALUE,
              sort,
              false);

      Object[] afterValues =
          Arrays.stream(afterDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(afterValues)
          .asList()
          .containsExactly(
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.POSITIVE_INFINITY),
              BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortSearchAfterWithLong() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          new MqlDoubleSort(
              FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
              new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
              true,
              Optional.empty(),
                  false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs =
          this.searcher.searchAfter(
              new FieldDoc(-1, 0.0f, new Object[] {new BsonInt64(-100L)}),
              query,
              Integer.MAX_VALUE,
              sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(
              new BsonDouble(-Double.MIN_VALUE),
              new BsonDouble(0.0),
              new BsonDouble(0.0),
              new BsonDouble(Double.MIN_VALUE),
              new BsonDouble(Double.MIN_NORMAL),
              new BsonDouble(Double.MAX_VALUE),
              new BsonDouble(Double.POSITIVE_INFINITY))
          .inOrder();
    }

    private SortField createSortField(SortOrder order, NullEmptySortPosition nullPosition) {
      SortOptions options = new UserFieldSortOptions(order, nullPosition);
      return new MqlDoubleSort(
          FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
          new MongotSortField(FieldPath.newRoot("field"), options),
          true,
          Optional.empty(),
              false);
    }

    private Object[] runSortQuery(SortField sortField) throws IOException {
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortField);
      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);
      return Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
    }
  }

  /**
   * Tests adapted from <a
   * href="https://github.com/apache/lucene/blob/72968d30ba1761a9bd7d584471397e05f0fb8fd6/lucene/core/src/test/org/apache/lucene/search/testsortoptimization.java">lucene
   * pruning tests</a>.
   */
  @RunWith(Parameterized.class)
  public static class PruningTest {
    private static final FieldName.TypeField FIELD_TYPE = FieldName.TypeField.NUMBER_DOUBLE_V2;

    SortPruningTestUtils.TestRunner testRunner;

    public PruningTest(
        CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException> testFunc) {
      this.testRunner = new SortPruningTestUtils.TestRunner(testFunc);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException>>
        data() {
      return List.of(
          PruningTest::testSortOptimizationExplain,
          PruningTest::testSortOptimizationExplainAfter,
          PruningTest::testSortOptimizationSimpleAndAfter,
          PruningTest::testSortOptimizationSimpleAndAfterNullsHighest,
          PruningTest::testSortOptimizationSecondarySortScore,
          PruningTest::testMinValueAscPruned,
          PruningTest::testMinValueAscPrunedNullsHighest,
          PruningTest::testMinValueDescPruned,
          PruningTest::testSearchAfterTopNullBottomNonNullAscPruned,
          PruningTest::testSearchAfterTopNullBottomNullAscPruned,
          PruningTest::testSearchAfterTopNullBottomNonNullDescPrunedNullsHighest,
          PruningTest::testSearchAfterTopNullBottomNullDescPrunedNullsHighest,
          PruningTest::testSearchAfterDescTopNonNullBottomNull,
          PruningTest::testSortOptimizationEqualValues,
          PruningTest::testSortOptimizationWithNonCompetitiveMissingValue,
          PruningTest::testSortOptimizationWithNonCompetitiveMissingValueNullsHighest,
          PruningTest::testSortOptimizationMissingValueNonCompetitiveWhenBottomNullSingleSort,
          PruningTest::testNoSortOptimizationMissingValueCompetitiveWhenBottomNullCompoundSort,
          PruningTest::testNoSortOptimizationWithCompetitiveMissingValue,
          PruningTest::testSortOptimizationWithMultipleCompetitiveMissingValue,
          PruningTest::testNoOptimizationOnFieldNotIndexedWithPoints,
          PruningTest::testNoOptimizationIfSecondarySort);
    }

    @Test
    public void runTest() throws IOException {
      this.testRunner.runTest();
    }

    static List<TopDocs> testSortOptimizationExplain(boolean enablePruning, IndexWriter writer)
        throws IOException {
      try (var unused =
          Explain.setup(
              Optional.of(Explain.Verbosity.EXECUTION_STATS),
              Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
        var mongotSortField = new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC);

        SortFeatureExplainer explainer =
            Explain.getQueryInfo()
                .get()
                .getFeatureExplainer(
                    SortFeatureExplainer.class,
                    () ->
                        new SortFeatureExplainer(
                            SortSpecBuilder.builder().sortField(mongotSortField).buildSort(),
                            ImmutableSetMultimap.of(
                                mongotSortField.field(), FieldName.TypeField.NUMBER_DOUBLE_V2)));

        int numDocs = 11000;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
                .collect(Collectors.toList());
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
        SortField sortField =
            new MqlDoubleSort(FIELD_TYPE, mongotSortField, enablePruning, Optional.empty(),
                false);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        int numHits = 3;
        int totalHitsThreshold = 3;

        // simple sort
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

        if (enablePruning) {
          SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
          SortStats sortExplainResult =
              Explain.collect().get().collectStats().get().sortStats().get();
          SortPruningTestUtils.assertExplain(topDocs, numDocs, sortExplainResult);
        }

        return List.of(topDocs);
      }
    }

    static List<TopDocs> testSortOptimizationExplainAfter(boolean enablePruning, IndexWriter writer)
        throws IOException {
      try (var unused =
          Explain.setup(
              Optional.of(Explain.Verbosity.EXECUTION_STATS),
              Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
        var mongotSortField = new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC);

        SortFeatureExplainer explainer =
            Explain.getQueryInfo()
                .get()
                .getFeatureExplainer(
                    SortFeatureExplainer.class,
                    () ->
                        new SortFeatureExplainer(
                            SortSpecBuilder.builder().sortField(mongotSortField).buildSort(),
                            ImmutableSetMultimap.of(
                                mongotSortField.field(), FieldName.TypeField.NUMBER_DOUBLE_V2)));

        int numDocs = 11000;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
                .collect(Collectors.toList());
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
        SortField sortField =
            new MqlDoubleSort(FIELD_TYPE, mongotSortField, enablePruning, Optional.empty(),
                false);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        int numHits = 3;
        int totalHitsThreshold = 3;

        BsonValue afterValue = new BsonDouble(2.0);
        FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

        if (enablePruning) {
          SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
          SortStats sortExplainResult =
              Explain.collect().get().collectStats().get().sortStats().get();
          SortPruningTestUtils.assertExplain(topDocs, numDocs, sortExplainResult);
        }

        return List.of(topDocs);
      }
    }

    public static List<TopDocs> testSortOptimizationSimpleAndAfter(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(sortField);
      int numHits = 3;
      int totalHitsThreshold = 3;
      List<TopDocs> res = new ArrayList<>();

      { // simple sort
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        res.add(topDocs);
      }

      { // paging sort with after
        BsonValue afterValue = new BsonDouble(2.0);
        FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        res.add(topDocs);
      }

      if (enablePruning) {
        res.forEach(topDocs -> SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs));
      }

      return res;
    }

    public static List<TopDocs> testSortOptimizationSimpleAndAfterNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(sortField);
      int numHits = 3;
      int totalHitsThreshold = 3;
      List<TopDocs> res = new ArrayList<>();

      { // simple sort
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        res.add(topDocs);
      }

      { // paging sort with after
        BsonValue afterValue = new BsonDouble(2.0);
        FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        res.add(topDocs);
      }

      if (enablePruning) {
        res.forEach(topDocs -> SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs));
      }

      return res;
    }

    /** test that if there is the secondary sort on _score, scores are filled correctly. */
    public static List<TopDocs> testSortOptimizationSecondarySortScore(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(sortField, FIELD_SCORE);
      int numHits = 3;
      int totalHitsThreshold = 3;

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(numHits, topDocs.scoreDocs.length);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
        SortPruningTestUtils.assertScore(topDocs, 1, 1.0f);
      }
      return List.of(topDocs);
    }

    /** Test that if numeric field is a secondary sort, no optimization is run. */
    public static List<TopDocs> testNoOptimizationIfSecondarySort(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, false, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(FIELD_SCORE, sortField);
      int numHits = 3;
      int totalHitsThreshold = 3;

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    /**
     * test that even if a field is not indexed with points, optimized sort still works as expected,
     * although no optimization will be run.
     */
    public static List<TopDocs> testNoOptimizationOnFieldNotIndexedWithPoints(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, false, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(sortField);
      int numHits = 3;
      int totalHitsThreshold = 3;

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    public static List<TopDocs> testNoSortOptimizationWithCompetitiveMissingValue(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(
                  value ->
                      SortPruningTestUtils.createDoc(value, true, (x) -> (x != 5000 && x != 10000)))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      // test that optimization is not run when missing value setting of SortField is competitive
      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(new Sort(ascSortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    /**
     * Tests that if bottom == missing value during a single sort, then optimized sort is run if
     * missing values exist in a segment since they are deemed non-competitive.
     */
    public static List<TopDocs>
        testSortOptimizationMissingValueNonCompetitiveWhenBottomNullSingleSort(
            boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 4000;
      int maxDocsPerSegment = 7000;
      int numMissingValues = 300;

      // Index docs containing missing values in the first part of the segment
      Stream<Document> segMissing =
          IntStream.range(0, numMissingValues)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      // All of these documents contain values that are non-competitive in an ascending sort so will
      // be pruned.
      Stream<Document> segNotMissing =
          IntStream.range(0, numDocs - numMissingValues)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true));

      List<Document> docs = Streams.concat(segMissing, segNotMissing).collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, maxDocsPerSegment);
      int numHits = 3;
      int totalHitsThreshold = 3;

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(new Sort(ascSortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      if (enablePruning) {
        // The first 3 values are eagerly collected, and the sort queue is [null, null, null
        // (bottom)]. The 4th hit is collected and kicks off pruning. The remaining documents in the
        // segment are non-competitive and should be pruned.
        //
        // Missing values after the first 3 docs are non-competitive since we are performing a
        // single sort. Only values that are strictly weaker than bottom should be considered
        // competitive.
        Truth.assertThat(topDocs.totalHits.value()).isEqualTo(4);
        SortPruningTestUtils.assertNullOptimizedSort(
            topDocs, numHits, numDocs, NullEmptySortPosition.LOWEST);
      }
      return List.of(topDocs);
    }

    /**
     * Tests that if bottom == missing value during a compound sort, then no optimization is run if
     * missing values exist in a segment since they are deemed competitive.
     */
    public static List<TopDocs>
        testNoSortOptimizationMissingValueCompetitiveWhenBottomNullCompoundSort(
            boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 4000;
      int maxDocsPerSegment = 7000;
      int numMissingValues = 300;

      // Index docs containing missing values in the first part of the segment
      Stream<Document> segMissing =
          IntStream.range(0, numMissingValues)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      Stream<Document> segNotMissing =
          IntStream.range(0, numDocs - numMissingValues)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true));

      List<Document> docs = Streams.concat(segMissing, segNotMissing).collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, maxDocsPerSegment);
      int numHits = 3;
      int totalHitsThreshold = 3;

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      // Compound sort
      Sort sort = new Sort(ascSortField, FIELD_SCORE);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortOptimizationWithMultipleCompetitiveMissingValue(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(
                  value -> SortPruningTestUtils.createDoc(value, true, (x) -> ((x % 500) != 0)))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      // test that optimization takes place when there are more than 1 competitive missing value.
      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(new Sort(ascSortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      if (enablePruning) {
        SortPruningTestUtils.assertNullOptimizedSort(
            topDocs, numHits, numDocs, NullEmptySortPosition.LOWEST);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortOptimizationWithNonCompetitiveMissingValue(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(
                  value ->
                      SortPruningTestUtils.createDoc(
                          numDocs - value, true, (x) -> ((x % 500) != 0)))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      // test that optimization is run when missing value setting of SortField is NOT competitive
      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_DESC),
              enablePruning,
              Optional.empty(),
                  false);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(new Sort(descSortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortOptimizationWithNonCompetitiveMissingValueNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 4000;
      // Index docs with a few of them containing missing values
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> x % 1000 != 0))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      // Test that optimization is run when missing value setting of SortField is NOT competitive
      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(),
                  false);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(new Sort(descSortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        // Collect 3 hits into sort queue. Collecting 4th hit triggers pruning of remaining docs in
        // segment. In ascending sort with nulls: highest, docs with missing values are
        // non-competitive.
        Truth.assertThat(topDocs.totalHits.value()).isEqualTo(4);
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    /**
     * Simulates a scenario that prunes in an ascending sort when bottom has been set to null based
     * on the first segment. Lucene requires < 500 competitive documents in a 4k segment to trigger
     * pruning. Note that if Lucene changes the heuristic behavior this test may break.
     */
    public static List<TopDocs> testMinValueAscPruned(boolean enablePruning, IndexWriter writer)
        throws IOException {
      int numDocs = 11000;
      int maxDocsPerSegment = 7000;
      int numHits = 3;
      int numDocsToPrune = 3700;
      Stream<Document> firstSeg =
          IntStream.range(0, maxDocsPerSegment)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      // All of these documents contain values that are non-competitive in an ascending sort so will
      // be pruned.
      Stream<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true));

      // Docs with lowest possible double value = Double.NaN
      Stream<Document> secondSegMin =
          IntStream.range(0, numDocs - (maxDocsPerSegment + numDocsToPrune))
              .mapToObj(ignored -> new Document())
              .peek(
                  document -> {
                    document.add(
                        new NumericDocValuesField(
                            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN)));

                    document.add(
                        new LongPoint(
                            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN)));
                  });

      List<Document> docs =
          Streams.concat(firstSeg, secondSegPruned, secondSegMin).collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, maxDocsPerSegment);

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(ascSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, 3));

      if (enablePruning) {
        // In doubleSort documents with the lowest possible double (Double.NaN) are not considered
        // competitive
        Truth.assertThat(topDocs.totalHits.value()).isEqualTo(7000);
        SortPruningTestUtils.assertNullOptimizedSort(
            topDocs, numHits, numDocs, NullEmptySortPosition.LOWEST);
      }
      return List.of(topDocs);
    }

    /**
     * Simulates a scenario that prunes in an ascending sort with noData: "highest" when bottom has
     * been set to null based on the first segment. Lucene requires < 500 competitive documents in a
     * 4k segment to trigger pruning. Note that if Lucene changes the heuristic behavior this test
     * may break.
     *
     * <p>This behavior also tests that bottom is encoded correctly as Long.MAX_VALUE when pruning
     * with noData: "highest". If bottom is encoded as Long.MIN_VALUE, this test will break.
     */
    public static List<TopDocs> testMinValueAscPrunedNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      // Update comments below if modifying these values
      int numDocsContainingField = 11000;
      int maxDocsPerSegment = 7000;
      int numHits = 3;
      int numDocsToPrune = 3997;
      // These docs are empty i.e. field is missing
      Stream<Document> firstSeg =
          IntStream.range(0, maxDocsPerSegment)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      // The first 3 documents in this segment contain competitive values in an ascending sort, but
      // the remaining documents will be pruned.
      Stream<Document> secondSegPruned =
          IntStream.range(0, (numHits + numDocsToPrune))
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true));

      List<Document> docs = Streams.concat(firstSeg, secondSegPruned).collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, maxDocsPerSegment);

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(ascSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, 3));

      if (enablePruning) {
        // Lucene collects all 7000 docs containing missing values, with its sort priority queue
        // as [null, null, null (bottom)] after the first 3 hits are collected. Competitive
        // documents from this point on should be documents that contain values < Long.MAX_VALUE.
        // Since all remaining documents satisfy this in the segment, no values in the first segment
        // are pruned.

        // In the second segment the bottom value is updated 3 times, collecting the first 3 hits
        // since they're competitive. The queue is now [0, 1, 2 (bottom)]. Lucene prunes the 3997
        // documents containing values in the range from [3, 4000) since they are not competitive in
        // an ascending sort. Thus, the total number of hits collected should be 7000 + 3 = 7003.

        // If bottom is encoded incorrectly, i.e. as Long.MIN_VALUE, the range query performed
        // during pruning to find competitive documents in the first segment would be
        // < Long.MIN_VALUE instead of < Long.MAX_VALUE and all 4000 documents in the second segment
        // would be pruned. The resulting queue would be unchanged and end up as
        // [null, null, null (bottom)], which is incorrect.
        Truth.assertThat(topDocs.totalHits.value()).isEqualTo(7003);
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocsContainingField);
      }
      return List.of(topDocs);
    }

    /**
     * In this test we are simulating a scenario where pruning is triggered when top=null and
     * bottom!=null. In order for that to happen, we must execute an initial search() where the last
     * document in the result set contains a null value for the field we are sorting over. In the
     * subsequent searchAfter(), top will be set to null and pruning will be triggered when the 2nd
     * segment is visited because after traversing the remainder of the first segment the top k hits
     * in the priority queue will be filled where k=numHits.
     */
    public static List<TopDocs> testSearchAfterTopNullBottomNonNullAscPruned(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numHits = 3;
      int numDocs = 4006;
      int numDocsToPrune = 3700;
      List<Document> firstSegMissingValues =
          IntStream.range(0, numHits)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false))
              .toList();

      List<Document> firstSegNonNull =
          IntStream.range(numHits, numHits * 2)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true))
              .toList();

      List<Document> firstSegDocs = ListUtils.union(firstSegMissingValues, firstSegNonNull);

      // All of these documents contain values that are non-competitive in an ascending
      // sort so will be pruned.
      List<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      // Docs with competitive values
      List<Document> secondSegMin =
          IntStream.range(numDocsToPrune, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true))
              .toList();

      List<Document> secondSegDocs = ListUtils.union(secondSegPruned, secondSegMin);

      SortPruningTestUtils.indexDocs(firstSegDocs, writer, Integer.MAX_VALUE);
      IndexSearcher searcher =
          SortPruningTestUtils.indexDocs(secondSegDocs, writer, Integer.MAX_VALUE);

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(ascSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, 3));
      if (enablePruning) {
        /*
         * Initial search returns 3 documents with only null values for the field being sorted over
         * retrieved from the first segment.
         */
        SortPruningTestUtils.assertNullOptimizedSort(
            topDocs, numHits, numDocs, NullEmptySortPosition.LOWEST);
      }

      TopDocs searchAfterDocs =
          searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              new MatchAllDocsQuery(),
              numHits,
              sort,
              false);
      if (enablePruning) {
        /*
         * The searchAfter() returns 3 documents with double(Long.MIN_VALUE) for the field being
         * sorted over after filling the top k from the first segment and pruning documents in the
         * 2nd segment.
         */
        SortPruningTestUtils.assertOptimizedSort(searchAfterDocs, numHits, numDocs);
      }

      return List.of(topDocs, searchAfterDocs);
    }

    /**
     * Same test scenario as <code>testSearchAfterTopNullBottomNonNullAscPruned</code> except with
     * nullEmptySortPosition=highest.
     */
    public static List<TopDocs> testSearchAfterTopNullBottomNonNullDescPrunedNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numHits = 3;
      int numDocs = 4006;
      int numDocsToPrune = 3700;
      List<Document> firstSegMissingValues =
          IntStream.range(0, numHits)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false))
              .toList();

      List<Document> firstSegNonNull =
          IntStream.range(numHits, numHits * 2)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true))
              .toList();

      List<Document> firstSegDocs = ListUtils.union(firstSegMissingValues, firstSegNonNull);

      // All of these documents contain values that are non-competitive in a descending sort so will
      // be pruned.
      List<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true))
              .toList();

      // Docs with competitive values
      List<Document> secondSegMin =
          IntStream.range(numDocsToPrune, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      List<Document> secondSegDocs = ListUtils.union(secondSegPruned, secondSegMin);

      SortPruningTestUtils.indexDocs(firstSegDocs, writer, Integer.MAX_VALUE);
      IndexSearcher searcher =
          SortPruningTestUtils.indexDocs(secondSegDocs, writer, Integer.MAX_VALUE);

      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.DESC, NullEmptySortPosition.HIGHEST)),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(descSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, 3));
      if (enablePruning) {
        /*
         * Initial search returns 3 documents with only null values for the field being sorted over
         * retrieved from the first segment.
         */
        SortPruningTestUtils.assertNullOptimizedSort(
            topDocs, numHits, numDocs, NullEmptySortPosition.HIGHEST);
      }

      TopDocs searchAfterDocs =
          searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              new MatchAllDocsQuery(),
              numHits,
              sort,
              false);
      if (enablePruning) {
        /*
         * The searchAfter() returns 3 documents with double(Long.MAX_VALUE) for the field being
         * sorted over after filling the top k from the first segment and pruning documents in the
         * 2nd segment.
         */
        SortPruningTestUtils.assertOptimizedSort(searchAfterDocs, numHits, numDocs);
      }

      return List.of(topDocs, searchAfterDocs);
    }

    /**
     * In this test we are simulating a scenario where pruning is triggered when top=null and
     * bottom==null. In order for that to happen, we must execute an initial search() where the last
     * document in the result set contains a null value for the field we are sorting over. In the
     * subsequent searchAfter(), top will be set to null and pruning will be triggered when the 2nd
     * segment is visited because after traversing the remainder of the first segment the top k hits
     * in the priority queue will be filled where k=numHits.
     */
    public static List<TopDocs> testSearchAfterTopNullBottomNullAscPruned(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numHits = 3;
      int numDocs = 4006;
      int numDocsToPrune = 3700;
      List<Document> firstSegMissing =
          IntStream.range(0, numHits * 2)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false))
              .collect(Collectors.toList());

      // All of these documents contain values that are non-competitive in an ascending sort so will
      // be pruned.
      List<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      // All of these documents contain values that are non-competitive in an ascending sort so will
      // be pruned.
      List<Document> secondSegMin =
          IntStream.range(numDocsToPrune, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true))
              .toList();

      List<Document> secondSegDocs = ListUtils.union(secondSegPruned, secondSegMin);

      SortPruningTestUtils.indexDocs(firstSegMissing, writer, Integer.MAX_VALUE);
      IndexSearcher searcher =
          SortPruningTestUtils.indexDocs(secondSegDocs, writer, Integer.MAX_VALUE);

      SortField ascSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(ascSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, numHits));

      TopDocs searchAfterDocs =
          searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              new MatchAllDocsQuery(),
              numHits,
              sort,
              false);

      List<TopDocs> results = List.of(topDocs, searchAfterDocs);
      if (enablePruning) {
        /*
         * Since the first segment contains 6 missing values and the search/searchAfter is executed
         * with numHits=3, no documents from the 2nd segment will be in either result set.
         */
        results.forEach(
            result ->
                SortPruningTestUtils.assertNullOptimizedSort(
                    topDocs, numHits, numDocs, NullEmptySortPosition.LOWEST));
      }

      return results;
    }

    /**
     * Same test scenario as <code>testSearchAfterTopNullBottomNullAscPruned</code> except with
     * nullEmptySortPosition=highest.
     */
    public static List<TopDocs> testSearchAfterTopNullBottomNullDescPrunedNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numHits = 3;
      int numDocs = 4006;
      int numDocsToPrune = 3700;
      List<Document> firstSegMissing =
          IntStream.range(0, numHits * 2)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false))
              .collect(Collectors.toList());

      // All of these documents contain values that are non-competitive in a descending sort so will
      // be pruned.
      List<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true))
              .toList();

      // All of these documents contain values that are non-competitive in a descending sort so will
      // be pruned.
      List<Document> secondSegMin =
          IntStream.range(numDocsToPrune, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      List<Document> secondSegDocs = ListUtils.union(secondSegPruned, secondSegMin);

      SortPruningTestUtils.indexDocs(firstSegMissing, writer, Integer.MAX_VALUE);
      IndexSearcher searcher =
          SortPruningTestUtils.indexDocs(secondSegDocs, writer, Integer.MAX_VALUE);

      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.DESC, NullEmptySortPosition.HIGHEST)),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(descSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, numHits));

      TopDocs searchAfterDocs =
          searcher.searchAfter(
              topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
              new MatchAllDocsQuery(),
              numHits,
              sort,
              false);

      List<TopDocs> results = List.of(topDocs, searchAfterDocs);
      if (enablePruning) {
        /*
         * Since the first segment contains 6 missing values and the search/searchAfter is executed
         * with numHits=3, no documents from the 2nd segment will be in either result set.
         */
        results.forEach(
            result ->
                SortPruningTestUtils.assertNullOptimizedSort(
                    topDocs, numHits, numDocs, NullEmptySortPosition.HIGHEST));
      }

      return results;
    }

    /** Simluates a scenario where top!=null and bottom=null when pruning is triggered. */
    private static List<TopDocs> testSearchAfterDescTopNonNullBottomNull(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numHits = 3;
      int numDocs = 4006;
      int numDocsToPrune = 3700;
      List<Document> firstSegNonNulls =
          IntStream.range(0, numHits)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      List<Document> firstSegNulls =
          IntStream.range(3, numHits * 2)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false))
              .toList();

      // All of these documents contain values that are non-competitive in an ascending sort so will
      // be pruned.
      List<Document> secondSegPruned =
          IntStream.range(0, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true))
              .toList();

      // Docs with competitive values
      List<Document> secondSegMin =
          IntStream.range(numDocsToPrune, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MAX_VALUE, true, x -> true))
              .toList();

      List<Document> firstSegDocs = ListUtils.union(firstSegNonNulls, firstSegNulls);

      List<Document> secondSegDocs = ListUtils.union(secondSegPruned, secondSegMin);

      SortPruningTestUtils.indexDocs(firstSegDocs, writer, Integer.MAX_VALUE);
      IndexSearcher searcher =
          SortPruningTestUtils.indexDocs(secondSegDocs, writer, Integer.MAX_VALUE);

      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_DESC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(descSortField);
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, 3));
      if (enablePruning) {
        /*
         * Initial search yields documents with double(Long.MAX_VALUE) as the field being sorted
         * over.
         */
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }

      TopDocs searchAfterDocs =
          searcher.search(
              new MatchAllDocsQuery(),
              new TopFieldCollectorManager(
                  sort, numHits, (FieldDoc) topDocs.scoreDocs[topDocs.scoreDocs.length - 1], 3));
      if (enablePruning) {
        /*
         * In the searchAfter() the top k documents are populated with the values of the 3 remaining
         * documents from the first segment (which are null), and then pruning is triggered on the
         * 2nd segment resulting in the top k being replaced with documents where the field being
         * sorted over equals double(Long.MAX_VALUE).
         */
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }

      return List.of(topDocs, searchAfterDocs);
    }

    /**
     * Simulates a scenario that prunes in a descending sort when bottom has been set to null based
     * on the first segment. Lucene requires < 500 competitive documents in a 4k segment to trigger
     * pruning. Note that if Lucene changes the heuristic behavior the test may break.
     */
    public static List<TopDocs> testMinValueDescPruned(boolean enablePruning, IndexWriter writer)
        throws IOException {
      int numDocs = 11000;
      int maxDocsPerSegment = 7000;
      int numHits = 3;
      int numDocsToPrune = 3700;
      Stream<Document> firstSeg =
          IntStream.range(0, maxDocsPerSegment)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      // Have a numHits number of documents that are non-null in order to fill up the priorityQueue
      // and trigger pruning
      Stream<Document> secondSegNumHits =
          IntStream.range(0, numHits)
              .mapToObj(value -> SortPruningTestUtils.createDoc(Long.MIN_VALUE, true, x -> true));

      // All of these documents contain missing values so will be pruned
      Stream<Document> secondSegMissing =
          IntStream.range(numHits, numDocsToPrune)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> false));

      // These documents will be considered competitive and the last 3 will make up the result
      Stream<Document> secondSegCompetitive =
          IntStream.range(0, numDocs - (maxDocsPerSegment + numDocsToPrune))
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> true));

      List<Document> docs =
          Streams.concat(firstSeg, secondSegNumHits, secondSegMissing, secondSegCompetitive)
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, maxDocsPerSegment);

      SortField descSortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_DESC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(descSortField);

      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, null, numHits));

      if (enablePruning) {
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortOptimizationEqualValues(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(10, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      SortField sortField =
          new MqlDoubleSort(
              FIELD_TYPE,
              new MongotSortField(FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(),
                  false);
      Sort sort = new Sort(sortField);

      // test that sorting on a single field with equal values uses the optimization
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }
  }
}
