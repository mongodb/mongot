package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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
import org.apache.lucene.util.BytesRef;
import org.bson.BsonNull;
import org.bson.BsonString;
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
      MqlStringSortTest.ClassTest.class,
      MqlStringSortTest.PruningTokenFieldTest.class,
      MqlStringSortTest.PruningNullFieldTest.class
    })
public class MqlStringSortTest {
  public static class ClassTest {
    private Directory directory;
    private IndexSearcher searcher;

    private static final String stringField =
        FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.newRoot("field"), Optional.empty());
    private static final String nullField =
        FieldName.TypeField.NULL.getLuceneFieldName(
            FieldPath.newRoot("nullableField"), Optional.empty());

    private static Document createDoc(String value) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesField(stringField, new BytesRef(value)));
      doc.add(new SortedDocValuesField(nullField, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
      return doc;
    }

    /** Sets up test. */
    @Before
    public void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();
      IndexWriterConfig config = new IndexWriterConfig();

      ImmutableList<Document> docs =
          ImmutableList.of(
              createDoc("a"), createDoc("b"), createDoc("c"), createDoc("d"), new Document());

      try (IndexWriter writer = new RandomSegmentingIndexWriter(this.directory, config)) {
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
    public void sortTokenAsc() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
              true,
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY,
              new BsonString("a"),
              new BsonString("b"),
              new BsonString("c"),
              new BsonString("d"))
          .inOrder();
    }

    @Test
    public void sortTokenAscNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  FieldPath.newRoot("field"), SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              true,
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(
              new BsonString("a"),
              new BsonString("b"),
              new BsonString("c"),
              new BsonString("d"),
              BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortTokenAscSearchAfter() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
              true,
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values).asList().containsExactly(BsonUtils.MIN_KEY, new BsonString("a")).inOrder();

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
          .containsExactly(new BsonString("b"), new BsonString("c"), new BsonString("d"))
          .inOrder();
    }

    @Test
    public void sortTokenAscSearchAfterNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  FieldPath.newRoot("field"), SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              true,
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();
      assertThat(values)
          .asList()
          .containsExactly(new BsonString("a"), new BsonString("b"))
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
          .containsExactly(new BsonString("c"), new BsonString("d"), BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortNullAsc() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          new MqlMixedSort(
              new MongotSortField(
                  FieldPath.newRoot("nullableField"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY);
    }

    @Test
    public void sortNullAscNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          new MqlMixedSort(
              new MongotSortField(
                  FieldPath.newRoot("nullableField"),
                  SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty());
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY);
    }
  }

  /**
   * Tests adapted from <a
   * href="https://github.com/apache/lucene/blob/72968d30ba1761a9bd7d584471397e05f0fb8fd6/lucene/core/src/test/org/apache/lucene/search/testsortoptimization.java">lucene
   * pruning tests</a>.
   */
  @RunWith(Parameterized.class)
  public static class PruningTokenFieldTest {
    public static final FieldName.TypeField FIELD_TYPE = FieldName.TypeField.TOKEN;
    SortPruningTestUtils.TestRunner testRunner;

    public PruningTokenFieldTest(
        CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException> testFunc) {
      this.testRunner = new SortPruningTestUtils.TestRunner(testFunc);
    }

    @Parameterized.Parameters(name = "PruningTokenFieldTest-{index}")
    public static Iterable<CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException>>
        data() {
      return List.of(
          PruningTokenFieldTest::testStringSortExplain,
          PruningTokenFieldTest::testSortExplainAfterAscOptimization,
          PruningTokenFieldTest::testStringSortAscOptimization,
          PruningTokenFieldTest::testStringSortAscOptimizationNullsHighest,
          PruningTokenFieldTest::testStringSortDescOptimization,
          PruningTokenFieldTest::testStringSortDescOptimizationNullsHighest,
          PruningTokenFieldTest::testSortAfterAscOptimization,
          PruningTokenFieldTest::testSortAfterAscOptimizationNullsHighest,
          PruningTokenFieldTest::testSecondarySortNoOptimization,
          PruningTokenFieldTest::testTokenThenScoreSecondarySortOptimization,
          PruningTokenFieldTest::testSortOptimizationEqualValues);
    }

    @Test
    public void runTest() throws IOException {
      this.testRunner.runTest();
    }

    static List<TopDocs> testStringSortExplain(boolean enablePruning, IndexWriter writer)
        throws IOException {
      try (var unused =
          Explain.setup(
              Optional.of(Explain.Verbosity.EXECUTION_STATS),
              Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
        var mongotSortField =
            new MongotSortField(SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC);

        SortFeatureExplainer explainer =
            Explain.getQueryInfo()
                .get()
                .getFeatureExplainer(
                    SortFeatureExplainer.class,
                    () ->
                        new SortFeatureExplainer(
                            SortSpecBuilder.builder().sortField(mongotSortField).buildSort(),
                            ImmutableSetMultimap.of(
                                mongotSortField.field(), FieldName.TypeField.TOKEN)));

        int numDocs = 11000;
        int numHits = 5;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
                .collect(Collectors.toList());
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

        // simple ascending sort
        SortField sortField =
            MqlSortedSetSortField.stringSort(
                FIELD_TYPE, mongotSortField, enablePruning, Optional.empty(), false);
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, numHits);
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

    static List<TopDocs> testSortExplainAfterAscOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      try (var unused =
          Explain.setup(
              Optional.of(Explain.Verbosity.EXECUTION_STATS),
              Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
        var mongotSortField =
            new MongotSortField(SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC);

        SortFeatureExplainer explainer =
            Explain.getQueryInfo()
                .get()
                .getFeatureExplainer(
                    SortFeatureExplainer.class,
                    () ->
                        new SortFeatureExplainer(
                            SortSpecBuilder.builder().sortField(mongotSortField).buildSort(),
                            ImmutableSetMultimap.of(
                                mongotSortField.field(), FieldName.TypeField.TOKEN)));

        int numDocs = 11000;
        int numHits = 5;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
                .collect(Collectors.toList());
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

        int afterNum = 2;
        SortField sortField =
            MqlSortedSetSortField.stringSort(
                FieldName.TypeField.TOKEN, mongotSortField, enablePruning, Optional.empty(), false);
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        BsonValue afterValue = new BsonString(Character.toString((char) afterNum));
        FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, after, numHits);
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

    public static List<TopDocs> testStringSortAscOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortAscOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      // Index docs with some of them containing missing values
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> x % 100 != 0))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortDescOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(numDocs - value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple descending sort
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_DESC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortDescOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(numDocs - value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple descending sort
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_DESC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortAfterAscOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      int afterNum = 2;
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);
      BsonValue afterValue = new BsonString(Character.toString((char) afterNum));
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortAfterAscOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      int afterNum = 2;
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      BsonValue afterValue = new BsonString(Character.toString((char) afterNum));
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSecondarySortNoOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(SortField.FIELD_SCORE, sortField);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    public static List<TopDocs> testTokenThenScoreSecondarySortOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField, SortField.FIELD_SCORE);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      // test that if there is the secondary sort on _score, hits are still skipped
      SortPruningTestUtils.assertScore(topDocs, 1, 1.0f);
      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
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
          MqlSortedSetSortField.stringSort(
              FIELD_TYPE,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
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

  @RunWith(Parameterized.class)
  public static class PruningNullFieldTest {

    SortPruningTestUtils.TestRunner testRunner;

    public PruningNullFieldTest(
        CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException> testFunc) {
      this.testRunner = new SortPruningTestUtils.TestRunner(testFunc);
    }

    @Parameterized.Parameters(name = "PruningNullFieldTest-{index}")
    public static Iterable<CheckedBiFunction<Boolean, IndexWriter, List<TopDocs>, IOException>>
        data() {
      return List.of(
          PruningNullFieldTest::testStringSortAscOptimization,
          PruningNullFieldTest::testStringSortAscOptimizationNullsHighest,
          PruningNullFieldTest::testStringSortDescOptimization,
          PruningNullFieldTest::testStringSortDescOptimizationNullsHighest,
          PruningNullFieldTest::testSortAfterAscOptimization,
          PruningNullFieldTest::testSortAfterAscOptimizationNullsHighest,
          PruningNullFieldTest::testNullSortAfterAscNoOptimization,
          PruningNullFieldTest::testNullThenScoreSecondarySortNoOptimization,
          PruningNullFieldTest::testSecondarySortNoOptimization,
          PruningNullFieldTest::testSortOptimizationEqualValues);
    }

    @Test
    public void runTest() throws IOException {
      this.testRunner.runTest();
    }

    public static List<TopDocs> testStringSortAscOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple ascending sort
      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortAscOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      // Index docs with some of them containing missing values
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> x % 100 != 0))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple ascending sort
      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty());
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortDescOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(numDocs - value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple descending sort
      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_DESC),
              Optional.empty());
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      return List.of(topDocs);
    }

    public static List<TopDocs> testStringSortDescOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(numDocs - value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      // simple descending sort
      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_DESC_NULLS_HIGHEST),
              Optional.empty());
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortAfterAscOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      int afterNum = 2;
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);
      BsonValue afterValue = new BsonString(Character.toString((char) afterNum));
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testSortAfterAscOptimizationNullsHighest(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      int afterNum = 2;
      SortField sortField =
          MqlSortedSetSortField.stringSort(
              FieldName.TypeField.TOKEN,
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              enablePruning,
              Optional.empty(), false);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      BsonValue afterValue = new BsonString(Character.toString((char) afterNum));
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      if (enablePruning) {
        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      }
      return List.of(topDocs);
    }

    public static List<TopDocs> testNullSortAfterAscNoOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Sort sort = new Sort(sortField);

      BsonValue afterValue = BsonNull.VALUE;
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      // singleSort is disabled if an "after" FieldDoc is specified. See:
      // https://github.com/apache/lucene/blob/ee3d60ff92961c744e61b56a2b6272983b3b69dd/lucene/core/src/java/org/apache/lucene/search/TopFieldCollectorManager.java#L153-L156
      // since singleSort is false, due to this line:
      // https://github.com/apache/lucene/blob/branch_9_7/lucene/core/src/java/org/apache/lucene/search/comparators/TermOrdValComparator.java#L419,
      // Lucene does not ignore values that are equal to the bottom "weakest" value, which in this
      // case is just "N", so the "competitive" doc id iterator is not decreased and Lucene still
      // looks through all the hits.
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      return List.of(topDocs);
    }

    public static List<TopDocs> testNullThenScoreSecondarySortNoOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Sort sort = new Sort(sortField, SortField.FIELD_SCORE);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      // Tests that if there is the secondary sort on _score, hits are not skipped when sorting over
      // null values. For reference, all documents contain the same null field value "N". Due to
      // this line,
      // https://github.com/apache/lucene/blob/branch_9_7/lucene/core/src/java/org/apache/lucene/search/comparators/TermOrdValComparator.java#L419,
      // if a compound sort occurs, Lucene does not ignore values that are equal to the bottom
      // "weakest" value, which in this case is just "N", so the "competitive" doc id iterator is
      // not decreased and Lucene still looks through all the hits.
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
      SortPruningTestUtils.assertScore(topDocs, 1, 1.0f);
      return List.of(topDocs);
    }

    public static List<TopDocs> testSecondarySortNoOptimization(
        boolean enablePruning, IndexWriter writer) throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, writer, 2000);

      SortField sortField =
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Sort sort = new Sort(SortField.FIELD_SCORE, sortField);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
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
          new MqlMixedSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Sort sort = new Sort(sortField);

      // test that sorting on a single field with equal values uses the optimization
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      return List.of(topDocs);
    }
  }
}
