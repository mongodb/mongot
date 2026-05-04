package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.query.sort.SortPruningTestUtils.FIELD_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
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
import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MqlUuidSortTest.ClassTest.class,
      MqlUuidSortTest.PruningTest.class,
    })
public class MqlUuidSortTest {
  public static class ClassTest {
    private Directory directory;
    private IndexSearcher searcher;
    private static final String uuidField =
        FieldName.TypeField.UUID.getLuceneFieldName(
            FieldPath.newRoot("uuidField"), Optional.empty());

    private static Document createDoc(String uuidValue) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesField(uuidField, new BytesRef(uuidValue)));
      return doc;
    }

    /** Sets up test. */
    @Before
    public void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();
      IndexWriterConfig config = new IndexWriterConfig();

      ImmutableList<Document> docs =
          ImmutableList.of(
              createDoc("00000000-0000-0000-0000-000000000000"),
              createDoc("00000000-0000-0000-0000-000000000001"),
              createDoc("00000000-0000-0000-0000-000000000002"),
              createDoc("00000000-0000-0000-0000-000000000003"),
              new Document());

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
    public void sortUuidAsc() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(FieldPath.newRoot("uuidField"), UserFieldSortOptions.DEFAULT_ASC),
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
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000000")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000001")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000002")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000003")))
          .inOrder();
    }

    @Test
    public void sortUuidAscNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  FieldPath.newRoot("uuidField"), SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, Integer.MAX_VALUE, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000000")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000001")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000002")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000003")),
              BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortUuidAscSearchAfter() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(FieldPath.newRoot("uuidField"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY,
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000000")))
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
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000001")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000002")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000003")))
          .inOrder();
    }

    @Test
    public void sortUuidAscSearchAfterNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  FieldPath.newRoot("uuidField"), SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty(), false);
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000000")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000001")))
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
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000002")),
              new BsonBinary(UUID.fromString("00000000-0000-0000-0000-000000000003")),
              BsonUtils.MAX_KEY)
          .inOrder();
    }
  }

  @RunWith(Theories.class)
  public static class PruningTest {
    private Directory directory;
    private IndexWriter writer;

    @Before
    public void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();
      IndexWriterConfig config = new IndexWriterConfig();
      config.setMergePolicy(NoMergePolicy.INSTANCE);
      this.writer = new IndexWriter(this.directory, config);
    }

    @After
    public void cleanup() throws IOException {
      this.writer.close();
      this.directory.close();
    }

    @DataPoints
    public static final NullEmptySortPosition[] nullEmptySortPositions =
        NullEmptySortPosition.values();

    @Test
    public void testUuidSortAscOptimizationExplain() throws IOException {
      try (var unusedExplain =
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
                                mongotSortField.field(), FieldName.TypeField.UUID)));

        int numDocs = 11000;
        int numHits = 5;
        int unused = 0;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
                .collect(Collectors.toList());
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

        // simple ascending sort
        SortField sortField =
            MqlSortedSetSortField.uuidSort(mongotSortField, Optional.empty(), false);
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, numHits);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

        SortPruningTestUtils.assertOptimizedSort(
            topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
        SortStats sortExplainResult =
            Explain.collect().get().collectStats().get().sortStats().get();
        SortPruningTestUtils.assertExplain(topDocs, numDocs, sortExplainResult);
      }
    }

    @Theory
    public void testUuidSortAscOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.ASC, nullEmptySortPosition)),
              Optional.empty(), false);
      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
    }

    @Test
    public void testUuidSortAscOptimizationNullsHighestAndMissingValues() throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      // Index docs with some of them containing missing values
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> x % 100 != 0))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty(), false);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
    }

    @Theory
    public void testUuidSortDescOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple descending sort
      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.DESC, nullEmptySortPosition)),
              Optional.empty(), false);
      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      SortPruningTestUtils.assertReversedOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
    }

    @Theory
    public void testUuidSortAfterAscOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      int afterNum = getMinDocIndexFromSampleSet(docs, numHits + 1);
      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  FIELD_NAME, new UserFieldSortOptions(SortOrder.ASC, nullEmptySortPosition)),
              Optional.empty(), false);
      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);

      // Get UUID of the after doc
      BsonValue afterValue =
          new BsonBinary(UUID.fromString(getUuidFieldForDoc(docs.get(afterNum))));
      FieldDoc after = new FieldDoc(afterNum, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
    }

    private int getMinDocIndexFromSampleSet(List<Document> docs, int sampleSize) {
      @Var int min = 0;
      for (int i = 1; i < sampleSize; i++) {
        if (getUuidFieldForDoc(docs.get(i)).compareTo(getUuidFieldForDoc(docs.get(min))) < 0) {
          min = i;
        }
      }
      return min;
    }

    @Test
    public void testSecondarySortNoOptimization() throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(SortField.FIELD_SCORE, sortField);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
    }

    @Test
    public void testUuidThenScoreSecondarySortOptimization() throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField, SortField.FIELD_SCORE);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      // test that if there is the secondary sort on _score, hits are still skipped
      SortPruningTestUtils.assertOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
      SortPruningTestUtils.assertScore(topDocs, 1, 1.0f);
    }

    @Test
    public void testUuidSortOptimizationEqualValues() throws IOException {
      int numDocs = 11000;
      String uuidStr = UUID.randomUUID().toString();
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(ignored -> new Document())
              .peek(
                  document -> {
                    document.add(
                        new SortedSetDocValuesField(
                            FieldName.TypeField.UUID.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            new BytesRef(uuidStr)));

                    document.add(
                        new StringField(
                            FieldName.TypeField.UUID.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            uuidStr,
                            Field.Store.NO));
                  })
              .toList();
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      SortField sortField =
          MqlSortedSetSortField.uuidSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty(), false);
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);

      // test that sorting on a single field with equal values uses the optimization
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(
          topDocs, numHits, numDocs, SortPruningTestUtils.UUID_STRING_COMPARABLE);
    }

    private static String getUuidFieldForDoc(Document doc) {
      String luceneFieldName =
          FieldName.TypeField.UUID.getLuceneFieldName(FIELD_NAME, Optional.empty());
      return doc.getValues(luceneFieldName)[0];
    }
  }
}
