package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.query.sort.SortPruningTestUtils.FIELD_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.util.FieldTypeBuilder;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
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
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
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
      MqlObjectIdSortTest.TestClass.class,
      MqlObjectIdSortTest.TestPruning.class,
    })
public class MqlObjectIdSortTest {
  public static class TestClass {
    private Directory directory;
    private IndexSearcher searcher;

    private static Document createDoc(ObjectId objectIdValue) {
      Document doc = new Document();
      doc.add(
          new SortedSetDocValuesField(
              FieldName.TypeField.OBJECT_ID.getLuceneFieldName(
                  FieldPath.newRoot("objectIdField"), Optional.empty()),
              new BytesRef(objectIdValue.toByteArray())));
      return doc;
    }

    /** Sets up test. */
    @Before
    public void setup() throws IOException {
      this.directory = new ByteBuffersDirectory();
      IndexWriterConfig config = new IndexWriterConfig();

      ImmutableList<Document> docs =
          ImmutableList.of(
              createDoc(new ObjectId("000000000000000000000000")),
              createDoc(new ObjectId("000000000000000000000001")),
              createDoc(new ObjectId("000000000000000000000002")),
              createDoc(new ObjectId("000000000000000000000003")),
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
    public void sortObjectIdAsc() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  FieldPath.newRoot("objectIdField"), UserFieldSortOptions.DEFAULT_ASC),
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
              new BsonObjectId(new ObjectId("000000000000000000000000")),
              new BsonObjectId(new ObjectId("000000000000000000000001")),
              new BsonObjectId(new ObjectId("000000000000000000000002")),
              new BsonObjectId(new ObjectId("000000000000000000000003")))
          .inOrder();
    }

    @Test
    public void sortObjectIdAscNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  FieldPath.newRoot("objectIdField"),
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
              new BsonObjectId(new ObjectId("000000000000000000000000")),
              new BsonObjectId(new ObjectId("000000000000000000000001")),
              new BsonObjectId(new ObjectId("000000000000000000000002")),
              new BsonObjectId(new ObjectId("000000000000000000000003")),
              BsonUtils.MAX_KEY)
          .inOrder();
    }

    @Test
    public void sortObjectIdAscSearchAfter() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  FieldPath.newRoot("objectIdField"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              BsonUtils.MIN_KEY, new BsonObjectId(new ObjectId("000000000000000000000000")))
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
              new BsonObjectId(new ObjectId("000000000000000000000001")),
              new BsonObjectId(new ObjectId("000000000000000000000002")),
              new BsonObjectId(new ObjectId("000000000000000000000003")))
          .inOrder();
    }

    @Test
    public void sortObjectIdAscSearchAfterNullsHighest() throws IOException {
      SortField[] sortFields = new SortField[1];
      sortFields[0] =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  FieldPath.newRoot("objectIdField"),
                  SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty());
      Query query = new MatchAllDocsQuery();
      Sort sort = new Sort(sortFields);

      TopDocs topDocs = this.searcher.search(query, 2, sort);

      Object[] values =
          Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

      assertThat(values)
          .asList()
          .containsExactly(
              new BsonObjectId(new ObjectId("000000000000000000000000")),
              new BsonObjectId(new ObjectId("000000000000000000000001")))
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
              new BsonObjectId(new ObjectId("000000000000000000000002")),
              new BsonObjectId(new ObjectId("000000000000000000000003")),
              BsonUtils.MAX_KEY)
          .inOrder();
    }
  }

  /**
   * Tests adapted from <a
   * href="https://github.com/apache/lucene/blob/72968d30ba1761a9bd7d584471397e05f0fb8fd6/lucene/core/src/test/org/apache/lucene/search/testsortoptimization.java">lucene
   * pruning tests</a>.
   */
  @RunWith(Theories.class)
  public static class TestPruning {
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
    public void testObjectIdSortAscOptimizationExplain() throws IOException {
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
                                mongotSortField.field(), FieldName.TypeField.OBJECT_ID)));

        int numDocs = 11000;
        int numHits = 5;
        int unused = 0;
        List<Document> docs =
            IntStream.range(0, numDocs)
                .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
                .collect(Collectors.toList());
        // ObjectIds were generated in an increasing order above, so randomize the order of the
        // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
        // inserted likely won't correspond to the order of ObjectId values in the real world.
        Collections.shuffle(docs);
        IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

        // simple ascending sort
        SortField sortField = MqlSortedSetSortField.objectIdSort(mongotSortField, Optional.empty());
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
        Sort sort = new Sort(new ExplainSortField(sortField, explainer));
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            new TopFieldCollectorManager(sort, numHits, null, numHits);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

        SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
        SortStats sortExplainResult =
            Explain.collect().get().collectStats().get().sortStats().get();
        SortPruningTestUtils.assertExplain(topDocs, numDocs, sortExplainResult);
      }
    }

    @Theory
    public void testObjectIdSortAscOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.ASC, nullEmptySortPosition)),
              Optional.empty());
      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
    }

    @Test
    public void testObjectIdSortAscOptimizationNullsHighestAndMissingValues() throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      // Index docs with some of them containing missing values
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(value, true, x -> x % 100 != 0))
              .collect(Collectors.toList());
      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple ascending sort
      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, SortPruningTestUtils.DEFAULT_ASC_NULLS_HIGHEST),
              Optional.empty());
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
    }

    @Theory
    public void testObjectIdSortDescOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      // simple descending sort
      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME,
                  new UserFieldSortOptions(SortOrder.DESC, nullEmptySortPosition)),
              Optional.empty());
      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);

      SortPruningTestUtils.assertReversedOptimizedSort(topDocs, numHits, numDocs);
    }

    @Theory
    public void testObjectIdSortAfterAscOptimization(NullEmptySortPosition nullEmptySortPosition)
        throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      // Extract the "after" ObjectId before shuffling so that afterNum indexes into the
      // monotonically-increasing generation order, guaranteeing enough documents sort after it.
      int afterNum = 2;
      BsonValue afterValue =
          new BsonObjectId(
              new ObjectId(
                  docs.get(afterNum)
                      .getFields(
                          FieldName.TypeField.OBJECT_ID.getLuceneFieldName(
                              FIELD_NAME, Optional.empty()))[0]
                      .binaryValue()
                      .bytes));

      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  FIELD_NAME, new UserFieldSortOptions(SortOrder.ASC, nullEmptySortPosition)),
              Optional.empty());

      if (nullEmptySortPosition == NullEmptySortPosition.HIGHEST) {
        sortField.setMissingValue(SortField.STRING_LAST);
      } else {
        sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      }
      Sort sort = new Sort(sortField);
      FieldDoc after = new FieldDoc(2, Float.NaN, new BsonValue[] {afterValue});

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, after, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
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
      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(SortField.FIELD_SCORE, sortField);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertUnoptimizedSort(topDocs, numHits, numDocs);
    }

    @Test
    public void testObjectIdThenScoreSecondarySortOptimization() throws IOException {
      int numDocs = 11000;
      int numHits = 5;
      int unused = 0;
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(value -> SortPruningTestUtils.createDoc(unused, true, (x) -> true))
              .collect(Collectors.toList());
      // ObjectIds were generated in an increasing order above, so randomize the order of the
      // ObjectIds inserted to reflect a more realistic configuration. The order of the doc ids
      // inserted likely won't correspond to the order of ObjectId values in the real world.
      Collections.shuffle(docs);
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 2000);

      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField, SortField.FIELD_SCORE);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, numHits);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      // test that if there is the secondary sort on _score, hits are still skipped
      SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
      SortPruningTestUtils.assertScore(topDocs, 1, 1.0f);
    }

    @Test
    public void testObjectIdSortOptimizationEqualValues() throws IOException {
      int numDocs = 11000;
      var sameObjectId = new ObjectId();
      List<Document> docs =
          IntStream.range(0, numDocs)
              .mapToObj(ignored -> new Document())
              .peek(
                  document -> {
                    document.add(
                        new SortedSetDocValuesField(
                            FieldName.TypeField.OBJECT_ID.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            new BytesRef(sameObjectId.toByteArray())));

                    document.add(
                        new Field(
                            FieldName.TypeField.OBJECT_ID.getLuceneFieldName(
                                FIELD_NAME, Optional.empty()),
                            sameObjectId.toByteArray(),
                            new FieldTypeBuilder()
                                .withIndexOptions(IndexOptions.DOCS)
                                .tokenized(false)
                                .stored(false)
                                .build()));
                  })
              .toList();
      IndexSearcher searcher = SortPruningTestUtils.indexDocs(docs, this.writer, 7000);
      int numHits = 3;
      int totalHitsThreshold = 3;

      SortField sortField =
          MqlSortedSetSortField.objectIdSort(
              new MongotSortField(
                  SortPruningTestUtils.FIELD_NAME, UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      sortField.setMissingValue(SortPruningTestUtils.MISSING_VALUE_ORDER);
      Sort sort = new Sort(sortField);

      // test that sorting on a single field with equal values uses the optimization
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          new TopFieldCollectorManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      SortPruningTestUtils.assertOptimizedSort(topDocs, numHits, numDocs);
    }
  }
}
