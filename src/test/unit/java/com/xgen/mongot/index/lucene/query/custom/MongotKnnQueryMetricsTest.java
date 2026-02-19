package com.xgen.mongot.index.lucene.query.custom;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MongotKnnQueryMetricsTest {

  private static final String FLOAT_VECTOR_FIELD = "floatVector";
  private static final String BYTE_VECTOR_FIELD = "byteVector";
  private static final String CATEGORY_FIELD = "category";
  private static final int K = 10;
  private static final int NUM_SEGMENTS = 5;
  private static final int DOCS_PER_SEGMENT = 20;

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  private ExecutorService executor;
  private PerIndexMetricsFactory metricsFactory;
  private IndexMetricsUpdater.QueryingMetricsUpdater metrics;

  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    this.directory = new MMapDirectory(temporaryFolder.getRoot().toPath());

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(NoMergePolicy.INSTANCE); // Disable merging to keep segments separate

    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      // Create multiple segments by committing after each batch
      for (int segment = 0; segment < NUM_SEGMENTS; segment++) {
        for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
          Document doc = new Document();
          int docId = segment * DOCS_PER_SEGMENT + i;
          float[] floatVector = new float[] {docId * 0.1f, docId * 0.2f, docId * 0.3f};
          byte[] byteVector = new byte[] {(byte) docId, (byte) (docId * 2), (byte) (docId * 3)};
          doc.add(
              new KnnFloatVectorField(
                  FLOAT_VECTOR_FIELD, floatVector, VectorSimilarityFunction.EUCLIDEAN));
          doc.add(
              new KnnByteVectorField(
                  BYTE_VECTOR_FIELD, byteVector, VectorSimilarityFunction.EUCLIDEAN));
          // only first 2 docs in each segment are "rare"
          // This gives 10 rare docs total (2 per segment)
          String category = i < 2 ? "rare" : "common";
          doc.add(new StringField(CATEGORY_FIELD, category, Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.commit(); // Create a new segment
      }
    }

    this.reader = DirectoryReader.open(this.directory);

    // Use multi-threaded searcher
    this.executor = Executors.newFixedThreadPool(4);
    this.searcher = new IndexSearcher(this.reader, this.executor);

    this.metricsFactory =
        new PerIndexMetricsFactory(
            IndexMetricsUpdater.NAMESPACE,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            GenerationIdBuilder.create());
    this.metrics = new IndexMetricsUpdater.QueryingMetricsUpdater(this.metricsFactory);
  }

  @After
  public void tearDown() throws IOException {
    this.reader.close();
    this.directory.close();
    this.executor.shutdown();
  }

  @Test
  public void testVisitedNodesMetricRecordedForUnfilteredQuery() throws IOException {
    float[] target = new float[] {0.5f, 1.0f, 1.5f};
    Query query = new MongotKnnFloatQuery(this.metrics, FLOAT_VECTOR_FIELD, target, K);

    // Rewrite triggers the KNN search
    query.rewrite(this.searcher);

    // Verify unfiltered approximate counter was incremented
    double unfilteredApproxCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"))
            .count();
    Assert.assertTrue(
        "Visited nodes metric should be recorded for unfiltered approximate query",
        unfilteredApproxCount > 0);

    // Verify filtered counters were not incremented
    double filteredApproxCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "approximate"))
            .count();
    double filteredExactCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "exact"))
            .count();
    Assert.assertEquals(
        "Filtered approximate counter should not be incremented for unfiltered query",
        0.0,
        filteredApproxCount,
        0.0);
    Assert.assertEquals(
        "Filtered exact counter should not be incremented for unfiltered query",
        0.0,
        filteredExactCount,
        0.0);
  }

  @Test
  public void testVisitedNodesMetricRecordedForFilteredQuery() throws IOException {
    float[] target = new float[] {0.5f, 1.0f, 1.5f};
    // Use a filter that matches all documents
    Query filter = new MatchAllDocsQuery();
    Query query = new MongotKnnFloatQuery(this.metrics, FLOAT_VECTOR_FIELD, target, K, filter);

    // Rewrite triggers the KNN search
    query.rewrite(this.searcher);

    // Verify filtered approximate counter was incremented
    double filteredApproxCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "approximate"))
            .count();
    Assert.assertTrue(
        "Visited nodes metric should be recorded for filtered query, got: " + filteredApproxCount,
        filteredApproxCount > 0);

    // Verify unfiltered counters were not incremented
    double unfilteredApproxCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"))
            .count();
    double unfilteredExactCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "exact"))
            .count();
    Assert.assertEquals(
        "Unfiltered approximate counter should not be incremented for filtered query",
        0.0,
        unfilteredApproxCount,
        0.0);
    Assert.assertEquals(
        "Unfiltered exact counter should not be incremented for filtered query",
        0.0,
        unfilteredExactCount,
        0.0);
  }

  @Test
  public void testVisitedNodesMetricAccumulatesAcrossQueries() throws IOException {
    float[] target1 = new float[] {0.5f, 1.0f, 1.5f};
    float[] target2 = new float[] {1.0f, 2.0f, 3.0f};

    // Execute first query
    Query query1 = new MongotKnnFloatQuery(this.metrics, FLOAT_VECTOR_FIELD, target1, K);
    query1.rewrite(this.searcher);

    double countAfterFirst =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"))
            .count();
    Assert.assertTrue("Should have visited nodes after first query", countAfterFirst > 0);

    // Execute second query
    Query query2 = new MongotKnnFloatQuery(this.metrics, FLOAT_VECTOR_FIELD, target2, K);
    query2.rewrite(this.searcher);

    double countAfterSecond =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"))
            .count();
    Assert.assertTrue(
        "Counter should accumulate across queries", countAfterSecond > countAfterFirst);
  }

  @Test
  public void testByteVectorQueryRecordsMetrics() throws IOException {
    byte[] target = new byte[] {10, 20, 30};
    Query query = new MongotKnnByteQuery(this.metrics, BYTE_VECTOR_FIELD, target, K);

    query.rewrite(this.searcher);

    double visitedCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"))
            .count();
    Assert.assertTrue("Byte vector query should record visited nodes metric", visitedCount > 0);
  }

  @Test
  public void testExactSearchMetricRecordedForRestrictiveFilter() throws IOException {
    float[] target = new float[] {0.5f, 1.0f, 1.5f};
    // Use a filter that matches only 2 docs per segment (10 total across 5 segments)
    // With k=10, this forces exact search since approximate search can't find enough candidates
    Query filter = new TermQuery(new Term(CATEGORY_FIELD, "rare"));
    Query query = new MongotKnnFloatQuery(this.metrics, FLOAT_VECTOR_FIELD, target, K, filter);

    // Rewrite triggers the KNN search
    query.rewrite(this.searcher);

    // Verify filtered exact counter was incremented (exact search should be triggered)
    double filteredExactCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "exact"))
            .count();
    Assert.assertTrue(
        "Exact search visited nodes metric should be recorded for  filter, got: "
            + filteredExactCount,
        filteredExactCount > 0);
  }

  @Test
  public void testByteVectorExactSearchMetricRecordedForRestrictiveFilter() throws IOException {
    byte[] target = new byte[] {10, 20, 30};
    // Use a filter that matches only 2 docs per segment (10 total across 5 segments)
    // With k=10, this forces exact search since approximate search can't find enough candidates
    Query filter = new TermQuery(new Term(CATEGORY_FIELD, "rare"));
    Query query = new MongotKnnByteQuery(this.metrics, BYTE_VECTOR_FIELD, target, K, filter);

    // Rewrite triggers the KNN search
    query.rewrite(this.searcher);

    // Verify filtered exact counter was incremented (exact search should be triggered)
    double filteredExactCount =
        this.metricsFactory
            .counter("vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "exact"))
            .count();
    Assert.assertTrue(
        "Byte vector exact search visited nodes metric should be recorded for filter, got: "
            + filteredExactCount,
        filteredExactCount > 0);
  }
}
