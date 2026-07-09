package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.NUM_PARTITIONS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.MeteredVectorIndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MultiLuceneVectorIndexReaderTest {

  private VectorIndexingAndQueryingTestHarness testHarness;

  /** Set up resources for test. */
  @Before
  public void setUp() throws Exception {
    this.testHarness = new VectorIndexingAndQueryingTestHarness();
    this.testHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION);

    var indexReader = this.testHarness.getReader();
    Assert.assertTrue(
        ((MeteredVectorIndexReader) indexReader).unwrap() instanceof MultiLuceneVectorIndexReader);
    var multiReader =
        (MultiLuceneVectorIndexReader) ((MeteredVectorIndexReader) indexReader).unwrap();
    Assert.assertEquals(NUM_PARTITIONS, multiReader.numUnderlyingReaders());
  }

  @After
  public void tearDown() throws Exception {
    this.testHarness.close();
  }

  @Test
  public void testNoDocumentWithFloatVector() throws Exception {
    Vector queryVector = Vector.fromFloats(new float[] {1f, 0f, 0f}, NATIVE);
    this.testHarness.runTest(Collections.emptyList(), queryVector, Collections.emptyList());
  }

  @Test
  public void testSingleDocumentWithFloatVector() throws Exception {
    float[][] singleVector = new float[][] {{1f, 0f, 0f}};
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 99, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    List<Integer> expectedDocIds = List.of(99);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithFloatVectorInAscendingRadians() throws Exception {
    // Unit vectors with ascending radians. Since the query vector lies on the x-axis, the
    // Cosine similarity function will score the vectors in descending order.
    float[][] singleVector =
        new float[][] {
          {1.0000f, 0.0000f, 0f},
          {0.9969f, 0.0785f, 0f},
          {0.9877f, 0.1564f, 0f},
          {0.9724f, 0.2334f, 0f},
          {0.9511f, 0.3090f, 0f},
          {0.9239f, 0.3827f, 0f},
          {0.8910f, 0.4540f, 0f},
          {0.8526f, 0.5225f, 0f},
          {0.8090f, 0.5878f, 0f},
          {0.7604f, 0.6494f, 0f},
          {0.7071f, 0.7071f, 0f},
          {0.6494f, 0.7604f, 0f},
          {0.5878f, 0.8090f, 0f},
          {0.5225f, 0.8526f, 0f},
          {0.4540f, 0.8910f, 0f},
          {0.3827f, 0.9239f, 0f}
        };
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(15, 14, 13, 12, 11, 10, 9, 8, 7, 6);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithFloatVectorInDescendingRadians() throws Exception {
    // Unit vectors with descending radians. Since the query vector lies on the x-axis, the
    // Cosine similarity function will score the vectors in ascending order.
    float[][] singleVector =
        new float[][] {
          {0.0000f, 1.0000f, 0f},
          {0.0784f, 0.9969f, 0f},
          {0.1564f, 0.9876f, 0f},
          {0.2334f, 0.9723f, 0f},
          {0.3090f, 0.9510f, 0f},
          {0.3826f, 0.9238f, 0f},
          {0.4539f, 0.8910f, 0f},
          {0.5224f, 0.8526f, 0f},
          {0.5877f, 0.8090f, 0f},
          {0.6494f, 0.7604f, 0f},
          {0.7071f, 0.7071f, 0f},
          {0.7604f, 0.6494f, 0f},
          {0.8090f, 0.5877f, 0f},
          {0.8526f, 0.5224f, 0f},
          {0.8910f, 0.4539f, 0f},
          {0.9238f, 0.3826f, 0f}
        };
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithRandomFloatVector() throws Exception {
    // Random unit vectors in the 3d space.
    float[][] singleVector =
        new float[][] {
          {0.0065f, 0.5351f, 0.8447f},
          {0.7103f, 0.0593f, 0.7013f},
          {0.6611f, 0.5433f, 0.5173f},
          {0.7586f, 0.6193f, 0.2022f},
          {0.0759f, 0.9922f, 0.0983f},
          {0.9583f, 0.2769f, 0.0694f},
          {0.1365f, 0.1073f, 0.9848f},
          {0.9239f, 0.3749f, 0.0751f},
          {0.6948f, 0.3089f, 0.6494f},
          {0.3616f, 0.4854f, 0.7959f},
          {0.6242f, 0.5859f, 0.5166f},
          {0.6708f, 0.6858f, 0.2821f},
          {0.8722f, 0.1227f, 0.4734f},
          {0.7160f, 0.0256f, 0.6975f},
          {0.1150f, 0.3623f, 0.9249f},
          {0.6639f, 0.5555f, 0.5005f},
          {0.6888f, 0.0375f, 0.7239f},
          {0.5447f, 0.5500f, 0.6329f},
          {0.7632f, 0.4875f, 0.4239f},
          {0.4668f, 0.2598f, 0.8452f}
        };
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0.9826f, 0.1790f, 0.0483f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(5, 7, 12, 3, 18, 11, 15, 2, 8, 10);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testConcurrentQueryUsesExecutor() throws Exception {
    // Set up a harness with concurrent search enabled
    VectorIndexingAndQueryingTestHarness concurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    concurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, true);

    try {
      // Add some documents to ensure there are results
      float[][] vectors =
          new float[][] {
            {1.0f, 0.0f, 0.0f},
            {0.9f, 0.1f, 0.0f},
            {0.8f, 0.2f, 0.0f},
            {0.7f, 0.3f, 0.0f},
            {0.6f, 0.4f, 0.0f}
          };
      var docs =
          concurrentHarness.createVectorDocs(
              vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
      Vector queryVector = Vector.fromFloats(new float[] {1.0f, 0.0f, 0.0f}, NATIVE);

      // Index documents
      for (var doc : docs) {
        concurrentHarness.addDocument(doc);
      }

      // Run the query (which uses concurrent execution since the harness sets up with an executor)
      BsonArray results = concurrentHarness.runVectorSearchQuery(queryVector, false);

      // Verify the executor was used (execute is called by TaskExecutor for parallel execution)
      Assert.assertTrue(
          "Concurrent search executor should be present",
          concurrentHarness.getConcurrentSearchExecutor().isPresent());
      verify(concurrentHarness.getConcurrentSearchExecutor().get(), atLeast(1)).execute(any());

      // Verify results were returned
      Assert.assertEquals(5, results.size());

      // Verify each result has expected structure (document ID and vector search score)
      for (var result : results) {
        Assert.assertTrue("Result should contain _id", result.asDocument().containsKey("_id"));
        Assert.assertTrue(
            "Result should contain $vectorSearchScore",
            result.asDocument().containsKey("$vectorSearchScore"));
      }
    } finally {
      concurrentHarness.close();
    }
  }

  @Test
  public void testPerQueryConcurrentFalseOverridesExecutorPresence() throws Exception {
    // Index-level concurrent search is enabled (executor present), but the per-query
    // `concurrent: false` knob should force the serial path anyway.
    VectorIndexingAndQueryingTestHarness concurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    concurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, true);

    try {
      float[][] vectors =
          new float[][] {
            {1.0f, 0.0f, 0.0f},
            {0.9f, 0.1f, 0.0f},
            {0.8f, 0.2f, 0.0f},
            {0.7f, 0.3f, 0.0f},
            {0.6f, 0.4f, 0.0f}
          };
      var docs =
          concurrentHarness.createVectorDocs(
              vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
      Vector queryVector = Vector.fromFloats(new float[] {1.0f, 0.0f, 0.0f}, NATIVE);

      for (var doc : docs) {
        concurrentHarness.addDocument(doc);
      }

      BsonArray results =
          concurrentHarness.runVectorSearchQuery(queryVector, false, Optional.of(false));

      Assert.assertTrue(
          "Concurrent search executor should be present",
          concurrentHarness.getConcurrentSearchExecutor().isPresent());
      Mockito.verify(concurrentHarness.getConcurrentSearchExecutor().get(), Mockito.never())
          .execute(any());

      Assert.assertEquals(5, results.size());
    } finally {
      concurrentHarness.close();
    }
  }

  @Test
  public void testNonConcurrentQueryDoesNotUseExecutor() throws Exception {
    // Set up a new harness without concurrent search over partitions
    VectorIndexingAndQueryingTestHarness nonConcurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    nonConcurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, false);

    try {
      // Add some documents
      float[][] vectors =
          new float[][] {
            {1.0f, 0.0f, 0.0f},
            {0.9f, 0.1f, 0.0f},
            {0.8f, 0.2f, 0.0f},
            {0.7f, 0.3f, 0.0f},
            {0.6f, 0.4f, 0.0f}
          };
      var docs =
          nonConcurrentHarness.createVectorDocs(
              vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
      Vector queryVector = Vector.fromFloats(new float[] {1.0f, 0.0f, 0.0f}, NATIVE);

      // Index documents
      for (var doc : docs) {
        nonConcurrentHarness.addDocument(doc);
      }

      // Run the query
      BsonArray results = nonConcurrentHarness.runVectorSearchQuery(queryVector, false);

      // Verify the executor is not present (not set up for non-concurrent)
      Assert.assertFalse(
          "Concurrent search executor should not be present",
          nonConcurrentHarness.getConcurrentSearchExecutor().isPresent());

      // Verify all documents were returned
      Assert.assertEquals(5, results.size());

      // Verify each result has expected structure
      for (var result : results) {
        Assert.assertTrue("Result should contain _id", result.asDocument().containsKey("_id"));
        Assert.assertTrue(
            "Result should contain $vectorSearchScore",
            result.asDocument().containsKey("$vectorSearchScore"));
      }
    } finally {
      nonConcurrentHarness.close();
    }
  }

  @Test
  public void testNonConcurrentQueryReturnsCorrectResults() throws Exception {
    // Set up a new harness without concurrent search over partitions
    VectorIndexingAndQueryingTestHarness nonConcurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    nonConcurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, false);

    // Unit vectors with descending radians for deterministic ordering
    float[][] vectors =
        new float[][] {
          {0.0000f, 1.0000f, 0f},
          {0.0784f, 0.9969f, 0f},
          {0.1564f, 0.9876f, 0f},
          {0.2334f, 0.9723f, 0f},
          {0.3090f, 0.9510f, 0f},
          {0.3826f, 0.9238f, 0f},
          {0.4539f, 0.8910f, 0f},
          {0.5224f, 0.8526f, 0f},
          {0.5877f, 0.8090f, 0f},
          {0.6494f, 0.7604f, 0f}
        };
    var docs =
        nonConcurrentHarness.createVectorDocs(
            vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    List<Integer> expectedDocIds = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    nonConcurrentHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testConcurrentAndNonConcurrentQueryReturnSameResults() throws Exception {
    // Set up harnesses for concurrent and non-concurrent
    VectorIndexingAndQueryingTestHarness concurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    concurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, true);

    VectorIndexingAndQueryingTestHarness nonConcurrentHarness =
        new VectorIndexingAndQueryingTestHarness();
    nonConcurrentHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION, false);

    try {
      // Unit vectors with descending radians for deterministic ordering
      float[][] vectors =
          new float[][] {
            {0.0000f, 1.0000f, 0f},
            {0.0784f, 0.9969f, 0f},
            {0.1564f, 0.9876f, 0f},
            {0.2334f, 0.9723f, 0f},
            {0.3090f, 0.9510f, 0f},
            {0.3826f, 0.9238f, 0f},
            {0.4539f, 0.8910f, 0f},
            {0.5224f, 0.8526f, 0f},
            {0.5877f, 0.8090f, 0f},
            {0.6494f, 0.7604f, 0f}
          };
      var concurrentDocs =
          concurrentHarness.createVectorDocs(
              vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
      var nonConcurrentDocs =
          nonConcurrentHarness.createVectorDocs(
              vectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
      Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);

      // Index documents in both harnesses
      for (var doc : concurrentDocs) {
        concurrentHarness.addDocument(doc);
      }
      for (var doc : nonConcurrentDocs) {
        nonConcurrentHarness.addDocument(doc);
      }

      // Query with concurrent execution over partitions
      BsonArray concurrentResults = concurrentHarness.runVectorSearchQuery(queryVector, false);

      // Query with non-concurrent execution over partitions
      BsonArray nonConcurrentResults =
          nonConcurrentHarness.runVectorSearchQuery(queryVector, false);

      // Both should return the same number of documents
      Assert.assertEquals(concurrentResults.size(), nonConcurrentResults.size());
      Assert.assertEquals(10, concurrentResults.size());

      // Extract and compare document IDs from both result sets
      Set<Integer> concurrentIds = new HashSet<>();
      for (var result : concurrentResults) {
        concurrentIds.add(result.asDocument().getInt32("_id").getValue());
      }

      Set<Integer> nonConcurrentIds = new HashSet<>();
      for (var result : nonConcurrentResults) {
        nonConcurrentIds.add(result.asDocument().getInt32("_id").getValue());
      }

      // Both should contain the same document IDs
      Assert.assertEquals(
          "Concurrent and non-concurrent queries should return the same document IDs",
          concurrentIds,
          nonConcurrentIds);

      // Verify all expected IDs are present
      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            "Both results should contain document with _id " + i, concurrentIds.contains(i));
      }
    } finally {
      concurrentHarness.close();
      nonConcurrentHarness.close();
    }
  }

  // ============================================================================
  // Exception Handling Tests for Concurrent Partition Execution
  // ============================================================================

  // Leaves the concurrent knob unset, so VectorSearchQuery.concurrent() resolves to the engine
  // default (true) and this query is always concurrent. Used for all exception handling tests.
  private static final VectorSearchQuery TEST_QUERY_DEFINITION =
      VectorQueryBuilder.builder()
          .index(MOCK_INDEX_NAME)
          .criteria(
              ApproximateVectorQueryCriteriaBuilder.builder()
                  .limit(5)
                  .numCandidates(10)
                  .queryVector(Vector.fromFloats(new float[] {1f, 0f, 0f}, NATIVE))
                  .path(FieldPath.newRoot("vector.path"))
                  .build())
          .build();
  private static final MaterializedVectorSearchQuery TEST_MATERIALIZED_QUERY =
      new MaterializedVectorSearchQuery(
          TEST_QUERY_DEFINITION, TEST_QUERY_DEFINITION.criteria().queryVector().get());

  private static final QueryExecutionContext EMPTY_CONTEXT = QueryExecutionContext.empty();

  @Test
  public void testGetNumEmbeddedRootDocuments_sumsAcrossPartitions() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);
    Mockito.when(mockReader1.getNumEmbeddedRootDocuments()).thenReturn(3L);
    Mockito.when(mockReader2.getNumEmbeddedRootDocuments()).thenReturn(4L);

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2),
            mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            Optional.empty());

    // The multi reader sums the embedded-root-doc count across all partition readers.
    Assert.assertEquals(7L, multiReader.getNumEmbeddedRootDocuments());
  }

  /**
   * Tests that ReaderClosedException thrown by a partition reader is correctly propagated through
   * the concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesReaderClosedException() throws Exception {
    ReaderClosedException originalException = ReaderClosedException.create("testMethod");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    // First reader throws ReaderClosedException
    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    // Second reader would succeed
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2),
              mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
              Optional.of(executor));

      ReaderClosedException thrown =
          Assert.assertThrows(
              ReaderClosedException.class,
              () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

      Assert.assertSame(originalException, thrown);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that IOException thrown by a partition reader is correctly propagated through the
   * concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesIoException() throws Exception {
    IOException originalException = new IOException("Test IO exception from partition");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2),
              mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
              Optional.of(executor));

      IOException thrown =
          Assert.assertThrows(
              IOException.class, () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

      Assert.assertSame(originalException, thrown);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that InvalidQueryException thrown by a partition reader is correctly propagated through
   * the concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesInvalidQueryException() throws Exception {
    InvalidQueryException originalException = new InvalidQueryException("Test invalid query");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2),
              mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
              Optional.of(executor));

      InvalidQueryException thrown =
          Assert.assertThrows(
              InvalidQueryException.class,
              () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

      Assert.assertSame(originalException, thrown);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that when no partition reader throws an exception, the concurrent query completes
   * successfully.
   */
  @Test
  public void testConcurrentQuerySucceedsWithNoExceptions() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenReturn(Collections.emptyList());
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    // Create a properly configured metrics updater mock
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        mock(IndexMetricsUpdater.QueryingMetricsUpdater.class);
    io.micrometer.core.instrument.DistributionSummary mockSummary =
        mock(io.micrometer.core.instrument.DistributionSummary.class);
    Mockito.when(metricsUpdater.getBatchDocumentCount()).thenReturn(mockSummary);
    Mockito.when(metricsUpdater.getBatchDataSize()).thenReturn(mockSummary);

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2), metricsUpdater, Optional.of(executor));

      // Should not throw any exception
      multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT);

      // Verify both readers were queried
      verify(mockReader1).queryResults(any(), any());
      verify(mockReader2).queryResults(any(), any());
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests the behavior when a partition reader throws a RuntimeException during concurrent query.
   *
   * <p>The current implementation catches all exceptions in the Callable and rethrows non-checked
   * exceptions as RuntimeException from rethrowException().
   */
  @Test
  public void testConcurrentQueryWithRuntimeException() throws Exception {
    RuntimeException originalException = new RuntimeException("Test runtime exception");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    // Create a properly configured metrics updater mock
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        mock(IndexMetricsUpdater.QueryingMetricsUpdater.class);
    io.micrometer.core.instrument.DistributionSummary mockSummary =
        mock(io.micrometer.core.instrument.DistributionSummary.class);
    Mockito.when(metricsUpdater.getBatchDocumentCount()).thenReturn(mockSummary);
    Mockito.when(metricsUpdater.getBatchDataSize()).thenReturn(mockSummary);

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2), metricsUpdater, Optional.of(executor));

      RuntimeException thrown =
          Assert.assertThrows(
              RuntimeException.class,
              () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));
      Assert.assertSame(originalException, thrown.getCause());
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that when the deadline has already passed, the sequential query path throws {@link
   * DeadlineExceededException} before querying any partition reader, rather than silently returning
   * a partial result.
   */
  @Test
  public void testExpiredDeadlineThrowsInSequentialQuery() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        mock(IndexMetricsUpdater.QueryingMetricsUpdater.class);

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2), metricsUpdater, Optional.empty());

    QueryExecutionContext expiredContext =
        QueryExecutionContext.withDeadline(Optional.of(System.currentTimeMillis() - 1000));

    Assert.assertThrows(
        DeadlineExceededException.class,
        () -> multiReader.query(TEST_MATERIALIZED_QUERY, expiredContext));

    Mockito.verify(mockReader1, Mockito.never()).queryResults(any(), any());
    Mockito.verify(mockReader2, Mockito.never()).queryResults(any(), any());
  }

  /**
   * Tests that when the deadline has already passed, the concurrent query path throws {@link
   * DeadlineExceededException} before submitting any partition work.
   */
  @Test
  public void testExpiredDeadlineThrowsBeforeSubmittingConcurrentQuery() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2),
              mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
              Optional.of(executor));

      QueryExecutionContext expiredContext =
          QueryExecutionContext.withDeadline(Optional.of(System.currentTimeMillis() - 1000));

      Assert.assertThrows(
          DeadlineExceededException.class,
          () -> multiReader.query(TEST_MATERIALIZED_QUERY, expiredContext));

      Mockito.verify(mockReader1, Mockito.never()).queryResults(any(), any());
      Mockito.verify(mockReader2, Mockito.never()).queryResults(any(), any());
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that a {@link DeadlineExceededException} thrown by a partition reader is propagated
   * through the concurrent query path unchanged (not wrapped in a RuntimeException), so its
   * user-facing message survives.
   */
  @Test
  public void testConcurrentQueryPropagatesDeadlineExceededException() throws Exception {
    DeadlineExceededException originalException = new DeadlineExceededException();
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    NamedExecutorService executor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 2, new SimpleMeterRegistry()));
    try {
      MultiLuceneVectorIndexReader multiReader =
          new MultiLuceneVectorIndexReader(
              List.of(mockReader1, mockReader2),
              mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
              Optional.of(executor));

      DeadlineExceededException thrown =
          Assert.assertThrows(
              DeadlineExceededException.class,
              () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

      Assert.assertSame(originalException, thrown);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests that when the deadline is far in the future, the sequential query path queries all
   * partition readers normally.
   */
  @Test
  public void testFutureDeadlineQueriesAllPartitionsInSequentialQuery() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenReturn(Collections.emptyList());
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        mock(IndexMetricsUpdater.QueryingMetricsUpdater.class);
    io.micrometer.core.instrument.DistributionSummary mockSummary =
        mock(io.micrometer.core.instrument.DistributionSummary.class);
    Mockito.when(metricsUpdater.getBatchDocumentCount()).thenReturn(mockSummary);
    Mockito.when(metricsUpdater.getBatchDataSize()).thenReturn(mockSummary);

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2), metricsUpdater, Optional.empty());

    QueryExecutionContext futureContext =
        QueryExecutionContext.withDeadline(Optional.of(System.currentTimeMillis() + 60_000));

    multiReader.query(TEST_MATERIALIZED_QUERY, futureContext);

    Mockito.verify(mockReader1).queryResults(any(), any());
    Mockito.verify(mockReader2).queryResults(any(), any());
  }

  /** Tests that a query without a deadline queries all partitions (no early exit). */
  @Test
  public void testNoDeadlineQueriesAllPartitionsInSequentialQuery() throws Exception {
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenReturn(Collections.emptyList());
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        mock(IndexMetricsUpdater.QueryingMetricsUpdater.class);
    io.micrometer.core.instrument.DistributionSummary mockSummary =
        mock(io.micrometer.core.instrument.DistributionSummary.class);
    Mockito.when(metricsUpdater.getBatchDocumentCount()).thenReturn(mockSummary);
    Mockito.when(metricsUpdater.getBatchDataSize()).thenReturn(mockSummary);

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2), metricsUpdater, Optional.empty());

    multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT);

    Mockito.verify(mockReader1).queryResults(any(), any());
    Mockito.verify(mockReader2).queryResults(any(), any());
  }

  // ============================================================================
  // Exception Handling Tests for Sequential Partition Execution
  // ============================================================================

  /**
   * Tests that ReaderClosedException thrown by a partition reader is correctly propagated through
   * the sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesReaderClosedException() throws Exception {
    ReaderClosedException originalException = ReaderClosedException.create("testMethod");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    // Pass Optional.empty() for executor to get sequential processing
    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2),
            mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            Optional.empty());

    ReaderClosedException thrown =
        Assert.assertThrows(
            ReaderClosedException.class,
            () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that IOException thrown by a partition reader is correctly propagated through the
   * sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesIoException() throws Exception {
    IOException originalException = new IOException("Test IO exception from partition");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2),
            mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            Optional.empty());

    IOException thrown =
        Assert.assertThrows(
            IOException.class, () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that InvalidQueryException thrown by a partition reader is correctly propagated through
   * the sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesInvalidQueryException() throws Exception {
    InvalidQueryException originalException = new InvalidQueryException("Test invalid query");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2),
            mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            Optional.empty());

    InvalidQueryException thrown =
        Assert.assertThrows(
            InvalidQueryException.class,
            () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that RuntimeException thrown by a partition reader is correctly propagated through the
   * sequential query path. Unlike the concurrent path where RuntimeException is swallowed, the
   * sequential path propagates it directly since there's no catch block in the simple for loop.
   */
  @Test
  public void testSequentialQueryPropagatesRuntimeException() throws Exception {
    RuntimeException originalException = new RuntimeException("Test runtime exception");
    LuceneVectorIndexReader mockReader1 = mock(LuceneVectorIndexReader.class);
    LuceneVectorIndexReader mockReader2 = mock(LuceneVectorIndexReader.class);

    Mockito.when(mockReader1.queryResults(any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.queryResults(any(), any())).thenReturn(Collections.emptyList());

    MultiLuceneVectorIndexReader multiReader =
        new MultiLuceneVectorIndexReader(
            List.of(mockReader1, mockReader2),
            mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            Optional.empty());

    RuntimeException thrown =
        Assert.assertThrows(
            RuntimeException.class,
            () -> multiReader.query(TEST_MATERIALIZED_QUERY, EMPTY_CONTEXT));

    Assert.assertSame(originalException, thrown);
  }
}
