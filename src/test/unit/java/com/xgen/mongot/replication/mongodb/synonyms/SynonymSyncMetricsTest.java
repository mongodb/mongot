package com.xgen.mongot.replication.mongodb.synonyms;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import com.xgen.testing.ControlledBlockingQueue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SynonymSyncMetricsTest {
  private static final BsonTimestamp SCAN_OPERATION_TIME = new BsonTimestamp(12345L);

  private SimpleMeterRegistry meterRegistry;
  private MetricsFactory metricsFactory;
  private SynonymSyncMongoClient mockMongoClient;
  private SynonymDocumentIndexer mockDocumentIndexer;
  private SynonymManager synonymManager;

  @Test
  public void collectionScan_regexLimit_countsOnceAfterTranslation() throws Exception {
    var cause = new RegexLimitExceededException();
    var failure =
        assertThrows(
            SynonymSyncException.class,
            () ->
                SynonymSyncException.wrapIfThrows(
                    () -> {
                      throw cause;
                    }));
    this.synonymManager = createSynonymManagerWithFailingScanner(this.metricsFactory, failure);

    var future =
        this.synonymManager.enqueueCollectionScan(
            this.mockDocumentIndexer,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            () -> {});
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));

    assertThat(this.metricsFactory.get("regexAnalysisLimitExceeded").counter().count())
        .isEqualTo(1.0);
    assertThat(this.metricsFactory.get("exceptions").counter().count()).isEqualTo(1.0);
  }

  @Test
  public void collectionScan_regexInterruption_doesNotCountAsLimitFailure() throws Exception {
    var failure =
        assertThrows(
            SynonymSyncException.class,
            () ->
                SynonymSyncException.wrapIfThrows(
                    () -> {
                      throw new RegexInterruptedException();
                    }));
    this.synonymManager = createSynonymManagerWithFailingScanner(this.metricsFactory, failure);

    var future =
        this.synonymManager.enqueueCollectionScan(
            this.mockDocumentIndexer,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            () -> {});
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));

    assertThat(this.metricsFactory.get("regexAnalysisLimitExceeded").counter().count())
        .isEqualTo(0.0);
  }

  @Test
  public void recordException_directAndTimestampWrappedLimits_countsBoth() {
    var metrics = new SynonymSyncMetrics(this.metricsFactory);
    var failure =
        assertThrows(
            SynonymSyncException.class,
            () ->
                SynonymSyncException.wrapIfThrows(
                    () -> {
                      throw new RegexLimitExceededException();
                    }));

    metrics.recordException(new RegexLimitExceededException());
    metrics.recordException(
        SynonymSyncException.withOpTime(failure, Optional.of(SCAN_OPERATION_TIME)));

    assertThat(this.metricsFactory.get("regexAnalysisLimitExceeded").counter().count())
        .isEqualTo(2.0);
    assertThat(this.metricsFactory.get("exceptions").counter().count()).isEqualTo(2.0);
  }

  @Before
  public void setUp() throws SynonymSyncException {
    this.meterRegistry = new SimpleMeterRegistry();
    this.metricsFactory = new MetricsFactory("synonymSync", this.meterRegistry);
    this.mockMongoClient = mock(SynonymSyncMongoClient.class);
    this.mockDocumentIndexer = mock(SynonymDocumentIndexer.class);
    doNothing().when(this.mockMongoClient).mongosHealthCheck();
    doNothing().when(this.mockDocumentIndexer).complete();
  }

  @After
  public void tearDown() throws Exception {
    if (this.synonymManager != null) {
      this.synonymManager.shutdown().get(5, TimeUnit.SECONDS);
      this.synonymManager = null;
    }
    this.meterRegistry.close();
  }

  @Test
  public void testCollectionScanCounts() throws Exception {
    this.synonymManager = createSynonymManagerWithMockScanner(this.metricsFactory);

    CompletableFuture<SynonymMappingHighWaterMark> future1 =
        this.synonymManager.enqueueCollectionScan(
            this.mockDocumentIndexer,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            () -> {});

    assertEquals(1.0, this.metricsFactory.get("collScans").counter().count(), 0.01);

    future1.get(5, TimeUnit.SECONDS);

    for (int i = 0; i < 2; i++) {
      CompletableFuture<SynonymMappingHighWaterMark> future =
          this.synonymManager.enqueueCollectionScan(
              this.mockDocumentIndexer,
              mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
              MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
              () -> {});
      future.get(5, TimeUnit.SECONDS);
    }

    assertEquals(3.0, this.metricsFactory.get("collScans").counter().count(), 0.01);
    assertThat(this.metricsFactory.get("regexAnalysisLimitExceeded").counter().count())
        .isEqualTo(0.0);
  }

  @Test
  public void testCollScansTriggeredByChangeStreamCounter() throws Exception {
    // Create a change stream request that detects changes
    ResettableChangeStreamClient mockChangeStreamClient = mock(ResettableChangeStreamClient.class);
    ChangeStreamBatch batchWithChanges = mock(ChangeStreamBatch.class);
    when(batchWithChanges.getRawEvents())
        .thenReturn(new ArrayList<>(List.of(BsonUtils.documentToRaw(new BsonDocument()))));
    when(mockChangeStreamClient.getNext()).thenReturn(batchWithChanges);

    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    SynonymSyncMetrics metrics = new SynonymSyncMetrics(this.metricsFactory);
    SynonymChangeStreamRequest changeStreamRequest =
        SynonymChangeStreamRequest.create(
            mockChangeStreamClient,
            indexGeneration,
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            metrics);

    changeStreamRequest.doWork(this.mockMongoClient);

    assertEquals(
        1.0, this.metricsFactory.get("collScansTriggeredByChangeStream").counter().count(), 0.01);
  }

  @Test
  public void testCollectionScanDurations() throws Exception {
    this.synonymManager = createSynonymManagerWithMockScanner(this.metricsFactory);

    CompletableFuture<SynonymMappingHighWaterMark> future =
        this.synonymManager.enqueueCollectionScan(
            this.mockDocumentIndexer,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            () -> {});

    future.get(5, TimeUnit.SECONDS);

    // Wait for sync thread to finish and metrics to be updated
    Thread.sleep(1000);
    Timer syncDurationTimer = this.metricsFactory.get("syncDurations").timer();
    assertEquals("Sync duration timer should have 1 sample", 1, syncDurationTimer.count());
    assertThat(syncDurationTimer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0.0);

    Timer collScanDurationTimer = this.metricsFactory.get("collScanDurations").timer();
    assertEquals("Coll scan duration timer should have 1 sample", 1, collScanDurationTimer.count());
    assertThat(collScanDurationTimer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0.0);
  }

  @Test
  public void testExceptionCounter() throws Exception {
    this.synonymManager = createSynonymManagerWithFailingScanner(this.metricsFactory);

    // Enqueue a collection scan that will fail
    CompletableFuture<SynonymMappingHighWaterMark> future =
        this.synonymManager.enqueueCollectionScan(
            this.mockDocumentIndexer,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            () -> {});

    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (Exception ignored) {
      // Expected to fail - the test verifies exception counter is incremented
    }

    // Verify exception counter was incremented
    Counter exceptionCounter = this.metricsFactory.get("exceptions").counter();
    assertEquals(1.0, exceptionCounter.count(), 0.01);
    assertThat(this.metricsFactory.get("regexAnalysisLimitExceeded").counter().count())
        .isEqualTo(0.0);
  }

  private SynonymManager createSynonymManagerWithFailingScanner(MetricsFactory metricsFactory)
      throws SynonymSyncException {
    return createSynonymManagerWithFailingScanner(
        metricsFactory, SynonymSyncException.createTransient(new RuntimeException()));
  }

  private SynonymManager createSynonymManagerWithFailingScanner(
      MetricsFactory metricsFactory, SynonymSyncException failure) throws SynonymSyncException {
    SynonymCollectionScanner mockScanner = mock(SynonymCollectionScanner.class);
    when(mockScanner.scan()).thenThrow(failure);
    SynonymCollectionScanner.Factory mockFactory = (mongoClient, request) -> mockScanner;
    doNothing().when(this.mockDocumentIndexer).completeExceptionally(any());

    return SynonymManager.create(
        this.mockMongoClient,
        ControlledBlockingQueue.ready(),
        new HashMap<>(),
        new HashSet<>(),
        new HashMap<>(),
        2,
        mockFactory,
        metricsFactory);
  }

  private SynonymCollectionScanner createMockScanner() throws SynonymSyncException {
    SynonymCollectionScanner mockScanner = mock(SynonymCollectionScanner.class);
    when(mockScanner.scan()).thenReturn(SCAN_OPERATION_TIME);
    return mockScanner;
  }

  private SynonymManager createSynonymManagerWithMockScanner(MetricsFactory metricsFactory)
      throws SynonymSyncException {
    SynonymCollectionScanner mockScanner = createMockScanner();
    SynonymCollectionScanner.Factory mockFactory = (mongoClient, request) -> mockScanner;

    return SynonymManager.create(
        this.mockMongoClient,
        ControlledBlockingQueue.ready(),
        new HashMap<>(),
        new HashSet<>(),
        new HashMap<>(),
        2,
        mockFactory,
        metricsFactory);
  }
}
