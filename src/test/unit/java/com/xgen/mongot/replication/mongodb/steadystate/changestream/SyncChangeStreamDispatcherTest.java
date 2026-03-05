package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class SyncChangeStreamDispatcherTest {

  private static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();

  @Test
  public void testConcurrentEmbeddingGetMoresSemaphoreReleased_indexManagerShutdown()
      throws SteadyStateException {
    VectorIndexDefinition embeddingDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration embeddingGeneration =
        mockIndexGeneration(embeddingDefinition).getDefinitionGeneration();

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mock(ChangeStreamMongoClient.class));

    AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
    ChangeStreamIndexManager indexManager =
        spy(
            DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                embeddingDefinition,
                mock(IndexingWorkScheduler.class),
                mockDocumentIndexer(),
                ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                resumeInfoReference::set,
                IGNORE_METRICS,
                new CompletableFuture<>(),
                embeddingGeneration.getGenerationId(),
                DecodingWorkScheduler.create(2, new SimpleMeterRegistry())));
    // Simulate index manager shutdown.
    when(indexManager.isShutdown()).thenReturn(true);

    // Create a dispatcher with a maximum of 1 concurrent embedding getMore.
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            new SimpleMeterRegistry(),
            mongoClientFactory,
            Executors.fixedSizeThreadScheduledExecutor("executor", 1, new SimpleMeterRegistry()),
            Optional.of(1));

    syncDispatcher.add(
        embeddingDefinition,
        embeddingGeneration.getGenerationId(),
        mock(ChangeStreamResumeInfo.class),
        indexManager,
        false);

    // The semaphore should be released when we return early due to index manager shutdown.
    assertNumberOfEmbeddingPermits(syncDispatcher, 1);
  }

  private static void assertNumberOfEmbeddingPermits(
      SyncChangeStreamDispatcher dispatcher, int expectedPermits) {
    assertThat(dispatcher.getEmbeddingAvailablePermits()).isPresent();
    // The concurrentEmbeddingGetMores semaphore's permit is released in "finally" block of the call
    // to doGetMore(). It's possible that this executes after the indexingFuture completes, so we
    // wait up to 1 second for the permit to be released.
    try {
      Condition.await()
          .atMost(Duration.ofSeconds(1))
          .until(() -> dispatcher.getEmbeddingAvailablePermits().get() == expectedPermits);
    } catch (Exception e) {
      Assert.fail(
          "Expected "
              + expectedPermits
              + " embedding permits, but got "
              + dispatcher.getEmbeddingAvailablePermits().get());
    }
  }
}
