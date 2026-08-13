package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

public class CustomVectorEngineIndexingWorkSchedulerTest {

  private static final IndexMetricsUpdater.IndexingMetricsUpdater IGNORE_METRICS =
      SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

  private static final ObjectId INDEX_ID = new ObjectId();
  private static final RawBsonDocument DOCUMENT =
      BsonUtils.documentToRaw(
          new BsonDocument(INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1))));
  private static final DocumentEvent INSERT =
      DocumentEvent.createInsert(
          DocumentMetadata.fromMetadataNamespace(Optional.of(DOCUMENT), INDEX_ID), DOCUMENT);
  private static final DocumentEvent UPDATE =
      DocumentEvent.createUpdate(
          DocumentMetadata.fromMetadataNamespace(Optional.of(DOCUMENT), INDEX_ID), DOCUMENT);
  private static final DocumentEvent DELETE = DocumentEvent.createDelete(new BsonInt32(1));

  @Test
  public void shouldCommitOnFinalize_always_returnsTrue() {
    assertThat(makeScheduler(2).shouldCommitOnFinalize()).isTrue();
  }

  @Test
  public void getBatchTasksFuture_multipleEvents_indexesSequentiallyInOrder() throws Exception {
    // Multi-thread executor so parallel allOf would allow overlap; the sequential chain must keep
    // at most one IndexingTask in flight.
    CustomVectorEngineIndexingWorkScheduler scheduler = makeScheduler(2);
    DocumentIndexer indexer = indexer();
    List<DocumentEvent> batch = List.of(INSERT, UPDATE, DELETE);

    var startedEvents = Collections.synchronizedList(new ArrayList<DocumentEvent>());
    var gates = new ConcurrentHashMap<DocumentEvent, CompletableFuture<Void>>();
    var inFlight = new AtomicInteger(0);

    // Each phase requires one invocation of indexDocumentEvent.
    var phaser = new Phaser(1);

    doAnswer(
            inv -> {
              DocumentEvent event = inv.getArgument(0);
              startedEvents.add(event);
              assertThat(inFlight.incrementAndGet()).isEqualTo(1);
              var gate =
                  gates
                      .computeIfAbsent(event, ignored -> new CompletableFuture<>())
                      .whenComplete((ignoreResult, ignoreException) -> inFlight.decrementAndGet());
              // Arrange local bookkeeping before arrive so the test thread cannot observe a race.
              phaser.arrive();
              gate.get(5, TimeUnit.SECONDS);
              return null;
            })
        .when(indexer)
        .indexDocumentEvent(any());

    CompletableFuture<Void> indexingFuture = schedule(scheduler, indexer, batch, Optional.empty());

    // Wait for the first event to start.
    phaser.awaitAdvanceInterruptibly(0, 5, TimeUnit.SECONDS);
    assertThat(phaser.isTerminated()).isFalse();
    assertThat(startedEvents).containsExactly(INSERT);
    assertThat(inFlight.get()).isEqualTo(1);

    // Complete the 1st event -> wait for 2nd start.
    gates.get(INSERT).complete(null);
    phaser.awaitAdvanceInterruptibly(1, 5, TimeUnit.SECONDS);
    assertThat(phaser.isTerminated()).isFalse();
    assertThat(startedEvents).containsExactly(INSERT, UPDATE).inOrder();
    assertThat(inFlight.get()).isAtMost(1);

    // Complete the 2nd event -> wait for 3rd start.
    gates.get(UPDATE).complete(null);
    phaser.awaitAdvanceInterruptibly(2, 5, TimeUnit.SECONDS);
    assertThat(phaser.isTerminated()).isFalse();
    assertThat(startedEvents).containsExactly(INSERT, UPDATE, DELETE).inOrder();
    assertThat(inFlight.get()).isAtMost(1);

    // Finish the 3rd event and confirm the batch future completes.
    gates.get(DELETE).complete(null);
    indexingFuture.get(5, TimeUnit.SECONDS);
    assertThat(inFlight.get()).isEqualTo(0);
  }

  private CompletableFuture<Void> schedule(
      CustomVectorEngineIndexingWorkScheduler scheduler,
      DocumentIndexer indexer,
      List<DocumentEvent> batch,
      Optional<IndexCommitUserData> commitUserData) {
    return scheduler.schedule(
        batch,
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        indexer,
        new GenerationId(new ObjectId(), Generation.CURRENT),
        Optional.of(new ObjectId()),
        commitUserData,
        IGNORE_METRICS);
  }

  private CustomVectorEngineIndexingWorkScheduler makeScheduler(int threads) {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor =
        Executors.fixedSizeThreadPool("indexing", threads, meterRegistry);
    return CustomVectorEngineIndexingWorkScheduler.create(executor);
  }

  private DocumentIndexer indexer() {
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentIndexer();
    doAnswer(invocation -> invocation.getArgument(0)).when(indexer).prepareBatch(any());
    return indexer;
  }
}
