package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class ReplicationOptimeUpdaterTest {

  @Test
  public void recordOplogMetrics_majorityAheadOfOldest_setsOldestAndWindow() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (ReplicationOptimeUpdater updater = newUpdater(meterRegistry)) {
      updater.recordOplogMetrics(new BsonTimestamp(1_000, 1), new BsonTimestamp(400, 7));

      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.oldestTimestampSeconds"))
          .isEqualTo(400.0);
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.windowSeconds")).isEqualTo(600.0);
    }
  }

  @Test
  public void recordOplogMetrics_majorityNotAheadOfOldest_clampsWindowToZero() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (ReplicationOptimeUpdater updater = newUpdater(meterRegistry)) {
      updater.recordOplogMetrics(new BsonTimestamp(400, 1), new BsonTimestamp(500, 1));

      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.oldestTimestampSeconds"))
          .isEqualTo(500.0);
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.windowSeconds")).isEqualTo(0.0);
    }
  }

  @Test
  public void recordOplogMetrics_beforeFirstSample_gaugesAreNaN() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (ReplicationOptimeUpdater updater = newUpdater(meterRegistry)) {
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.oldestTimestampSeconds")).isNaN();
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.windowSeconds")).isNaN();
    }
  }

  @Test
  public void update_fetchesMajorityAndOldestFromClient_setsOplogGauges() {
    BsonTimestamp oldestTs = new BsonTimestamp(400, 7);
    BsonTimestamp majorityTs = new BsonTimestamp(1_000, 1);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (ReplicationOptimeUpdater updater =
        newUpdater(clientReturningOplogAndMajority(oldestTs, majorityTs), meterRegistry)) {
      updater.update();

      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.oldestTimestampSeconds"))
          .isEqualTo(400.0);
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.windowSeconds")).isEqualTo(600.0);
    }
  }

  @Test
  public void update_oldestOplogReadFails_incrementsErrorCounterAndSkipsGauges() {
    BsonTimestamp majorityTs = new BsonTimestamp(1_000, 1);
    BsonTimestamp indexOptime = new BsonTimestamp(400, 1);
    MongoClient client = mock(MongoClient.class);
    MongoDatabase admin = mock(MongoDatabase.class);
    when(client.getDatabase("admin")).thenReturn(admin);
    when(admin.runCommand(any(BsonDocument.class), eq(BsonDocument.class)))
        .thenReturn(replSetStatus(majorityTs));
    when(client.getDatabase(Databases.LOCAL)).thenThrow(new RuntimeException("oplog unavailable"));

    var indexGeneration = IndexGeneration.mockIndexGeneration();
    InitializedIndex initializedIndex = SearchIndex.mockInitializedIndex(indexGeneration);
    when(initializedIndex.getStatus()).thenReturn(IndexStatus.stale("stale", indexOptime));
    var indexingMetricsUpdater = initializedIndex.getMetricsUpdater().getIndexingMetricsUpdater();
    doCallRealMethod().when(indexingMetricsUpdater).getReplicationOpTimeInfo();

    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndexes()).thenReturn(List.of(indexGeneration));
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.of(initializedIndex));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (ReplicationOptimeUpdater updater =
        newUpdater(client, indexCatalog, initializedIndexCatalog, meterRegistry)) {
      var errors = meterRegistry.counter(ReplicationOptimeUpdater.ERROR_COUNTER_METRIC_NAME);
      double errorsBefore = errors.count();
      updater.update();

      assertThat(errors.count()).isEqualTo(errorsBefore + 1.0);
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.oldestTimestampSeconds")).isNaN();
      assertThat(gaugeValue(meterRegistry, "mongodb.oplog.windowSeconds")).isNaN();

      var snapshot = indexingMetricsUpdater.getReplicationOpTimeInfo().snapshot().orElseThrow();
      assertThat(snapshot.replicationOpTime()).isEqualTo(indexOptime.getValue());
      assertThat(snapshot.maxPossibleReplicationOpTime()).isEqualTo(majorityTs.getValue());
    }
  }

  private static ReplicationOptimeUpdater newUpdater(SimpleMeterRegistry meterRegistry) {
    return newUpdater(mock(MongoClient.class), meterRegistry);
  }

  private static ReplicationOptimeUpdater newUpdater(
      MongoClient mongoClient, SimpleMeterRegistry meterRegistry) {
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndexes()).thenReturn(List.of());
    return newUpdater(
        mongoClient, indexCatalog, mock(InitializedIndexCatalog.class), meterRegistry);
  }

  private static ReplicationOptimeUpdater newUpdater(
      MongoClient mongoClient,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SimpleMeterRegistry meterRegistry) {
    var executor = Executors.singleThreadScheduledExecutor("oplogMetricsTest", meterRegistry);
    var updater =
        ReplicationOptimeUpdater.create(
            indexCatalog,
            initializedIndexCatalog,
            mongoClient,
            executor,
            Duration.ofMinutes(1),
            meterRegistry);
    // create() schedules update() at delay 0. Drain it so tests only observe explicit update()
    // calls.
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted waiting for scheduled update to drain", e);
    }
    return updater;
  }

  @SuppressWarnings("unchecked")
  private static MongoClient clientReturningOplogAndMajority(
      BsonTimestamp oldestTs, BsonTimestamp majorityTs) {
    MongoClient client = mock(MongoClient.class);

    MongoDatabase admin = mock(MongoDatabase.class);
    when(client.getDatabase("admin")).thenReturn(admin);
    when(admin.runCommand(any(BsonDocument.class), eq(BsonDocument.class)))
        .thenReturn(replSetStatus(majorityTs));

    MongoDatabase local = mock(MongoDatabase.class);
    MongoCollection<BsonDocument> oplog = mock(MongoCollection.class);
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(client.getDatabase(Databases.LOCAL)).thenReturn(local);
    when(local.getCollection(eq("oplog.rs"), eq(BsonDocument.class))).thenReturn(oplog);
    when(oplog.find()).thenReturn(findIterable);
    when(findIterable.projection(any())).thenReturn(findIterable);
    when(findIterable.sort(any())).thenReturn(findIterable);
    when(findIterable.limit(1)).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(new BsonDocument("ts", oldestTs));
    return client;
  }

  private static BsonDocument replSetStatus(BsonTimestamp opTime) {
    BsonDocument opTimeDocument = new BsonDocument("ts", opTime).append("t", new BsonInt64(1L));
    return new BsonDocument(
        "optimes",
        new BsonDocument()
            .append("lastCommittedOpTime", opTimeDocument)
            .append("readConcernMajorityOpTime", opTimeDocument)
            .append("appliedOpTime", opTimeDocument)
            .append("durableOpTime", opTimeDocument));
  }

  private static double gaugeValue(SimpleMeterRegistry meterRegistry, String name) {
    Gauge gauge = meterRegistry.find(name).gauge();
    assertThat(gauge).isNotNull();
    return gauge.value();
  }
}
