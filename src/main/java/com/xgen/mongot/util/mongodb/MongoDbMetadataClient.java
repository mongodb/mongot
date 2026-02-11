package com.xgen.mongot.util.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbMetadataClient implements MongoDbServerInfoProvider, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbMetadataClient.class);
  private static final String MONGODB_VERSION_METRIC_NAME = "version";

  private Optional<SyncSourceConfig> syncSource;
  private final MeterRegistry meterRegistry;
  private final MetricsFactory mongodbMetricsFactory;
  private Optional<MongoClient> mongosPreferredClient;
  private Optional<MongoClient> directMongodClient;
  private volatile MongoDbServerInfo mongoDbServerInfo;
  private volatile boolean closed;
  private volatile Optional<String> previousVersionString;
  private volatile Optional<AtomicLong> previousMongodbVersionGauge;

  public MongoDbMetadataClient(Optional<SyncSourceConfig> syncSource, MeterRegistry meterRegistry) {
    this.mongoDbServerInfo = MongoDbServerInfo.EMPTY;
    this.syncSource = syncSource;
    this.closed = false;
    this.meterRegistry = meterRegistry;
    this.mongodbMetricsFactory = new MetricsFactory("mongodb", meterRegistry);
    this.mongosPreferredClient = Optional.empty();
    this.directMongodClient = Optional.empty();
    this.previousVersionString = Optional.empty();
    this.previousMongodbVersionGauge = Optional.empty();

    syncSource.ifPresent(this::createMongoClients);
  }

  private void createMongoClients(SyncSourceConfig syncSource) {
    this.mongosPreferredClient =
        Optional.of(
            MongoClientBuilder.buildNonReplicationPreferringMongos(
                syncSource, "database metadata resolver", this.meterRegistry));
    this.directMongodClient =
        Optional.of(
            MongoClientBuilder.buildNonReplicationWithDefaults(
                syncSource.mongodUri,
                "server info resolver",
                syncSource.sslContext,
                this.meterRegistry));
  }

  private static MongoDbCollectionInfos resolveCollectionInfos(
      Set<String> databases, MongoClient client) throws CheckedMongoException {

    List<MongoDbCollectionInfos> result = new ArrayList<>();

    for (var database : databases) {
      // prefer mongos URI as local shard might not have all collection infos, e.g. views
      result.add(MongoDbDatabase.getCollectionInfos(client, database));
    }

    Map<MongoNamespace, MongoDbCollectionInfo> collect =
        result.stream()
            .map(MongoDbCollectionInfos::getAll)
            .flatMap((Map<MongoNamespace, MongoDbCollectionInfo> map) -> map.entrySet().stream())
            .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));

    return new MongoDbCollectionInfos(ImmutableMap.copyOf(collect));
  }

  public MongoDbCollectionInfos resolveCollectionInfos(Set<String> databases)
      throws CheckedMongoException {
    if (this.closed || this.mongosPreferredClient.isEmpty()) {
      throw new CheckedMongoException(
          new MongoException("mongod unavailable, cannot resolve collection infos"));
    }

    return resolveCollectionInfos(databases, this.mongosPreferredClient.get());
  }

  public MongoDbCollectionInfos resolveCollectionInfosOnDirectMongod(Set<String> databases)
      throws CheckedMongoException {
    if (this.closed || this.directMongodClient.isEmpty()) {
      throw new CheckedMongoException(
          new MongoException("mongod unavailable, cannot resolve collection infos"));
    }

    return resolveCollectionInfos(databases, this.directMongodClient.get());
  }

  public synchronized void refreshServerInfo() {
    if (this.closed || this.directMongodClient.isEmpty()) {
      return;
    }
    // use direct mongod connection as mongos will not be able to provide replica set info
    var newServerInfo = MongoDbDatabase.getServerInfo(this.directMongodClient.get());
    if (newServerInfo.mongoDbVersion().isEmpty()
        && this.mongoDbServerInfo.mongoDbVersion().isPresent()) {
      LOG.warn(
          "Failed to retrieve mongoDbVersion from serverInfo, using "
              + "previously cached mongoDbVersion");
      this.mongoDbServerInfo =
          new MongoDbServerInfo(this.mongoDbServerInfo.mongoDbVersion(), newServerInfo.rsId());
    } else {
      this.mongoDbServerInfo = newServerInfo;
    }
    updateMongoDbVersionMetric();
  }

  private void updateMongoDbVersionMetric() {
    String versionString =
        this.mongoDbServerInfo.mongoDbVersion().map(MongoDbVersion::toString).orElse("unknown");

    // Early return if version hasn't changed
    if (this.previousVersionString.filter(versionString::equals).isPresent()) {
      return;
    }

    // If the version has changed, set the previous version gauge to 0
    this.previousMongodbVersionGauge.ifPresent(gauge -> gauge.set(0));

    // Create or update the gauge with the current version as a tag
    AtomicLong newGauge =
        this.mongodbMetricsFactory.numGauge(
            MONGODB_VERSION_METRIC_NAME, Tags.of("version", versionString));
    newGauge.set(1);
    this.previousVersionString = Optional.of(versionString);
    this.previousMongodbVersionGauge = Optional.of(newGauge);
  }

  public synchronized void maybeUpdateSyncSource(SyncSourceConfig newSyncSource) {

    if (this.closed
        || (this.syncSource.isPresent() && this.syncSource.get().equals(newSyncSource))) {
      return;
    }

    this.mongodbMetricsFactory
        .counter(
            "syncSourceChange",
            Tags.of("type", this.syncSource.isEmpty() ? "fromMmsDown" : "normal"))
        .increment();

    this.syncSource = Optional.of(newSyncSource);
    this.closeMongoClients();
    this.createMongoClients(newSyncSource);
  }

  @Override
  public MongoDbServerInfo getCachedMongoDbServerInfo() {
    return this.mongoDbServerInfo;
  }

  @Override
  public synchronized void close() {
    this.closed = true;
    this.mongoDbServerInfo = MongoDbServerInfo.EMPTY;
    this.closeMongoClients();
  }

  private void closeMongoClients() {
    if (this.mongosPreferredClient.isPresent()) {
      this.mongosPreferredClient.get().close();
    }
    if (this.directMongodClient.isPresent()) {
      this.directMongodClient.get().close();
    }
  }
}
