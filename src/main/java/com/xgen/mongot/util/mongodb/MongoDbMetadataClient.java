package com.xgen.mongot.util.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.mongot.util.mongodb.serialization.MongoDbInvalidReplStatusFormatException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbMetadataClient
    implements MongoDbServerInfoProvider, ReadConcernMajorityOpTimeProvider, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbMetadataClient.class);
  private static final String MONGODB_VERSION_METRIC_NAME = "version";

  private Optional<SyncSourceConfig> syncSource;
  private final MeterRegistry meterRegistry;
  private final MetricsFactory mongodbMetricsFactory;
  private volatile Optional<MongoClient> mongosPreferredClient;
  private volatile Optional<MongoClient> directMongodClient;
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

    syncSource
        .filter(SyncSourceConfig::hasReplicationUrisAvailable)
        .ifPresent(this::createMongoClients);
  }

  private void createMongoClients(SyncSourceConfig syncSource) {
    syncSource.validateReplicationUrisAvailable();
    // Build both clients before publishing either, so readers never observe a half-initialized
    // pair if client construction throws partway through.
    var newMongosPreferredClient =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, "database metadata resolver", this.meterRegistry);
    MongoClient newDirectMongodClient;
    try {
      newDirectMongodClient =
          MongoClientBuilder.buildNonReplicationWithDefaults(
              Optionals.orElseThrow(
                  syncSource.mongodSingleHostReplicationUri,
                  "mongodSingleHostReplicationUri must be present"),
              "server info resolver",
              this.meterRegistry);
    } catch (RuntimeException e) {
      // The mongos client is not reachable from any field yet, so close it here to avoid a leak.
      newMongosPreferredClient.close();
      throw e;
    }
    this.mongosPreferredClient = Optional.of(newMongosPreferredClient);
    this.directMongodClient = Optional.of(newDirectMongodClient);
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
    return resolveCollectionInfos(
        databases, requireMongosPreferredClient("resolve collection infos"));
  }

  public MongoDbCollectionInfos resolveCollectionInfosOnDirectMongod(Set<String> databases)
      throws CheckedMongoException {
    return resolveCollectionInfos(databases, requireDirectMongodClient("resolve collection infos"));
  }

  private MongoClient requireMongosPreferredClient(String action) throws CheckedMongoException {
    var client = this.mongosPreferredClient;
    if (this.closed || client.isEmpty()) {
      throw new CheckedMongoException(new MongoException("mongod unavailable, cannot " + action));
    }
    return client.get();
  }

  private MongoClient requireDirectMongodClient(String action) throws CheckedMongoException {
    var client = this.directMongodClient;
    if (this.closed || client.isEmpty()) {
      throw new CheckedMongoException(new MongoException("mongod unavailable, cannot " + action));
    }
    return client.get();
  }

  /**
   * Returns the majority-committed optime of the sync source, as reported by replSetGetStatus on
   * the direct mongod connection. mongos cannot serve replica set status, so the direct connection
   * is required here.
   */
  @Override
  public BsonTimestamp getReadConcernMajorityOpTime() throws CheckedMongoException {
    var client = requireDirectMongodClient("resolve readConcernMajorityOpTime");
    try {
      return MongoDbReplSetStatus.getReadConcernMajorityOpTime(client);
    } catch (MongoDbInvalidReplStatusFormatException e) {
      throw new CheckedMongoException(
          new MongoException(
              "could not parse replica set status while resolving readConcernMajorityOpTime", e));
    } catch (MongoException e) {
      throw new CheckedMongoException(e);
    }
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
    LOG.atInfo().addKeyValue("mongoDbVersion", versionString).log("Updated Mongod version info");

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
    // Close existing clients; they will be recreated once URIs are available.
    this.closeMongoClients();
    if (newSyncSource.hasReplicationUrisAvailable()) {
      this.createMongoClients(newSyncSource);
    }
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
    this.mongosPreferredClient.ifPresent(MongoClient::close);
    this.mongosPreferredClient = Optional.empty();
    this.directMongodClient.ifPresent(MongoClient::close);
    this.directMongodClient = Optional.empty();
  }
}
