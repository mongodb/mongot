package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects whether the connected mongod is part of a sharded cluster. Consumed by {@link
 * CatalogAccessGuard} to block catalog reads/writes when the mongot's {@code syncSource.router}
 * config does not match the real topology.
 *
 * <p>The cached value may lag real topology by up to one refresh interval. This is safe for the
 * catalog-access guard because the harmful case (RS-to-sharded conversion) requires moving the
 * catalog database to another shard, and {@code moveCollection} takes at least 5 minutes. With a
 * 1-minute refresh interval, a stale {@code isSharded=false} read cannot cause problems.
 */
public class MongodTopologyMonitor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MongodTopologyMonitor.class);

  public static final String MONGO_CLIENT_NAME = "MongodTopologyMonitor";
  public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(1);
  public static final Duration DEFAULT_EXPIRY = Duration.ofMinutes(3);
  public static final Duration DEFAULT_PROBE_WAIT = Duration.ofSeconds(2);

  private final MongoClient topologyClient;
  private final NamedScheduledExecutorService scheduler;
  private final Duration refreshInterval;
  private final Duration expiry;
  private final Duration probeWait;
  private final Clock clock;

  private final AtomicReference<CacheEntry> cache = new AtomicReference<>();
  private Optional<Future<?>> inFlightProbe = Optional.empty();
  private volatile boolean closed = false;

  @VisibleForTesting
  record CacheEntry(boolean sharded, Instant refreshedAt) {}

  public MongodTopologyMonitor(SyncSourceConfig syncSource, MeterRegistry meterRegistry) {
    this(
        buildClient(syncSource, meterRegistry),
        Executors.singleThreadScheduledExecutor("mongod-topology-monitor", meterRegistry),
        DEFAULT_REFRESH_INTERVAL,
        DEFAULT_EXPIRY,
        DEFAULT_PROBE_WAIT,
        Clock.systemUTC());
  }

  @VisibleForTesting
  MongodTopologyMonitor(
      MongoClient topologyClient,
      NamedScheduledExecutorService scheduler,
      Duration refreshInterval,
      Duration expiry,
      Duration probeWait,
      Clock clock) {
    this.topologyClient = topologyClient;
    this.scheduler = scheduler;
    this.refreshInterval = refreshInterval;
    this.expiry = expiry;
    this.probeWait = probeWait;
    this.clock = clock;
  }

  private static MongoClient buildClient(SyncSourceConfig syncSource, MeterRegistry meterRegistry) {
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        syncSource.mongodClusterReadWriteUri, MONGO_CLIENT_NAME, meterRegistry);
  }

  public void start() {
    LOG.atInfo()
        .addKeyValue("refreshInterval", this.refreshInterval)
        .addKeyValue("expiry", this.expiry)
        .log("Starting mongod topology monitor schedule.");
    this.scheduler.scheduleWithFixedDelay(
        this::refreshCache, 0L, this.refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Returns the cached topology if it is present and not expired.
   *
   * <p>If the cache has not been initialized yet (e.g. mongod started after mongot), this triggers
   * an on-demand probe and blocks up to {@link #probeWait} for it to complete, so a ready mongod
   * can be served on the first call instead of failing. If the probe does not populate the cache in
   * time, {@link CheckedMongoException} is thrown.
   *
   * <p>If the cache is present but stale, this throws immediately and relies on the periodic
   * refresh to repopulate it.
   */
  public boolean isShardedCluster() throws CheckedMongoException {
    CacheEntry entry = this.cache.get();
    if (entry != null && isFresh(entry)) {
      return entry.sharded();
    }
    if (entry == null) {
      return awaitProbe();
    }
    throw cacheUnavailableException();
  }

  /**
   * Waits for an in-flight on-demand probe (starting one if none is running) and then serves the
   * cache. Returns the probed value if the cache was populated in time, otherwise throws.
   */
  private boolean awaitProbe() throws CheckedMongoException {
    triggerAsyncProbe()
        .ifPresent(
            probe -> {
              try {
                probe.get(this.probeWait.toMillis(), TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.atWarn().setCause(e).log("Interrupted while waiting for the topology probe.");
              } catch (Exception e) {
                LOG.atWarn().setCause(e).log("Topology probe did not populate the cache in time.");
              }
            });
    CacheEntry entry = this.cache.get();
    if (entry != null) {
      return entry.sharded();
    }
    throw cacheUnavailableException();
  }

  /**
   * Ensures a single on-demand probe is in flight and returns its future, or an empty optional when
   * no probe could be scheduled. Concurrent callers coalesce onto the running probe's future.
   */
  private synchronized Optional<Future<?>> triggerAsyncProbe() {
    if (this.closed) {
      return Optional.empty();
    }
    if (this.inFlightProbe.filter(probe -> !probe.isDone()).isPresent()) {
      return this.inFlightProbe;
    }
    try {
      this.inFlightProbe =
          Optional.of(
              this.scheduler.submit(
                  () -> {
                    // Refresh only if the cache is still empty; a periodic refresh may have already
                    // populated it between scheduling this task and it running.
                    if (this.cache.get() == null) {
                      refreshCache();
                    }
                  }));
      return this.inFlightProbe;
    } catch (RejectedExecutionException e) {
      LOG.atWarn().setCause(e).log("Failed to schedule async topology probe.");
      return Optional.empty();
    }
  }

  private static CheckedMongoException cacheUnavailableException() {
    return new CheckedMongoException(new MongoException("Topology cache is unavailable."));
  }

  @VisibleForTesting
  void refreshCache() {
    if (this.closed) {
      return;
    }
    try {
      boolean sharded = probeShardedCluster();
      updateCache(sharded);
    } catch (Throwable e) {
      LOG.atError().setCause(e).log("Periodic topology refresh failed.");
    }
  }

  /**
   * Issues {@code getCmdLineOpts} on the dedicated mongod client and parses {@code
   * parsed.sharding}. Any node started with a {@code clusterRole} (i.e. {@code --shardsvr} or
   * {@code --configsvr}) has {@code parsed.sharding} populated; a plain replica-set member does
   * not.
   */
  private boolean probeShardedCluster() throws CheckedMongoException {
    try {
      BsonDocument result =
          this.topologyClient
              .getDatabase("admin")
              .runCommand(new BsonDocument("getCmdLineOpts", new BsonInt32(1)), BsonDocument.class);
      return result.getDocument("parsed", new BsonDocument()).containsKey("sharding");
    } catch (MongoException e) {
      throw new CheckedMongoException(e);
    }
  }

  private void updateCache(boolean sharded) {
    CacheEntry previous = this.cache.getAndSet(new CacheEntry(sharded, this.clock.instant()));
    if (previous == null) {
      LOG.info("Mongod topology cache is initialized to: sharded = {}", sharded);
    } else if (previous.sharded() != sharded) {
      LOG.warn(
          "Mongod topology change detected: was {}, now {}",
          previous.sharded() ? "sharded" : "replica set",
          sharded ? "sharded" : "replica set");
    }
  }

  private boolean isFresh(CacheEntry entry) {
    return Duration.between(entry.refreshedAt(), this.clock.instant()).compareTo(this.expiry) <= 0;
  }

  @VisibleForTesting
  Optional<CacheEntry> cachedEntry() {
    return Optional.ofNullable(this.cache.get());
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    Executors.shutdownOrFail(this.scheduler);
    this.topologyClient.close();
  }
}
