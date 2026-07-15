package com.xgen.mongot.replication.mongodb.common;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.session.ClientSession;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData.MongodbClientMeterData;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.mongot.util.mongodb.RefreshSessionsCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSessionRefresher implements SessionRefresher, VerboseRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionRefresher.class);
  private static final Duration DEFAULT_REFRESH_PERIOD = Duration.ofMinutes(10);

  private final NamedScheduledExecutorService refreshExecutor;
  private final MongoClient mongoClient;

  @GuardedBy("this")
  private final Set<BsonDocument> sessionIds;

  private final AtomicLong sessionsGauge;
  private final Counter refreshCounter;
  private final Counter failedRefreshCounter;
  private final Timer refreshTimer;

  // Set when the replication manager is closing the shared client on shutdown. Once set, refresh
  // failures are attributed to the client being torn down and are ignored rather than counted.
  private volatile boolean stopping = false;

  private DefaultSessionRefresher(
      MetricsFactory metricsFactory,
      CommonReplicationConfig.Type replicationType,
      NamedScheduledExecutorService refreshExecutor,
      MongoClient mongoClient) {
    this.refreshExecutor = refreshExecutor;
    this.mongoClient = mongoClient;
    this.sessionIds = new HashSet<>();
    Tags replicationTags = Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag());

    this.sessionsGauge =
        metricsFactory.numGauge("sessions", Tags.of("replicationType", replicationType.name()));
    this.refreshCounter = metricsFactory.counter("refreshes");
    this.failedRefreshCounter =
        metricsFactory.counter(MongodbClientMeterData.FAILED_SESSION_REFRESHES, replicationTags);
    this.refreshTimer =
        metricsFactory.timer(MongodbClientMeterData.SESSION_REFRESH_DURATIONS, replicationTags);
  }

  public static DefaultSessionRefresher create(
      MetricsFactory metricsFactory,
      CommonReplicationConfig.Type replicationType,
      NamedScheduledExecutorService refreshExecutor,
      MongoClient mongoClient) {
    return create(
        metricsFactory, replicationType, refreshExecutor, mongoClient, DEFAULT_REFRESH_PERIOD);
  }

  static DefaultSessionRefresher create(
      MetricsFactory metricsFactory,
      CommonReplicationConfig.Type replicationType,
      NamedScheduledExecutorService refreshExecutor,
      MongoClient mongoClient,
      Duration refreshPeriod) {
    DefaultSessionRefresher refresher =
        new DefaultSessionRefresher(metricsFactory, replicationType, refreshExecutor, mongoClient);
    refreshExecutor.scheduleWithFixedDelay(
        refresher, refreshPeriod.toMillis(), refreshPeriod.toMillis(), TimeUnit.MILLISECONDS);
    return refresher;
  }

  @Override
  public synchronized <S extends ClientSession> RefreshingClientSession<S> register(S session) {
    BsonDocument sessionId = session.getServerSession().getIdentifier();
    Check.checkState(
        !this.sessionIds.contains(sessionId),
        "session %s already registered with SessionRefresher",
        session);

    this.sessionIds.add(sessionId);
    this.sessionsGauge.incrementAndGet();
    return new DefaultRefreshingClientSession<>(session, sessionId);
  }

  @Override
  public void markStopping() {
    this.stopping = true;
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down.");

    Executors.shutdownOrFail(this.refreshExecutor);
  }

  @Override
  public void verboseRun() {
    refresh();
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  private void refresh() {
    List<BsonDocument> sessionIdsCopy;
    synchronized (this) {
      sessionIdsCopy = new ArrayList<>(this.sessionIds);
    }

    if (sessionIdsCopy.isEmpty()) {
      LOG.debug("No sessions to refresh.");
      return;
    }

    LOG.atInfo().addKeyValue("numSessions", sessionIdsCopy.size()).log("Refreshing session(s).");
    this.refreshCounter.increment();

    RefreshSessionsCommand command = new RefreshSessionsCommand(sessionIdsCopy);
    try {
      Timed.runnable(
          this.refreshTimer,
          () -> this.mongoClient.getDatabase(Databases.ADMIN).runCommand(command.toProxy()));
    } catch (RuntimeException e) {
      if (this.stopping) {
        // The replication manager closed the shared client out from under this refresh on
        // shutdown. Depending on timing this surfaces as an IllegalStateException (pool already
        // closed) or a MongoException (socket closed mid-read); either way the cursors it keeps
        // alive are being torn down anyway, so it is not a real failure. Ignore it, but keep the
        // periodic task running so it isn't cancelled.
        LOG.debug("Ignoring session refresh failure while shutting down.", e);
        return;
      }
      if (e instanceof MongoException) {
        // A genuine refresh failure (command error, network blip). Count it.
        LOG.error("Caught exception attempting to refresh sessions.", e);
        this.failedRefreshCounter.increment();
        return;
      }
      // An unexpected runtime exception. Log it loudly, but keep the periodic task running rather
      // than letting it escape and cancel refreshes (which would eventually let the server reap
      // the sessions and kill the replication cursors). Don't count it as a refresh failure so the
      // failedRefreshCounter metric keeps meaning "a real refresh command failed".
      LOG.error("Unexpected exception while refreshing sessions; continuing.", e);
    }
  }

  /**
   * DefaultRefreshingClientSession removes the ClientSession from the list of sessions to be
   * refreshed when close() is called.
   */
  private class DefaultRefreshingClientSession<S extends ClientSession>
      implements RefreshingClientSession<S> {

    private final S session;
    private final BsonDocument identifier;

    DefaultRefreshingClientSession(S session, BsonDocument identifier) {
      this.session = session;
      this.identifier = identifier;
    }

    @Override
    public S getSession() {
      return this.session;
    }

    @Override
    public void close() {
      synchronized (DefaultSessionRefresher.this) {
        DefaultSessionRefresher.this.sessionIds.remove(this.identifier);
      }
      synchronized (this) {
        // it's necessary to keep it synchronized as ClientSession is not thread-safe
        // and concurrent close caused an issue: https://jira.mongodb.org/browse/JAVA-5107
        this.session.close();
      }
      DefaultSessionRefresher.this.sessionsGauge.decrementAndGet();
    }
  }
}
