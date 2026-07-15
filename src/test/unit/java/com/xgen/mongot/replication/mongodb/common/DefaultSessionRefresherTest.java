package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.Executors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

public class DefaultSessionRefresherTest {

  @Test
  public void testDoesNotRefreshIfNoSessions() throws Exception {
    var client = mock(MongoClient.class);
    var database = mock(MongoDatabase.class);

    var latch = new CountDownLatch(1);
    when(database.runCommand(any()))
        .then(
            invocation -> {
              latch.countDown();
              return new Document();
            });

    when(client.getDatabase(any())).thenReturn(database);

    var refresher =
        DefaultSessionRefresher.create(
            new MetricsFactory("replication.sessionRefresher", new SimpleMeterRegistry()),
            CommonReplicationConfig.Type.DEFAULT,
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      // Should not call refreshSessions if no sessions are registered.
      Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
    } finally {
      refresher.shutdown();
    }
  }

  @Test
  public void testIgnoresClosedClientExceptionDuringShutdownAndKeepsRefreshing() throws Exception {
    // Simulate the replication manager closing the shared client out from under the refresher on
    // shutdown: after markStopping(), runCommand throws IllegalStateException ("state should be:
    // open"). This must not be counted as a failed refresh, and the periodic task must keep running
    // (an escaping exception would cancel it).
    var registry = new SimpleMeterRegistry();
    var invocationCount = new AtomicInteger();
    var client =
        clientThrowing(() -> new IllegalStateException("state should be: open"), invocationCount);

    var refresher =
        DefaultSessionRefresher.create(
            new MetricsFactory("replication.sessionRefresher", registry),
            CommonReplicationConfig.Type.DEFAULT,
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    // Mark stopping before any refresh runs so every failure is attributed to shutdown.
    refresher.markStopping();

    try {
      try (var ignored = refresher.register(dummySession())) {
        waitForInvocations(invocationCount, 2);
        Assert.assertTrue(
            "expected refresh task to keep running after a closed-client exception",
            invocationCount.get() >= 2);
      }
      Assert.assertEquals(
          "shutdown-race failures must not be counted", 0.0, failedRefreshCount(registry), 0.0);
    } finally {
      refresher.shutdown();
    }
  }

  @Test
  public void testCountsMongoExceptionFailureAndKeepsRefreshing() throws Exception {
    // A genuine refresh failure during normal operation (not stopping) is counted, and the periodic
    // task keeps running.
    var registry = new SimpleMeterRegistry();
    var invocationCount = new AtomicInteger();
    var client = clientThrowing(() -> new MongoException("refresh failed"), invocationCount);

    var refresher =
        DefaultSessionRefresher.create(
            new MetricsFactory("replication.sessionRefresher", registry),
            CommonReplicationConfig.Type.DEFAULT,
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      try (var ignored = refresher.register(dummySession())) {
        waitForInvocations(invocationCount, 2);
        Assert.assertTrue(
            "expected refresh task to keep running after a failure", invocationCount.get() >= 2);
        Assert.assertTrue(
            "genuine MongoException failures must be counted", failedRefreshCount(registry) >= 1);
      }
    } finally {
      refresher.shutdown();
    }
  }

  @Test
  public void testDoesNotCountUnexpectedExceptionButKeepsRefreshing() throws Exception {
    // An unexpected (non-Mongo) runtime exception during normal operation must not cancel the
    // periodic task, and must not be counted as a refresh failure.
    var registry = new SimpleMeterRegistry();
    var invocationCount = new AtomicInteger();
    var client = clientThrowing(() -> new IllegalStateException("unexpected"), invocationCount);

    var refresher =
        DefaultSessionRefresher.create(
            new MetricsFactory("replication.sessionRefresher", registry),
            CommonReplicationConfig.Type.DEFAULT,
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      try (var ignored = refresher.register(dummySession())) {
        waitForInvocations(invocationCount, 2);
        Assert.assertTrue(
            "expected refresh task to keep running after an unexpected exception",
            invocationCount.get() >= 2);
      }
      Assert.assertEquals(
          "unexpected exceptions must not be counted as refresh failures",
          0.0,
          failedRefreshCount(registry),
          0.0);
    } finally {
      refresher.shutdown();
    }
  }

  @Test
  public void testRefreshesWhileSessionRegistered() throws Exception {
    var client = mock(MongoClient.class);
    var database = mock(MongoDatabase.class);

    var latch = new CountDownLatch(1);
    var closed = new AtomicBoolean(false);
    var invocationCount = new AtomicInteger();
    var closedInvocationCount = new AtomicInteger();
    when(database.runCommand(any()))
        .then(
            invocation -> {
              invocationCount.incrementAndGet();
              latch.countDown();
              if (closed.get()) {
                closedInvocationCount.incrementAndGet();
              }
              return new Document();
            });

    when(client.getDatabase(any())).thenReturn(database);

    var refresher =
        DefaultSessionRefresher.create(
            new MetricsFactory("replication.sessionRefresher", new SimpleMeterRegistry()),
            CommonReplicationConfig.Type.DEFAULT,
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      // Should refresh once we register a session.
      var session = mock(ClientSession.class);
      var serverSession = mock(ServerSession.class);
      var sessionId = new BsonDocument().append("id", new BsonObjectId());
      when(serverSession.getIdentifier()).thenReturn(sessionId);
      when(session.getServerSession()).thenReturn(serverSession);

      try (var refreshingSession = refresher.register(session)) {
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      }
      closed.set(true);

      // The RefreshingClientSession should have also closed the ClientSession.
      verify(session).close();

      // Wait on the main thread for a little bit. This will allow the thread pool to potentially
      // schedule a few more refresh attempts. These refresh attempts may or may not invoke the
      // runCommand resulting in an actual RefreshSessionsCommand.
      Thread.sleep(250);
      // Verify that we've issued to the RefreshCommand at least once.
      Assert.assertTrue(invocationCount.get() > 0);
      // When a session is closed, it's session id is removed from the list of sessions to refresh
      // that the refresher maintains. However, the refresher may have already initiated a refresh
      // which will include the session for refresh. However, the next refresh after session.close()
      // has returned will not include it. This means that the number of refresh invocations for a
      // given session after close() has been called is at most 1.
      Assert.assertTrue(closedInvocationCount.get() <= 1);
    } finally {
      refresher.shutdown();
    }
  }

  private static MongoClient clientThrowing(
      Supplier<? extends RuntimeException> exception, AtomicInteger invocationCount) {
    var client = mock(MongoClient.class);
    var database = mock(MongoDatabase.class);
    when(database.runCommand(any()))
        .then(
            invocation -> {
              invocationCount.incrementAndGet();
              throw exception.get();
            });
    when(client.getDatabase(any())).thenReturn(database);
    return client;
  }

  private static ClientSession dummySession() {
    var session = mock(ClientSession.class);
    var serverSession = mock(ServerSession.class);
    var sessionId = new BsonDocument().append("id", new BsonObjectId());
    when(serverSession.getIdentifier()).thenReturn(sessionId);
    when(session.getServerSession()).thenReturn(serverSession);
    return session;
  }

  private static void waitForInvocations(AtomicInteger invocationCount, int target)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (invocationCount.get() < target && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
  }

  private static double failedRefreshCount(SimpleMeterRegistry registry) {
    return registry.get("replication.sessionRefresher.failedSessionRefreshes").counter().count();
  }
}
