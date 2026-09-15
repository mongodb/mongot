package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamActivityObserverTest {

  private static final String DATABASE = "testdb";
  private static final Duration THRESHOLD = Duration.ofSeconds(300);
  private static final Duration RETRY_DELAY = Duration.ofMillis(10);
  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

  private static final String PER_INDEX_NAMESPACE =
      "indexing.steadyStateChangeStream.activityObserver";

  private SimpleMeterRegistry meterRegistry;
  private MeterAndFtdcRegistry meterAndFtdcRegistry;
  private MetricsFactory metricsFactory;
  private NamedScheduledExecutorService executor;
  private ScriptedStream stream;
  private ChangeStreamActivityObserver observer;

  @Before
  public void setUp() {
    this.meterRegistry = new SimpleMeterRegistry();
    this.meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithMeterRegistry(this.meterRegistry);
    this.metricsFactory = new MetricsFactory("test.activityObserver", this.meterRegistry);
    this.executor =
        Executors.singleThreadScheduledExecutor("test-activity-observer", this.meterRegistry);
    this.stream = new ScriptedStream();
  }

  @After
  public void tearDown() {
    if (this.observer != null) {
      this.observer.close();
    }
  }

  private void startObserver() {
    this.observer =
        new ChangeStreamActivityObserver(
            DATABASE,
            startAfter -> {
              this.stream.opens.incrementAndGet();
              this.stream.lastStartAfter = startAfter.orElse(null);
              return this.stream;
            },
            this.executor,
            THRESHOLD,
            this.metricsFactory,
            this.meterAndFtdcRegistry,
            RETRY_DELAY,
            Duration.ofMillis(200));
  }

  @Test
  public void demuxedEventMarksCollectionActive() throws Exception {
    UUID collection = UUID.randomUUID();
    GenerationId index = newGenerationId();
    startObserver();
    this.observer.register(index, namespace("coll"), collection);

    // First batch is empty and establishes the position; the collection seeds to it.
    this.stream.offerBatch(emptyBatch(1000));
    // Second batch carries an event for the collection at t=1500 with position t=1600.
    this.stream.offerBatch(batchWithEvent(event("coll", 1500), 1600));
    this.stream.awaitCalls(3);

    // Idle = position (1600) - last activity (1500) = 100s, below the threshold, so not inactive.
    assertThat(gauge("trackedIndexes")).isEqualTo(1);
    assertThat(gauge("trackedCollections")).isEqualTo(1);
    assertThat(gauge("inactiveIndexes")).isEqualTo(0);
  }

  @Test
  public void collectionWithNoEventsAgesFromItsSeed() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    this.observer.register(newGenerationId(), namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerBatch(emptyBatch(1400));
    this.stream.awaitCalls(3);

    // Seeded at 1000, position advanced to 1400 with no events: idle 400 >= threshold 300.
    assertThat(gauge("inactiveIndexes")).isEqualTo(1);
  }

  @Test
  public void registrationBeforeFirstBatchIsSeededByReadLoop() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    // Registered before any batch: no position yet, seed is deferred.
    this.observer.register(newGenerationId(), namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(2000));
    this.stream.awaitCalls(2);

    // Seeded to the first position, so idle is ~0 (< threshold) and the index is active, rather
    // than some unbounded value that would count inactive.
    assertThat(gauge("trackedIndexes")).isEqualTo(1);
    assertThat(gauge("inactiveIndexes")).isEqualTo(0);
  }

  @Test
  public void eventsForUnmanagedCollectionsAreIgnored() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    this.observer.register(newGenerationId(), namespace("managed"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerBatch(batchWithEvent(event("unmanaged", 1500), 1600));
    this.stream.awaitCalls(3);

    // The unmanaged event does not touch the managed collection: it stays seeded at 1000, so idle
    // is 600 (>= threshold 300) and it counts inactive. Had the event wrongly been attributed,
    // idle would be 100 and it would be active.
    assertThat(gauge("inactiveIndexes")).isEqualTo(1);
  }

  @Test
  public void eventsWithoutCollectionNamespaceAreIgnored() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    this.observer.register(newGenerationId(), namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    // A dropDatabase-style event carries a db-only ns; an invalidate carries none.
    RawBsonDocument dropDatabase =
        RawBsonDocument.parse(
            "{operationType: 'dropDatabase', ns: {db: 'testdb'},"
                + " clusterTime: {'$timestamp': {t: 1500, i: 1}}}");
    RawBsonDocument invalidate =
        RawBsonDocument.parse(
            "{operationType: 'invalidate', clusterTime: {'$timestamp': {t: 1500, i: 2}}}");
    this.stream.offerBatch(
        new ChangeStreamBatch(
            List.of(dropDatabase, invalidate),
            resumeTokenForTime(1600),
            Optional.of(new BsonTimestamp(1600, 0))));
    this.stream.awaitCalls(3);

    // The ns-less events (dropDatabase, invalidate) do not touch the collection: it stays seeded at
    // 1000, so idle is 600 (>= threshold) and it counts inactive.
    assertThat(gauge("inactiveIndexes")).isEqualTo(1);
  }

  @Test
  public void transientFailureReopensFromLastResumeToken() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    this.observer.register(newGenerationId(), namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("boom")));
    this.stream.offerBatch(emptyBatch(1100));
    this.stream.awaitCalls(3);
    this.stream.awaitOpens(2);

    // The reopen resumed from the token of the last successful batch, and activity survived.
    BsonDocument reopenToken = this.stream.lastStartAfter;
    assertThat(reopenToken).isEqualTo(resumeTokenForTime(1000));
    assertThat(reopens()).isEqualTo(1);
    assertThat(baselineResets()).isEqualTo(0);
    this.stream.awaitCalls(4);
  }

  @Test
  public void repeatedFailureReopensAtTipAndResetsBaselines() throws Exception {
    UUID collection = UUID.randomUUID();
    startObserver();
    this.observer.register(newGenerationId(), namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    // Two consecutive failures: the resume-from-token attempt itself fails.
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("first")));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("second")));
    this.stream.offerBatch(emptyBatch(5000));
    this.stream.awaitCalls(4);
    this.stream.awaitOpens(3);

    // The second reopen started at the tip (no startAfter) and baselines were reset: the
    // collection re-seeds at the new position instead of reporting 4000s idle.
    assertThat(this.stream.lastStartAfter).isNull();
    assertThat(baselineResets()).isEqualTo(1);
    this.stream.awaitCalls(5);
  }

  @Test
  public void registrationDuringResetWindowDoesNotSeedFromStalePosition() throws Exception {
    startObserver();
    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("first")));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("second")));
    this.stream.awaitOpens(3);

    // Registered after the baseline reset, before the reopened stream delivered a batch: the
    // stale pre-outage position must not seed the baseline.
    this.observer.register(newGenerationId(), namespace("coll"), UUID.randomUUID());

    this.stream.offerBatch(emptyBatch(5000));
    this.stream.awaitCalls(5);

    // Seeded at the reopened position (5000): idle is ~0 (< threshold) and active, not the outage
    // duration. A stale pre-outage seed (1000) would give idle 4000 and count inactive.
    assertThat(gauge("inactiveIndexes")).isEqualTo(0);
  }

  @Test
  public void continuedFailuresAfterResetDoNotRecountResets() throws Exception {
    startObserver();
    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("f1")));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("f2")));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("f3")));
    this.stream.offerFailure(SteadyStateException.createTransient(new RuntimeException("f4")));
    this.stream.offerBatch(emptyBatch(5000));
    this.stream.awaitCalls(6);

    // One reset at the second consecutive failure; the third and fourth do not recount.
    assertThat(baselineResets()).isEqualTo(1);
  }

  @Test
  public void closeClosesStreamAndDeregistersGauges() throws Exception {
    startObserver();
    this.stream.offerBatch(emptyBatch(1000));
    this.stream.awaitCalls(2);

    this.observer.close();

    assertThat(this.stream.closed.get()).isTrue();
    assertThat(this.meterRegistry.find("test.activityObserver.trackedIndexes").gauge()).isNull();
    // Closing twice is safe.
    this.observer.close();
  }

  @Test
  public void indexIdleSecondsReflectsPerCollectionIdle() throws Exception {
    GenerationId indexA = newGenerationId();
    GenerationId indexB = newGenerationId();
    startObserver();
    this.observer.register(indexA, namespace("collA"), UUID.randomUUID());
    this.observer.register(indexB, namespace("collB"), UUID.randomUUID());

    // Seed both, then drive events at different cluster times and advance the position to 2000.
    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerBatch(batchWithEvent(event("collA", 1800), 1900));
    this.stream.offerBatch(batchWithEvent(event("collB", 1200), 2000));
    this.stream.awaitCalls(4);

    // collA last active at 1800, collB at 1200; position is 2000.
    assertThat(indexIdleGauge(indexA).value()).isEqualTo(200.0);
    assertThat(indexIdleGauge(indexB).value()).isEqualTo(800.0);
  }

  @Test
  public void indexesOnSameCollectionShareIdleValue() throws Exception {
    UUID collection = UUID.randomUUID();
    GenerationId indexA = newGenerationId();
    GenerationId indexB = newGenerationId();
    startObserver();
    this.observer.register(indexA, namespace("coll"), collection);
    this.observer.register(indexB, namespace("coll"), collection);

    this.stream.offerBatch(emptyBatch(1000));
    this.stream.offerBatch(batchWithEvent(event("coll", 1500), 1700));
    this.stream.awaitCalls(3);

    assertThat(indexIdleGauge(indexA).value()).isEqualTo(200.0);
    assertThat(indexIdleGauge(indexB).value()).isEqualTo(200.0);
  }

  @Test
  public void deregisterRemovesOnlyThatIndexGauge() throws Exception {
    GenerationId indexA = newGenerationId();
    GenerationId indexB = newGenerationId();
    startObserver();
    this.observer.register(indexA, namespace("collA"), UUID.randomUUID());
    this.observer.register(indexB, namespace("collB"), UUID.randomUUID());
    this.stream.offerBatch(emptyBatch(1000));
    this.stream.awaitCalls(2);

    this.observer.deregister(indexA);

    assertThat(findIndexIdleGauge(indexA)).isNull();
    assertThat(findIndexIdleGauge(indexB)).isNotNull();
  }

  @Test
  public void indexIdleSecondsIsZeroBeforeAnyBatch() {
    GenerationId index = newGenerationId();
    startObserver();
    this.observer.register(index, namespace("coll"), UUID.randomUUID());

    // No batch processed yet, so the position is null and idle reads zero.
    assertThat(indexIdleGauge(index).value()).isEqualTo(0.0);
  }

  private Gauge indexIdleGauge(GenerationId generationId) {
    return this.meterRegistry
        .get(PER_INDEX_NAMESPACE + ".indexIdleSeconds")
        .tags(indexTags(generationId))
        .gauge();
  }

  private Gauge findIndexIdleGauge(GenerationId generationId) {
    var search =
        this.meterRegistry
            .find(PER_INDEX_NAMESPACE + ".indexIdleSeconds")
            .tags(indexTags(generationId));
    return search.gauge();
  }

  private static Tags indexTags(GenerationId generationId) {
    return Tags.of(
        "generationId logString",
        generationId.uniqueString(),
        "indexId logString",
        generationId.indexId.toHexString());
  }

  private double gauge(String name) {
    return this.meterRegistry.get("test.activityObserver." + name).gauge().value();
  }

  private double reopens() {
    return this.meterRegistry.get("test.activityObserver.streamReopens").counter().count();
  }

  private double baselineResets() {
    return this.meterRegistry.get("test.activityObserver.baselineResets").counter().count();
  }

  private static GenerationId newGenerationId() {
    return new GenerationId(new ObjectId(), Generation.CURRENT);
  }

  private static MongoNamespace namespace(String collection) {
    return new MongoNamespace(DATABASE, collection);
  }

  private static ChangeStreamBatch emptyBatch(int positionSeconds) {
    return new ChangeStreamBatch(
        List.of(),
        resumeTokenForTime(positionSeconds),
        Optional.of(new BsonTimestamp(positionSeconds, 0)));
  }

  private static ChangeStreamBatch batchWithEvent(RawBsonDocument event, int positionSeconds) {
    return new ChangeStreamBatch(
        List.of(event),
        resumeTokenForTime(positionSeconds),
        Optional.of(new BsonTimestamp(positionSeconds, 0)));
  }

  private static RawBsonDocument event(String collection, int clusterTimeSeconds) {
    return RawBsonDocument.parse(
        String.format(
            "{operationType: 'insert', ns: {db: '%s', coll: '%s'},"
                + " documentKey: {_id: 1}, clusterTime: {'$timestamp': {t: %d, i: 1}}}",
            DATABASE, collection, clusterTimeSeconds));
  }

  private static BsonDocument resumeTokenForTime(int seconds) {
    // Resume token binary format: 0x82 (timestamp identifier) followed by the 8-byte long value.
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.put((byte) 0x82);
    buf.putLong(new BsonTimestamp(seconds, 0).getValue());
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(buf.array())));
  }

  /**
   * A scripted stream: getNext blocks until the test offers the next step, so each assertion can
   * synchronize on the read loop having fully processed all previously offered steps (the read loop
   * only asks for step N+1 after finishing step N).
   */
  private static final class ScriptedStream
      implements ChangeStreamMongoClient<SteadyStateException> {

    private final BlockingQueue<Step> script = new LinkedBlockingQueue<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger opens = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile BsonDocument lastStartAfter;

    void offerBatch(ChangeStreamBatch batch) {
      this.script.add(new Step(batch, null));
    }

    void offerFailure(SteadyStateException failure) {
      this.script.add(new Step(null, failure));
    }

    /** Waits until getNext has been entered {@code count} times. */
    void awaitCalls(int count) throws InterruptedException {
      awaitAtLeast(this.calls, count);
    }

    /** Waits until the stream factory has been invoked {@code count} times. */
    void awaitOpens(int count) throws InterruptedException {
      awaitAtLeast(this.opens, count);
    }

    private static void awaitAtLeast(AtomicInteger counter, int count) throws InterruptedException {
      long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
      while (counter.get() < count) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("timed out waiting for " + count + ", have " + counter.get());
        }
        Thread.sleep(1);
      }
    }

    @Override
    public ChangeStreamBatch getNext() throws SteadyStateException {
      this.calls.incrementAndGet();
      Step step;
      try {
        step = this.script.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw SteadyStateException.createShutDown();
      }
      if (step.failure != null) {
        throw step.failure;
      }
      return step.batch;
    }

    @Override
    public void close() {
      this.closed.set(true);
    }

    private record Step(ChangeStreamBatch batch, SteadyStateException failure) {}
  }
}
