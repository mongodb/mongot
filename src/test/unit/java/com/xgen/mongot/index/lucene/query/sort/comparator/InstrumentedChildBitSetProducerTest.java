package com.xgen.mongot.index.lucene.query.sort.comparator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InstrumentedChildBitSetProducerTest {
  private static final TermQuery SELECTED = new TermQuery(new Term("selected", "true"));

  private Directory directory;
  private DirectoryReader reader;
  private MeterRegistry registry;
  private Timer latency;
  private DistributionSummary matchingChildren;

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(child(true), child(true), child(false), root()));
      writer.forceMerge(1);
    }
    this.reader = DirectoryReader.open(this.directory);
    this.registry = new SimpleMeterRegistry();
    this.latency = this.registry.timer("childEvaluationLatency");
    this.matchingChildren = this.registry.summary("matchingChildrenPerSegment");
  }

  @After
  public void tearDown() throws IOException {
    this.reader.close();
    this.directory.close();
    this.registry.close();
  }

  @Test
  public void getBitSet_firstCall_recordsLatencyAndCardinality() throws IOException {
    var producer = instrumented(new QueryBitSetProducer(SELECTED));

    BitSet bitSet = producer.getBitSet(leaf());

    assertThat(bitSet.cardinality()).isEqualTo(2);
    assertThat(this.latency.count()).isEqualTo(1);
    assertThat(this.matchingChildren.count()).isEqualTo(1);
    assertThat(this.matchingChildren.totalAmount()).isEqualTo(2.0);
  }

  @Test
  public void getBitSet_repeatedCallsOnSameSegment_recordOnlyTheFirst() throws IOException {
    var producer = instrumented(new QueryBitSetProducer(SELECTED));

    producer.getBitSet(leaf());
    producer.getBitSet(leaf());
    producer.getBitSet(leaf());

    // The delegate serves calls 2 and 3 from its per-segment cache; recording them would drive
    // the timer toward zero and triple the reported candidate count.
    assertThat(this.latency.count()).isEqualTo(1);
    assertThat(this.matchingChildren.count()).isEqualTo(1);
    assertThat(this.matchingChildren.totalAmount()).isEqualTo(2.0);
  }

  @Test
  public void getBitSet_segmentWithNoMatchingChildren_recordsZeroCandidates() throws IOException {
    var producer =
        instrumented(new QueryBitSetProducer(new TermQuery(new Term("selected", "absent"))));

    BitSet bitSet = producer.getBitSet(leaf());

    // QueryBitSetProducer returns null rather than an empty set when nothing matches.
    assertThat(bitSet).isNull();
    assertThat(this.matchingChildren.count()).isEqualTo(1);
    assertThat(this.matchingChildren.totalAmount()).isEqualTo(0.0);
  }

  @Test
  public void getBitSet_delegateResult_isReturnedUnchanged() throws IOException {
    BitSetProducer delegate = new QueryBitSetProducer(SELECTED);
    BitSet expected = delegate.getBitSet(leaf());

    BitSet actual = instrumented(context -> expected).getBitSet(leaf());

    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  public void getBitSet_failedEvaluation_retryIsMeasured() throws IOException {
    BitSet expected = new QueryBitSetProducer(SELECTED).getBitSet(leaf());
    AtomicInteger calls = new AtomicInteger();
    var producer =
        instrumented(
            context -> {
              if (calls.getAndIncrement() == 0) {
                throw new IOException("failed evaluation");
              }
              return expected;
            });

    assertThrows(IOException.class, () -> producer.getBitSet(leaf()));
    BitSet actual = producer.getBitSet(leaf());
    producer.getBitSet(leaf());

    assertThat(actual).isSameInstanceAs(expected);
    assertThat(this.latency.count()).isEqualTo(1);
    assertThat(this.matchingChildren.count()).isEqualTo(1);
    assertThat(this.matchingChildren.totalAmount()).isEqualTo(2.0);
  }

  @Test
  public void getBitSet_concurrentFirstReads_onlyOneEvaluation() throws Exception {
    BitSet expected = new QueryBitSetProducer(SELECTED).getBitSet(leaf());
    AtomicBoolean cached = new AtomicBoolean();
    AtomicInteger evaluations = new AtomicInteger();
    CountDownLatch evaluating = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // Like QueryBitSetProducer, this delegate's cache lookup and population are not atomic.
    var producer =
        instrumented(
            context -> {
              if (!cached.get()) {
                evaluations.incrementAndGet();
                evaluating.countDown();
                await(release);
                cached.set(true);
              }
              return expected;
            });
    FutureTask<BitSet> first = new FutureTask<>(() -> producer.getBitSet(leaf()));
    FutureTask<BitSet> second = new FutureTask<>(() -> producer.getBitSet(leaf()));
    Thread firstThread = new Thread(first);
    Thread secondThread = new Thread(second);

    try {
      firstThread.start();
      assertThat(evaluating.await(5, TimeUnit.SECONDS)).isTrue();
      secondThread.start();
      awaitBlockedOrWaiting(secondThread);

      assertThat(evaluations.get()).isEqualTo(1);
      release.countDown();
      assertThat(first.get(5, TimeUnit.SECONDS)).isSameInstanceAs(expected);
      assertThat(second.get(5, TimeUnit.SECONDS)).isSameInstanceAs(expected);
      assertThat(this.latency.count()).isEqualTo(1);
      assertThat(this.matchingChildren.count()).isEqualTo(1);
    } finally {
      release.countDown();
      firstThread.join(5000);
      secondThread.join(5000);
    }
  }

  @Test
  public void getBitSet_withoutCacheHelper_recordsEveryEvaluation() throws IOException {
    var producer = instrumented(new QueryBitSetProducer(SELECTED));
    LeafReader segment = leaf().reader();
    segment.incRef();
    try (LeafReader uncached =
        new FilterLeafReader(segment) {
          @Override
          public CacheHelper getCoreCacheHelper() {
            return null;
          }

          @Override
          public CacheHelper getReaderCacheHelper() {
            return null;
          }
        }) {
      producer.getBitSet(uncached.getContext());
      producer.getBitSet(uncached.getContext());

      assertThat(this.latency.count()).isEqualTo(2);
      assertThat(this.matchingChildren.count()).isEqualTo(2);
      assertThat(this.matchingChildren.totalAmount()).isEqualTo(4.0);
    }
  }

  private static void await(CountDownLatch latch) throws IOException {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private static void awaitBlockedOrWaiting(Thread thread) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.getState() != Thread.State.BLOCKED
        && thread.getState() != Thread.State.WAITING
        && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    assertThat(thread.getState()).isAnyOf(Thread.State.BLOCKED, Thread.State.WAITING);
  }

  private InstrumentedChildBitSetProducer instrumented(BitSetProducer delegate) {
    return new InstrumentedChildBitSetProducer(delegate, this.latency, this.matchingChildren);
  }

  private LeafReaderContext leaf() {
    return this.reader.leaves().getFirst();
  }

  private static Document child(boolean selected) {
    Document child = new Document();
    child.add(new StringField("selected", Boolean.toString(selected), Field.Store.NO));
    return child;
  }

  private static Document root() {
    Document root = new Document();
    root.add(new StringField("root", "true", Field.Store.NO));
    return root;
  }
}
