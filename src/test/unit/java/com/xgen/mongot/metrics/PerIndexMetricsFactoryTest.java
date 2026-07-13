package com.xgen.mongot.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class PerIndexMetricsFactoryTest {
  private static final String YES_MONGOT_NAME = "yesMongot";

  private static final String INDEX_GENERATION_ID = "genId";
  private static final String INDEX_ID = "indexId";
  private static final Tags INDEX_TAGS = Tags.of("generationId logString", INDEX_GENERATION_ID);
  private static final Tags INDEX_IDS = Tags.of("indexId logString", INDEX_ID);

  private final MeterAndFtdcRegistry meterAndFtdcRegistry =
      MeterAndFtdcRegistry.createWithSimpleRegistries();
  private final PerIndexMetricsFactory metricsFactory =
      new PerIndexMetricsFactory("test", this.meterAndFtdcRegistry, INDEX_GENERATION_ID, INDEX_ID);

  private enum SampleEnum {
    YES_MONGOT,
    NO_MONGOT,
  }

  private static boolean hasIndexGenerationId(Meter meter) {
    return meter.getId().getTags().stream()
            .filter(
                tag ->
                    tag.getKey().equals("generationId logString")
                        && tag.getValue().equals(INDEX_GENERATION_ID))
            .count()
        > 0;
  }

  private static boolean hasIndexId(Meter meter) {
    return meter.getId().getTags().stream()
            .filter(
                tag -> tag.getKey().equals("indexId logString") && tag.getValue().equals(INDEX_ID))
            .count()
        > 0;
  }

  private String name(String name) {
    return this.metricsFactory.metricName(name);
  }

  @Test
  public void testCounter() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.counter("counter").increment();
    // There will be a regular counter on registry and an index tagged counter on ftdcRegistry.
    // Both will be incremented.
    var processCounter = meterRegistry.get(name("counter")).counter();
    assertFalse(hasIndexGenerationId(processCounter));
    assertFalse(hasIndexId(processCounter));
    assertEquals(1, (int) meterRegistry.get(name("counter")).counter().count());
    assertEquals(1, (int) ftdcRegistry.get(name("counter")).tags(INDEX_TAGS).counter().count());
    assertEquals(1, (int) ftdcRegistry.get(name("counter")).tags(INDEX_IDS).counter().count());
  }

  @Test
  public void testPerIndexCounter() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.perIndexCounter("counter").increment();
    assertEquals(1, (int) meterRegistry.get(name("counter")).tags(INDEX_TAGS).counter().count());
    assertEquals(1, (int) meterRegistry.get(name("counter")).tags(INDEX_IDS).counter().count());
    assertThrows(MeterNotFoundException.class, () -> ftdcRegistry.get(name("counter")).meters());
  }

  @Test
  public void testSimpleGauge() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    AtomicLong metricsFactoryGauge = this.metricsFactory.perIndexNumGauge("gauge");
    metricsFactoryGauge.incrementAndGet();

    List<Meter> meters = meterRegistry.getMeters();
    assertEquals(1, meters.size());
    assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    assertEquals(gauge.value(), metricsFactoryGauge.get(), 0.0);
    assertTrue(hasIndexGenerationId(gauge));
    assertTrue(hasIndexId(gauge));
  }

  @Test
  public void testObjectGauge() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    List<String> list =
        this.metricsFactory.perIndexObjectValueGauge("objectGauge", new ArrayList<>(), List::size);

    List<Meter> meters = meterRegistry.getMeters();
    assertEquals(1, meters.size());
    assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    list.add("foo");
    assertEquals(list.size(), gauge.value(), 0.0);

    list.add("bar");
    assertEquals(list.size(), gauge.value(), 0.0);

    assertSame(list, this.metricsFactory.perIndexObjectValueGauge("objectGauge", list, List::size));
  }

  @Test
  public void testCollectionGauge() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    List<String> list =
        this.metricsFactory.perIndexCollectionSizeGauge("collectionGauge", new ArrayList<>());

    List<Meter> meters = meterRegistry.getMeters();
    assertEquals(1, meters.size());
    assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    list.add("foo");
    assertEquals(list.size(), gauge.value(), 0.0);

    list.add("bar");
    assertEquals(list.size(), gauge.value(), 0.0);

    assertSame(
        list, this.metricsFactory.perIndexObjectValueGauge("collectionGauge", list, List::size));
  }

  @Test
  public void summary() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.summary("summary");
    var processSummary = meterRegistry.get(name("summary")).summary();
    assertFalse(hasIndexGenerationId(processSummary));
    assertFalse(hasIndexId(processSummary));
    ftdcRegistry.get(name("summary")).tags(INDEX_TAGS).summary();
    ftdcRegistry.get(name("summary")).tags(INDEX_IDS).summary();
  }

  @Test
  public void histogram_slo_buckets() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    var histogram = this.metricsFactory.histogram("histogram", 1, 50, 100);

    for (int i = 0; i < 100; i++) {
      histogram.record(i);
    }

    var processHistogram = meterRegistry.get(name("histogram")).summary();
    assertFalse(hasIndexGenerationId(processHistogram));
    assertFalse(hasIndexId(processHistogram));
    ftdcRegistry.get(name("histogram")).tags(INDEX_TAGS).summary();
    ftdcRegistry.get(name("histogram")).tags(INDEX_IDS).summary();

    var histogramCounts = processHistogram.takeSnapshot().histogramCounts();
    Assert.assertEquals(3, histogramCounts.length);

    Assert.assertEquals(1.0, histogramCounts[0].bucket(), 0.01);
    Assert.assertEquals(2, (int) histogramCounts[0].count());

    Assert.assertEquals(50.0, histogramCounts[1].bucket(), 0.01);
    Assert.assertEquals(51, (int) histogramCounts[1].count());

    Assert.assertEquals(100.0, histogramCounts[2].bucket(), 0.01);
    Assert.assertEquals(100, (int) histogramCounts[2].count());
  }

  @Test
  public void histogram_emptyBuckets_throws() {
    Assert.assertThrows(
        IllegalArgumentException.class, () -> this.metricsFactory.histogram("histogram"));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> this.metricsFactory.histogram("histogram", Tags.empty()));
  }

  @Test
  public void testTimerWithPercentiles() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    var defaultTag =
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.SECONDS));
    Timer metricsFactoryTimer =
        this.metricsFactory.perIndexTimer("timer", Tags.empty(), 0.5, 0.75, 0.9, 0.99);
    Timer meterRegistryTimer =
        meterRegistry.timer(
            name("timer"),
            Tags.of("generationId logString", "genId")
                .and("indexId logString", "indexId")
                .and(defaultTag));

    assertSame(metricsFactoryTimer, meterRegistryTimer);
    assertSame(
        metricsFactoryTimer,
        this.metricsFactory.perIndexTimer("timer", Tags.empty(), 0.5, 0.75, 0.9, 0.99));
  }

  @Test
  public void testLongTaskTimerNoPercentiles() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    LongTaskTimer metricsFactoryLongTaskTimer =
        this.metricsFactory.perIndexLongTaskTimer("longTaskTimer");
    LongTaskTimer meterRegistryLongTaskTimer =
        meterRegistry
            .more()
            .longTaskTimer(
                name("longTaskTimer"),
                Tags.of("generationId logString", "genId").and("indexId logString", "indexId"));

    assertSame(metricsFactoryLongTaskTimer, meterRegistryLongTaskTimer);
    assertSame(
        metricsFactoryLongTaskTimer, this.metricsFactory.perIndexLongTaskTimer("longTaskTimer"));
  }

  @Test
  public void testLongTaskTimerWithPercentiles() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    LongTaskTimer metricsFactoryLongTaskTimer =
        this.metricsFactory.perIndexLongTaskTimer("longTaskTimer", 0.5, 0.75, 0.9, 0.99);
    LongTaskTimer meterRegistryLongTaskTimer =
        meterRegistry
            .more()
            .longTaskTimer(
                name("longTaskTimer"),
                Tags.of("generationId logString", "genId").and("indexId logString", "indexId"));

    assertSame(metricsFactoryLongTaskTimer, meterRegistryLongTaskTimer);
    assertSame(
        metricsFactoryLongTaskTimer,
        this.metricsFactory.perIndexLongTaskTimer("longTaskTimer", 0.5, 0.75, 0.9, 0.99));
  }

  @Test
  public void testReRegisteringMetric() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.perIndexNumGauge("gauge");
    this.metricsFactory.perIndexNumGauge("gauge");

    assertEquals(1, meterRegistry.getMeters().size());
    assertTrue(ftdcRegistry.getMeters().isEmpty());

    this.metricsFactory.counter("counter");
    this.metricsFactory.counter("counter");

    assertEquals(2, meterRegistry.getMeters().size());
    assertEquals(1, ftdcRegistry.getMeters().size());
  }

  @Test
  public void testMeterTags() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    var tags = Tags.of("ID", "1");
    this.metricsFactory.counter("counter", tags).increment();
    assertEquals(1, (int) meterRegistry.get(name("counter")).tags(tags).counter().count());
    assertEquals(
        1, (int) ftdcRegistry.get(name("counter")).tags(tags.and(INDEX_TAGS)).counter().count());
    assertEquals(
        1, (int) ftdcRegistry.get(name("counter")).tags(tags.and(INDEX_IDS)).counter().count());
  }

  @Test
  public void testGetAllMeters() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.counter("counter");
    this.metricsFactory.perIndexNumGauge("numGauge");
    this.metricsFactory.perIndexObjectValueGauge(
        "objectValueGauge", new ArrayList<>(), ArrayList::size);
    this.metricsFactory.perIndexCollectionSizeGauge("collectionGauge", new ArrayList<>());
    this.metricsFactory.perIndexTimer("timer", Tags.empty());
    this.metricsFactory.summary("histogram", new double[0]);

    assertEquals(10, meterRegistry.getMeters().size());
    assertEquals(2, ftdcRegistry.getMeters().size());
  }

  @Test
  public void testIndexFactoryEnumCounter() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.getEnumCounterMap(SampleEnum.class);
    assertFalse(hasIndexGenerationId(meterRegistry.get(name(YES_MONGOT_NAME)).counter()));
    assertFalse(hasIndexId(meterRegistry.get(name(YES_MONGOT_NAME)).counter()));
    ftdcRegistry.get(name(YES_MONGOT_NAME)).tags(INDEX_TAGS).counter();
    ftdcRegistry.get(name(YES_MONGOT_NAME)).tags(INDEX_IDS).counter();
  }

  @Test
  public void testPerIndexFactoryEnumCounter() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    this.metricsFactory.getPerIndexEnumCounterMap(SampleEnum.class, Tags.empty());
    meterRegistry.get(name(YES_MONGOT_NAME)).tags(INDEX_TAGS).counter();
    meterRegistry.get(name(YES_MONGOT_NAME)).tags(INDEX_IDS).counter();
    assertThrows(
        MeterNotFoundException.class, () -> ftdcRegistry.get(name(YES_MONGOT_NAME)).meters());
  }

  @Test
  public void testIndexFactoryChildMetricsFactory() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();

    var metricsFactory = this.metricsFactory.childMetricsFactory("child");
    metricsFactory.counter("counter");
    meterRegistry.get("test.child.counter").counter();
  }

  @Test
  public void testClose() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var ftdcRegistry = this.meterAndFtdcRegistry.ftdcRegistry();

    var metricsFactory0 = new PerIndexMetricsFactory("test", this.meterAndFtdcRegistry, "1", "1");
    var metricsFactory1 = new PerIndexMetricsFactory("test", this.meterAndFtdcRegistry, "2", "1");

    metricsFactory0.counter("counter");
    metricsFactory1.counter("counter");
    meterRegistry.get(name("counter")).counter();
    ftdcRegistry.get(name("counter")).counter();

    metricsFactory1.close();

    meterRegistry.get(name("counter")).counter();
    assertThrows(
        MeterNotFoundException.class,
        () -> meterRegistry.get("counter").tags(Tags.of("generationId logString", "2")).counter());
    assertEquals(1, meterRegistry.getMeters().size());
  }

  // Exercises the auto-embedding-facing wiring — identitySafeClose threaded to the
  // closed per-index factories and the ensure-re-register delegate. Two factories
  // share a generation id (as matview attempts a0/a1 do); dropping the predecessor must not remove
  // the successor's live gauge, and ensure re-registers it if it is evicted.
  @Test
  public void testMatviewIdentitySafeCloseAndSelfHeal() {
    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    PerIndexMetricsFactory a0 =
        new PerIndexMetricsFactory(
            "test",
            this.meterAndFtdcRegistry,
            "matview-abc-u0",
            "abc",
            /* identitySafeClose= */ true);
    PerIndexMetricsFactory a1 =
        new PerIndexMetricsFactory(
            "test",
            this.meterAndFtdcRegistry,
            "matview-abc-u0",
            "abc",
            /* identitySafeClose= */ true);

    a0.perIndexObjectValueGauge("leaderStatus", new AtomicLong(), Number::doubleValue);
    AtomicLong a1State =
        a1.perIndexObjectValueGauge("leaderStatus", new AtomicLong(), Number::doubleValue);
    a1State.set(1);

    // Dropping a0 must leave a1's live gauge intact (Fix B).
    a0.close();
    Gauge gauge = meterRegistry.get(name("leaderStatus")).gauge();
    assertEquals(1.0, gauge.value(), 0.0);

    // If the gauge is evicted, ensure re-registers it against the existing state object (Fix D).
    meterRegistry.remove(gauge);
    assertThrows(
        MeterNotFoundException.class, () -> meterRegistry.get(name("leaderStatus")).gauge());
    assertTrue(
        a1.ensurePerIndexObjectValueGaugeRegistered("leaderStatus", a1State, Number::doubleValue));
    assertEquals(1.0, meterRegistry.get(name("leaderStatus")).gauge().value(), 0.0);
  }
}
