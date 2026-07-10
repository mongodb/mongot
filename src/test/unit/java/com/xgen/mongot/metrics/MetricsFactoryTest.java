package com.xgen.mongot.metrics;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class MetricsFactoryTest {
  private static final String YES_MONGOT_NAME = "yesMongot";
  private static final String NO_MONGOT_NAME = "noMongot";

  private enum SampleEnum {
    YES_MONGOT,
    NO_MONGOT,
  }

  @Test
  public void testCounter() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    Counter metricsFactoryCounter = metricsFactory.counter("counter");
    Counter meterRegistryCounter =
        meterRegistry.counter("testMetricsFactory" + ".counter", Tags.empty());

    Assert.assertSame(metricsFactoryCounter, meterRegistryCounter);
    Assert.assertSame(metricsFactoryCounter, metricsFactory.counter("counter"));
  }

  @Test
  public void testSimpleGauge() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    AtomicLong metricsFactoryGauge = metricsFactory.numGauge("gauge");
    metricsFactoryGauge.incrementAndGet();

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    Assert.assertEquals(gauge.value(), metricsFactoryGauge.get(), 0.0);
  }

  @Test
  public void testObjectGauge() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    List<String> list =
        metricsFactory.objectValueGauge("objectGauge", new ArrayList<>(), List::size);

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    list.add("foo");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    list.add("bar");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    Assert.assertSame(list, metricsFactory.objectValueGauge("objectGauge", list, List::size));
  }

  @Test
  public void testCollectionGauge() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    List<String> list = metricsFactory.collectionSizeGauge("collectionGauge", new ArrayList<>());

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    list.add("foo");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    list.add("bar");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    Assert.assertSame(list, metricsFactory.objectValueGauge("collectionGauge", list, List::size));
  }

  @Test
  public void testRecreateGauge() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    // create a gauge
    List<String> list =
        metricsFactory.objectValueGauge("objectGauge", new ArrayList<>(), List::size);

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.GAUGE, meters.get(0).getId().getType());
    Gauge gauge = (Gauge) meters.get(0);

    list.add("foo");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    list.add("bar");
    Assert.assertEquals(list.size(), gauge.value(), 0.0);

    Assert.assertSame(list, metricsFactory.objectValueGauge("objectGauge", list, List::size));

    // create a second gauge with the same name
    // by default, list2 is ignored by Micrometer, so this tests that we overwrite the old gauge
    List<String> list2 =
        metricsFactory.objectValueGauge("objectGauge", new ArrayList<>(), List::size);

    List<Meter> meters2 = meterRegistry.getMeters();
    Assert.assertEquals(1, meters2.size());
    Assert.assertSame(Meter.Type.GAUGE, meters2.get(0).getId().getType());
    Gauge gauge2 = (Gauge) meters2.get(0);

    list2.add("foo2");
    Assert.assertEquals(list2.size(), gauge2.value(), 0.0);
  }

  @Test
  public void testSummaryNoPercentiles() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    DistributionSummary metricsFactorySummary = metricsFactory.summary("histogram", new double[0]);
    DistributionSummary meterRegistrySummary =
        meterRegistry.summary("testMetricsFactory" + ".histogram");

    Assert.assertSame(metricsFactorySummary, meterRegistrySummary);
    Assert.assertSame(metricsFactorySummary, metricsFactory.summary("histogram"));
  }

  @Test
  public void testSummaryWithPercentiles() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    DistributionSummary metricsFactorySummary =
        metricsFactory.summary("histogram", 0.5, 0.75, 0.9, 0.99);
    DistributionSummary meterRegistrySummary =
        meterRegistry.summary("testMetricsFactory" + ".histogram");

    Assert.assertSame(metricsFactorySummary, meterRegistrySummary);
    Assert.assertSame(
        metricsFactorySummary, metricsFactory.summary("histogram", 0.5, 0.75, 0.9, 0.99));
  }

  @Test
  public void histogram_slo_buckets() {
    MeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    DistributionSummary metricsFactoryHistogram = metricsFactory.histogram("histogram", 1, 50, 100);

    for (int i = 0; i < 100; i++) {
      metricsFactoryHistogram.record(i);
    }

    var histogramCounts = metricsFactoryHistogram.takeSnapshot().histogramCounts();
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
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    Assert.assertThrows(
        IllegalArgumentException.class, () -> metricsFactory.histogram("histogram"));
    Assert.assertThrows(
        IllegalArgumentException.class, () -> metricsFactory.histogram("histogram", Tags.empty()));
  }

  @Test
  public void testTimerWithPercentiles() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    var defaultTag =
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.SECONDS));
    Timer metricsFactoryTimer = metricsFactory.timer("timer", Tags.empty(), 0.5, 0.75, 0.9, 0.99);
    Timer meterRegistryTimer = meterRegistry.timer("testMetricsFactory" + ".timer", defaultTag);

    Assert.assertSame(metricsFactoryTimer, meterRegistryTimer);
    Assert.assertSame(
        metricsFactoryTimer, metricsFactory.timer("timer", defaultTag, 0.5, 0.75, 0.9, 0.99));
  }

  @Test
  public void testLongTaskTimerNoPercentiles() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    LongTaskTimer metricsFactoryLongTaskTimer = metricsFactory.longTaskTimer("longTaskTimer");
    LongTaskTimer meterRegistryLongTaskTimer =
        meterRegistry.more().longTaskTimer("testMetricsFactory" + ".longTaskTimer");

    Assert.assertSame(metricsFactoryLongTaskTimer, meterRegistryLongTaskTimer);
    Assert.assertSame(metricsFactoryLongTaskTimer, metricsFactory.longTaskTimer("longTaskTimer"));
  }

  @Test
  public void testLongTaskTimerWithPercentiles() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    LongTaskTimer metricsFactoryLongTaskTimer =
        metricsFactory.longTaskTimer("longTaskTimer", 0.5, 0.75, 0.9, 0.99);
    LongTaskTimer meterRegistryLongTaskTimer =
        meterRegistry.more().longTaskTimer("testMetricsFactory" + ".longTaskTimer");

    Assert.assertSame(metricsFactoryLongTaskTimer, meterRegistryLongTaskTimer);
    Assert.assertSame(
        metricsFactoryLongTaskTimer,
        metricsFactory.longTaskTimer("longTaskTimer", 0.5, 0.75, 0.9, 0.99));
  }

  @Test
  public void testReRegisteringMetric() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    metricsFactory.numGauge("gauge");
    metricsFactory.numGauge("gauge");

    Assert.assertEquals(1, metricsFactory.getAllRegisteredMeters().size());
  }

  @Test
  public void testMeterTags() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    Counter classCounter = metricsFactory.counter("counter", Tags.of(Tag.of("ID", "1")));

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.COUNTER, meters.get(0).getId().getType());
    Counter registryCounter = (Counter) meters.get(0);

    Assert.assertEquals(classCounter.getId().getTags(), registryCounter.getId().getTags());
  }

  @Test
  public void testTags() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.of(Tag.of("ID", "2"));
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);

    DistributionSummary classDistSummary = metricsFactory.summary("histogram", new double[0]);

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.DISTRIBUTION_SUMMARY, meters.get(0).getId().getType());
    DistributionSummary registryDistSummary = (DistributionSummary) meters.get(0);

    Assert.assertEquals(tags, registryDistSummary.getId().getTagsAsIterable());
    Assert.assertEquals(classDistSummary.getId().getTags(), registryDistSummary.getId().getTags());
  }

  @Test
  public void testClassAndMeterTags() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.of(Tag.of("ClassID", "2"));
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);

    Tags meterTags = Tags.of(Tag.of("Operation", "Indexing"));
    Timer classTimer = metricsFactory.timer("timer", meterTags);

    List<Meter> meters = meterRegistry.getMeters();
    Assert.assertEquals(1, meters.size());
    Assert.assertSame(Meter.Type.TIMER, meters.get(0).getId().getType());
    Timer registryTimer = (Timer) meters.get(0);

    var defaultTag =
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.SECONDS));
    Assert.assertEquals(
        tags.and(meterTags.and(defaultTag)), registryTimer.getId().getTagsAsIterable());
    Assert.assertEquals(classTimer.getId().getTags(), registryTimer.getId().getTags());
  }

  @Test
  public void testChildMetricsFactory() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.of(Tag.of("ClassID", "2"));
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);

    MetricsFactory childMetricsFactory = metricsFactory.childMetricsFactory("child");
    Counter counter = childMetricsFactory.counter("counter");
    Assert.assertEquals("testMetricsFactory.child" + ".counter", counter.getId().getName());
  }

  @Test
  public void testSiblingMetricsFactory() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags parentTags = Tags.of(Tag.of("ClassID", "2"));
    MetricsFactory metricsFactory =
        new MetricsFactory("testMetricsFactory", meterRegistry, parentTags);

    Tags siblingTags = Tags.of(Tag.of("phase", "index"));
    MetricsFactory siblingMetricsFactory =
        metricsFactory.siblingMetricsFactory("sibling", siblingTags);
    Counter counter = siblingMetricsFactory.counter("counter");

    // Independent namespace: not prefixed with the parent's namespace.
    Assert.assertEquals("sibling.counter", counter.getId().getName());
    // Sibling tags are applied; the parent's factory tags are not inherited.
    Assert.assertEquals(siblingTags, counter.getId().getTagsAsIterable());
    // The parent's registry is shared, so the same meter is returned.
    Assert.assertSame(counter, meterRegistry.counter("sibling.counter", siblingTags));
  }

  @Test
  public void testGet() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.of(Tag.of("ClassID", "2"));
    Tags meterTags = Tags.of(Tag.of("MeterID", "1"));
    Tags meterTags1 = Tags.of(Tag.of("MeterID", "2"));
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);

    Counter counter = metricsFactory.counter("counter", meterTags);
    metricsFactory.counter("counter", meterTags1);
    counter.increment();
    Assert.assertEquals(1, metricsFactory.get("counter", meterTags).counter().count(), 0);
    Assert.assertEquals(0, metricsFactory.get("counter", meterTags1).counter().count(), 0);
  }

  @Test
  public void testGetEnumCounterMap() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.empty();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);
    Map<SampleEnum, Counter> output = metricsFactory.getEnumCounterMap(SampleEnum.class);
    Counter expectedYesCounter = metricsFactory.get(YES_MONGOT_NAME).counter();
    Counter expectedNoCounter = metricsFactory.get(NO_MONGOT_NAME).counter();
    Assert.assertEquals(expectedYesCounter, output.get(SampleEnum.YES_MONGOT));
    Assert.assertEquals(expectedNoCounter, output.get(SampleEnum.NO_MONGOT));
  }

  @Test
  public void testGetMeterIncorrectSubclass() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    metricsFactory.counter("counter");

    Assert.assertThrows(MeterNotFoundException.class, () -> metricsFactory.get("counter").gauge());
  }

  @Test
  public void testGetMeterIncorrectMeterName() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    metricsFactory.counter("counter");

    Assert.assertThrows(MeterNotFoundException.class, () -> metricsFactory.get("gauge").meter());
  }

  @Test
  public void testGetAllMeters() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry);

    metricsFactory.counter("counter");
    metricsFactory.numGauge("numGauge");
    metricsFactory.objectValueGauge("objectValueGauge", new ArrayList<>(), ArrayList::size);
    metricsFactory.collectionSizeGauge("collectionGauge", new ArrayList<>());
    metricsFactory.timer("timer");
    metricsFactory.summary("histogram", new double[0]);

    Assert.assertEquals(6, metricsFactory.getAllRegisteredMeters().size());
    Assert.assertEquals(10, meterRegistry.getMeters().size());
    metricsFactory.close();
    Assert.assertEquals(0, metricsFactory.getAllRegisteredMeters().size());
    Assert.assertEquals(0, meterRegistry.getMeters().size());
  }

  @Test
  public void testClose() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Tags tags = Tags.of(Tag.of("ClassID", "1"));
    Tags tags1 = Tags.of(Tag.of("ClassID", "2"));
    MetricsFactory metricsFactory = new MetricsFactory("testMetricsFactory", meterRegistry, tags);
    MetricsFactory metricsFactory1 = new MetricsFactory("testMetricsFactory", meterRegistry, tags1);

    metricsFactory.counter("counter");
    metricsFactory1.counter("counter");
    metricsFactory.get("counter");
    metricsFactory1.get("counter");

    metricsFactory1.close();

    metricsFactory.get("counter");
    Assert.assertThrows(
        MeterNotFoundException.class, () -> metricsFactory1.get("counter").counter());
    Assert.assertEquals(1, meterRegistry.getMeters().size());
  }
}
