package com.xgen.mongot.metrics.ftdc;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.MoreCollectors;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class FtdcScheduledReporterTest {
  @Test
  public void testDoesNotReportBeforeStart() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(registry);
    FtdcScheduledReporter.create(
        registry, combinedRegistry, ftdc, false, FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    Thread.sleep(100);
    verifyNoMoreInteractions(ftdc);
  }

  @Test
  public void testStartAndStopReporting() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(registry);
    var reporter =
        FtdcScheduledReporter.create(
            registry, combinedRegistry, ftdc, false, FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    reporter.start(10, TimeUnit.MILLISECONDS);
    // we report every 10 ms, so we can report 10 times in under a second.
    verify(ftdc, timeout(1000).atLeast(10)).addSample(any(), anyLong());

    reporter.stop();
    clearInvocations(ftdc);
    // should not be reporting after executor has stopped
    Thread.sleep(100);
    verifyNoInteractions(ftdc);
  }

  @Test
  public void testReportsStartTime() throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);
    reporter.report();
    verify(ftdc).addSample(argThat(doc -> doc.isInt64("start")), anyLong());
  }

  @Test
  public void testStartTimeIsFirstKeyInDocument() throws Exception {
    // t2 expects the first metric in a chunk to be the start time.
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    meterRegistry.counter("foo").increment(42);
    meterRegistry.counter("bar").increment();
    meterRegistry.gauge("baz", 5);
    reporter.report();

    verify(ftdc).addSample(argThat(doc -> doc.getFirstKey().equals("start")), anyLong());
  }

  @Test
  public void createMeterName_gauge_sortsTags() {
    Ftdc ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);
    var metrics = new MetricsFactory("test", meterRegistry, Tag.of("t1", "v1"), Tag.of("t2", "v2"));

    metrics.objectValueGauge(
        "my_gauge",
        new Object(),
        x -> 42,
        Tags.of(List.of(Tag.of("k2", "v2"), Tag.of("k1", "v1"))));
    Meter gauge =
        meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().contains("my_gauge"))
            .collect(MoreCollectors.onlyElement());

    String result = reporter.createMeterName(gauge);

    assertEquals("mongot,test.my_gauge.[tag(k1=v1),tag(k2=v2),tag(t1=v1),tag(t2=v2)]", result);
  }

  @Test
  public void createMeterName_counter_sortsTags() {
    Ftdc ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);
    var metrics = new MetricsFactory("test", meterRegistry, Tag.of("t2", "v2"), Tag.of("t1", "v1"));
    Counter counter = metrics.counter("my_counter", Tags.of("k2", "v2", "k1", "v1"));

    String result = reporter.createMeterName(counter);

    assertEquals("mongot,test.my_counter.[tag(k1=v1),tag(k2=v2),tag(t1=v1),tag(t2=v2)]", result);
  }

  @Test
  public void createMeterName_returnsCachedValue() {
    Ftdc ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);
    var metrics = new MetricsFactory("test", meterRegistry, Tag.of("t2", "v2"), Tag.of("t1", "v1"));
    Counter counter = metrics.counter("my_counter", Tags.of("k2", "v2", "k1", "v1"));

    String first = reporter.createMeterName(counter);
    String second = reporter.createMeterName(counter);

    assertEquals("mongot,test.my_counter.[tag(k1=v1),tag(k2=v2),tag(t1=v1),tag(t2=v2)]", first);
    assertSame(first, second);
  }

  @Test
  public void testReportsCounters() throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    meterRegistry.counter("foo").increment(42);
    reporter.report();

    String keyInRegistry = getFormattedKey("foo", Optional.empty());
    verify(ftdc)
        .addSample(
            argThat(
                doc ->
                    doc.isDouble(keyInRegistry) && doc.getDouble(keyInRegistry).getValue() == 42.0),
            anyLong());
  }

  @Test
  public void testReportsGauge() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    var gauge = meterRegistry.gauge("gauge", 5.0);
    reporter.report();

    String registryKey = getFormattedKey("gauge", Optional.empty());
    verify(ftdc)
        .addSample(
            argThat(
                doc -> doc.isDouble(registryKey) && doc.getDouble(registryKey).getValue() == gauge),
            anyLong());
  }

  @Test
  public void testReportsGaugeToScale() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    var gauge = meterRegistry.gauge("system.cpu.usage", 0.15);
    reporter.report();

    // "‰" is added as a key suffix.
    String registryKey = getFormattedKey("system.cpu.usage", Optional.empty()) + "‰";
    // The gauge value is multiplied by 1000.
    verify(ftdc)
        .addSample(
            argThat(
                doc ->
                    doc.isDouble(registryKey)
                        && doc.getDouble(registryKey).getValue() == gauge * 1000),
            anyLong());
  }

  @Test
  public void testReportsTimer() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    String timerMeterName = "timer";
    Tags timerTags = Tags.of("timeUnit", "milliseconds");
    Timer timer =
        Timer.builder(timerMeterName)
            .tags(timerTags)
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.75, 0.9, 0.99)
            .register(meterRegistry);

    timer.record(5, TimeUnit.MILLISECONDS);
    timer.record(10, TimeUnit.MILLISECONDS);
    timer.record(15, TimeUnit.MILLISECONDS);
    reporter.report();

    String registryKey = getFormattedKey(timerMeterName, Optional.of(timerTags));
    String registryKeyPercentiles = String.format("%s.%s", registryKey, "percentiles");

    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    verify(ftdc).addSample(docCaptor.capture(), anyLong());
    BsonDocument doc = docCaptor.getValue();

    Assert.assertTrue(doc.isString(registryKey + ".timeUnit"));
    Assert.assertEquals("milliseconds", doc.getString(registryKey + ".timeUnit").getValue());

    Assert.assertTrue(doc.isInt64(registryKey + ".count"));
    Assert.assertEquals(3, doc.getInt64(registryKey + ".count").getValue());

    Assert.assertTrue(doc.isDouble(registryKey + ".totalTimeSeconds"));
    Assert.assertEquals(30.0, doc.getDouble(registryKey + ".totalTimeSeconds").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".maxTimeSeconds"));
    Assert.assertEquals(15.0, doc.getDouble(registryKey + ".maxTimeSeconds").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".meanTimeSeconds"));
    Assert.assertEquals(10.0, doc.getDouble(registryKey + ".meanTimeSeconds").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "50th")));
    Assert.assertEquals(
        10.223616,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "50th")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "75th")));
    Assert.assertEquals(
        14.942208,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "75th")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "90th")));
    Assert.assertEquals(
        14.942208,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "90th")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "99th")));
    Assert.assertEquals(
        14.942208,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "99th")).getValue(),
        0.0);
  }

  @Test
  public void testReportsTimerToScale() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    String timerMeterName = "indexing.steadyStateChangeStream.preprocessingBatchDurations";
    Timer timer =
        Timer.builder(timerMeterName)
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.75, 0.9, 0.99)
            .register(meterRegistry);

    timer.record(5, TimeUnit.SECONDS);
    timer.record(10, TimeUnit.SECONDS);
    timer.record(15, TimeUnit.SECONDS);
    reporter.report();

    String registryKey = getFormattedKey(timerMeterName, Optional.empty());
    String registryKeyPercentiles = String.format("%s.%s", registryKey, "percentiles");

    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    verify(ftdc).addSample(docCaptor.capture(), anyLong());
    BsonDocument doc = docCaptor.getValue();

    Assert.assertTrue(doc.isInt64(registryKey + ".count"));
    Assert.assertEquals(3, doc.getInt64(registryKey + ".count").getValue());

    Assert.assertTrue(doc.isDouble(registryKey + ".totalTimeSecondsScaled"));
    Assert.assertEquals(
        30000, doc.getDouble(registryKey + ".totalTimeSecondsScaled").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".maxTimeSecondsScaled"));
    Assert.assertEquals(
        15000, doc.getDouble(registryKey + ".maxTimeSecondsScaled").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".meanTimeSecondsScaled"));
    Assert.assertEquals(
        10000, doc.getDouble(registryKey + ".meanTimeSecondsScaled").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "50thScaled")));
    Assert.assertEquals(
        9932.111872,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "50thScaled")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "75thScaled")));
    Assert.assertEquals(
        14763.95008,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "75thScaled")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "90thScaled")));
    Assert.assertEquals(
        14763.95008,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "90thScaled")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "99thScaled")));
    Assert.assertEquals(
        14763.95008,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "99thScaled")).getValue(),
        0.0);
  }

  @Test
  public void testReportsHistogram() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    String histogramName = "histogram";
    DistributionSummary histogram =
        DistributionSummary.builder(histogramName)
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.75, 0.9, 0.99)
            .register(meterRegistry);

    histogram.record(4);
    histogram.record(5);
    histogram.record(6);
    histogram.record(7);
    histogram.record(8);
    histogram.record(9);
    histogram.record(10);
    reporter.report();

    String registryKey = getFormattedKey(histogramName, Optional.empty());
    String registryKeyPercentiles = String.format("%s.%s", registryKey, "percentiles");

    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    verify(ftdc).addSample(docCaptor.capture(), anyLong());
    BsonDocument doc = docCaptor.getValue();

    Assert.assertTrue(doc.isInt64(registryKey + ".count"));
    Assert.assertEquals(7, doc.getInt64(registryKey + ".count").getValue());

    Assert.assertTrue(doc.isDouble(registryKey + ".total"));
    Assert.assertEquals(49.0, doc.getDouble(registryKey + ".total").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".max"));
    Assert.assertEquals(10.0, doc.getDouble(registryKey + ".max").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(registryKey + ".mean"));
    Assert.assertEquals(7.0, doc.getDouble(registryKey + ".mean").getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "50th")));
    Assert.assertEquals(
        7.0, doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "50th")).getValue(), 0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "75th")));
    Assert.assertEquals(
        9.25,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "75th")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "90th")));
    Assert.assertEquals(
        10.25,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "90th")).getValue(),
        0.0);

    Assert.assertTrue(doc.isDouble(String.format("%s.%s", registryKeyPercentiles, "99th")));
    Assert.assertEquals(
        10.25,
        doc.getDouble(String.format("%s.%s", registryKeyPercentiles, "99th")).getValue(),
        0.0);
  }

  @Test
  public void testSelfReportingTimer() throws IOException {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    meterRegistry.counter("foo").increment(42);
    meterRegistry.counter("bar").increment();
    meterRegistry.gauge("baz", 5);

    String timerKey =
        getFormattedKey(
            FtdcScheduledReporter.FTDC_MONGOT_REPORTING_TIMER_NAME,
            Optional.of(FtdcScheduledReporter.FTDC_MONGOT_REPORTING_TIMER_TAGS));

    // the first sample collected will not contain a time, since the metric tracks the
    // reporting time of the cycle before
    reporter.verboseRun();
    verify(ftdc, times(1))
        .addSample(
            argThat(
                doc ->
                    doc.get(String.format("%s.%s", timerKey, "count")).asInt64().getValue() == 0),
            anyLong());

    for (int i = 0; i < 10; i++) {
      reporter.verboseRun();
    }
    verify(ftdc, times(10))
        .addSample(
            argThat(
                doc ->
                    doc.containsKey(String.format("%s.%s", timerKey, "count"))
                        && doc.get(String.format("%s.%s", timerKey, "totalTimeSeconds"))
                                .asDouble()
                                .getValue()
                            != 0),
            anyLong());
  }

  @Test
  public void report_exceptionDuringMetricCollection_handlesGracefully() throws Exception {
    var ftdc = mockFtdc();
    // Create a spy registry that throws an exception when getMeters() is called
    MeterRegistry failingRegistry = Mockito.spy(new SimpleMeterRegistry());
    Mockito.when(failingRegistry.getMeters())
        .thenThrow(new RuntimeException("Simulated exception during getMeters()"));

    // Create a reporter with the failing registry
    var failingReporter =
        new FtdcScheduledReporter.Reporter(failingRegistry, failingRegistry, ftdc);

    // Report should handle the exception gracefully and not throw
    failingReporter.report();

    // Verify that getMeters() was called (which threw the exception)
    verify(failingRegistry, times(1)).getMeters();

    // Verify that ftdc.addSample was NOT called (because exception was caught)
    verifyNoInteractions(ftdc);
  }

  @Test
  public void scheduledReporter_exceptionDuringReportCycle_continuesReporting() throws Exception {
    var ftdc = mockFtdc();
    // Create a spy registry that will fail on the third call to getMeters()
    SimpleMeterRegistry realRegistry = new SimpleMeterRegistry();
    MeterRegistry meterRegistry = Mockito.spy(realRegistry);

    // Create normal metrics that should be reported
    meterRegistry.counter("test_counter").increment(100);
    meterRegistry.gauge("test_gauge", 50.0);

    // Use a counter to track getMeters() calls and throw exception on the third call
    AtomicInteger callCount = new AtomicInteger(0);
    Mockito.doAnswer(
            invocation -> {
              int count = callCount.incrementAndGet();
              if (count == 3) {
                throw new RuntimeException("Simulated exception in scheduled report cycle");
              }
              // Call the real method on the original object
              return realRegistry.getMeters();
            })
        .when(meterRegistry)
        .getMeters();

    // Create FtdcScheduledReporter instance with short interval
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(meterRegistry);
    var scheduledReporter =
        FtdcScheduledReporter.create(
            meterRegistry,
            combinedRegistry,
            ftdc,
            false,
            FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);

    // Set up captor to capture all successful reports before starting
    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);

    scheduledReporter.start(10, TimeUnit.MILLISECONDS);

    try {
      // Wait for at least 5 report() calls to happen (each calls getMeters())
      // The third report will throw an exception, but subsequent reports should continue
      verify(meterRegistry, timeout(200).atLeast(5)).getMeters();

      // Stop the reporter to prevent further calls
      scheduledReporter.stop();

      // Get the actual number of times getMeters() was called
      // This equals the number of report() calls since each report() calls getMeters()
      int getMetersCallCount = callCount.get();

      // Verify that getMeters() was called at least 5 times
      assertThat(getMetersCallCount).isAtLeast(5);

      // Verify that getMeters() was called exactly getMetersCallCount times
      verify(meterRegistry, times(getMetersCallCount)).getMeters();

      // Verify that addSample() was called (getMetersCallCount - 1) times
      // because one report (the 3rd one) failed and didn't reach addSample()
      int expectedAddSampleCount = getMetersCallCount - 1;
      verify(ftdc, times(expectedAddSampleCount)).addSample(docCaptor.capture(), anyLong());

      // Verify that we got the expected number of successful reports
      Assert.assertEquals(
          "Should have exactly " + expectedAddSampleCount + " successful reports",
          expectedAddSampleCount,
          docCaptor.getAllValues().size());

      // Verify that metrics are correctly reported in successful reports
      String counterKey = getFormattedKey("test_counter", Optional.empty());
      String gaugeKey = getFormattedKey("test_gauge", Optional.empty());

      // Check the captured documents to ensure metrics are present in successful reports
      @Var boolean foundCounter = false;
      @Var boolean foundGauge = false;
      for (BsonDocument doc : docCaptor.getAllValues()) {
        if (doc.isDouble(counterKey)) {
          foundCounter = true;
          Assert.assertEquals(
              "Counter value should be 100.0", 100.0, doc.getDouble(counterKey).getValue(), 0.0);
        }
        if (doc.isDouble(gaugeKey)) {
          foundGauge = true;
          Assert.assertEquals(
              "Gauge value should be 50.0", 50.0, doc.getDouble(gaugeKey).getValue(), 0.0);
        }
      }

      Assert.assertTrue("At least one report should contain the counter", foundCounter);
      Assert.assertTrue("At least one report should contain the gauge", foundGauge);
    } finally {
      scheduledReporter.stop();
    }
  }

  @Test
  public void report_exceptionInOneCycle_continuesSubsequentCycles() throws Exception {
    var ftdc = mockFtdc();
    // Create a spy registry that will fail on the second call to getMeters()
    SimpleMeterRegistry realRegistry = new SimpleMeterRegistry();
    MeterRegistry meterRegistry = Mockito.spy(realRegistry);
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    // Set up captor to capture all calls throughout the test
    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);

    // Create normal metrics
    meterRegistry.counter("counter1").increment(10);
    meterRegistry.gauge("gauge1", 5.0);

    // Use a counter to track getMeters() calls and throw exception on the second call
    AtomicInteger callCount = new AtomicInteger(0);
    Mockito.doAnswer(
            invocation -> {
              int count = callCount.incrementAndGet();
              if (count == 2) {
                throw new RuntimeException("Simulated exception in report cycle");
              }
              // Call the real method on the original object
              return realRegistry.getMeters();
            })
        .when(meterRegistry)
        .getMeters();

    // First report should succeed - getMeters() returns normally
    reporter.report();
    verify(ftdc, times(1)).addSample(any(), anyLong());
    verify(meterRegistry, times(1)).getMeters();

    // Second report should catch the exception and not throw
    reporter.report();

    // Verify that getMeters() was called again (which threw the exception)
    verify(meterRegistry, times(2)).getMeters();

    // Verify that ftdc.addSample was NOT called for the failing report
    // (still only 1 call from the first successful report)
    verify(ftdc, times(1)).addSample(any(), anyLong());

    // Now verify that a subsequent report still works after the exception
    meterRegistry.counter("counter2").increment(20);
    reporter.report();
    verify(ftdc, times(2)).addSample(docCaptor.capture(), anyLong());
    verify(meterRegistry, times(3)).getMeters();

    BsonDocument firstDoc = docCaptor.getAllValues().get(0);
    BsonDocument secondDoc = docCaptor.getAllValues().get(1);

    String counter1Key = getFormattedKey("counter1", Optional.empty());
    String counter2Key = getFormattedKey("counter2", Optional.empty());

    // First report should have counter1
    assertWithMessage("First report should contain counter1")
        .that(firstDoc.isDouble(counter1Key))
        .isTrue();
    assertWithMessage("First report should contain counter1")
        .that(firstDoc.getDouble(counter1Key).getValue())
        .isEqualTo(10.0);

    // Second report should have both counters
    assertWithMessage("Second report should contain counter1")
        .that(secondDoc.isDouble(counter1Key))
        .isTrue();
    assertWithMessage("Second report should contain counter1")
        .that(secondDoc.getDouble(counter1Key).getValue())
        .isEqualTo(10.0);
    assertWithMessage("Second report should contain counter2")
        .that(secondDoc.isDouble(counter2Key))
        .isTrue();
    assertWithMessage("Second report should contain counter2")
        .that(secondDoc.getDouble(counter2Key).getValue())
        .isEqualTo(20.0);
  }

  @Test
  public void report_exceptionInMiddleGaugeSecondReport_otherGaugesContinue() throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    // Create three gauges: before, middle (will throw on second report), after
    meterRegistry.gauge("gauge_before", 10.0);

    // Create a gauge that will throw exception on the second report
    // Use AtomicInteger to track call count, increment in value() method
    AtomicInteger callCount = new AtomicInteger(0);
    Gauge.Builder<AtomicInteger> gaugeBuilder =
        Gauge.builder(
            "gauge_middle",
            callCount,
            count -> {
              int currentCount = count.incrementAndGet();
              if (currentCount == 2) { // Second call (1-indexed, so 2 means second)
                throw new RuntimeException("Simulated exception in gauge.value()");
              }
              return 20.0;
            });
    gaugeBuilder.register(meterRegistry);

    meterRegistry.gauge("gauge_after", 30.0);

    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    String gaugeBeforeKey = getFormattedKey("gauge_before", Optional.empty());
    String gaugeMiddleKey = getFormattedKey("gauge_middle", Optional.empty());
    String gaugeAfterKey = getFormattedKey("gauge_after", Optional.empty());

    // First report - all gauges should work
    // callCount starts at 0, first call increments to 1, so gauge_middle will return 20.0
    reporter.report();
    verify(ftdc, times(1)).addSample(docCaptor.capture(), anyLong());
    BsonDocument firstDoc = docCaptor.getValue();
    assertWithMessage("First report should contain gauge_before")
        .that(firstDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    assertWithMessage("First report should contain gauge_middle")
        .that(firstDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("First report should contain gauge_after")
        .that(firstDoc.isDouble(gaugeAfterKey))
        .isTrue();

    // Second report - middle gauge throws, but before and after should still work
    // callCount is now 1, second call increments to 2, so gauge_middle will throw
    // (Micrometer returns NaN when exception occurs)
    reporter.report();
    verify(ftdc, times(2)).addSample(docCaptor.capture(), anyLong());
    BsonDocument secondDoc = docCaptor.getValue();
    assertWithMessage("Second report should contain gauge_before")
        .that(secondDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    // gauge_middle should be present with NaN value (Micrometer returns NaN when exception occurs)
    assertWithMessage("Second report should contain gauge_middle")
        .that(secondDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("Second report gauge_middle should be NaN")
        .that(Double.isNaN(secondDoc.getDouble(gaugeMiddleKey).getValue()))
        .isTrue();
    assertWithMessage("Second report should contain gauge_after")
        .that(secondDoc.isDouble(gaugeAfterKey))
        .isTrue();

    // Third report - all gauges should work again
    // callCount is now 2, third call increments to 3, so gauge_middle will return 20.0
    reporter.report();
    verify(ftdc, times(3)).addSample(docCaptor.capture(), anyLong());
    BsonDocument thirdDoc = docCaptor.getValue();
    assertWithMessage("Third report should contain gauge_before")
        .that(thirdDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    assertWithMessage("Third report should contain gauge_middle")
        .that(thirdDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("Third report should contain gauge_after")
        .that(thirdDoc.isDouble(gaugeAfterKey))
        .isTrue();
  }

  @Test
  public void report_exceptionInMiddleGaugeEveryReport_otherGaugesContinue() throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc);

    // Create three gauges: before, middle (will always throw), after
    meterRegistry.gauge("gauge_before", 10.0);

    // Create a gauge that will always throw exception
    AtomicInteger throwCounter = new AtomicInteger(0);
    Gauge.Builder<AtomicInteger> gaugeBuilder =
        Gauge.builder(
            "gauge_middle",
            throwCounter,
            count -> {
              throw new RuntimeException("Simulated exception in gauge.value()");
            });
    gaugeBuilder.register(meterRegistry);

    meterRegistry.gauge("gauge_after", 30.0);

    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    String gaugeBeforeKey = getFormattedKey("gauge_before", Optional.empty());
    String gaugeMiddleKey = getFormattedKey("gauge_middle", Optional.empty());
    String gaugeAfterKey = getFormattedKey("gauge_after", Optional.empty());

    // First report - middle gauge throws, but before and after should work
    // Micrometer returns NaN when gauge throws exception
    reporter.report();
    verify(ftdc, times(1)).addSample(docCaptor.capture(), anyLong());
    BsonDocument firstDoc = docCaptor.getValue();
    assertWithMessage("First report should contain gauge_before")
        .that(firstDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    // gauge_middle should be present with NaN value (Micrometer returns NaN when exception occurs)
    assertWithMessage("First report should contain gauge_middle")
        .that(firstDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("First report gauge_middle should be NaN")
        .that(Double.isNaN(firstDoc.getDouble(gaugeMiddleKey).getValue()))
        .isTrue();
    assertWithMessage("First report should contain gauge_after")
        .that(firstDoc.isDouble(gaugeAfterKey))
        .isTrue();

    // Second report - middle gauge throws again, but before and after should work
    reporter.report();
    verify(ftdc, times(2)).addSample(docCaptor.capture(), anyLong());
    BsonDocument secondDoc = docCaptor.getValue();
    assertWithMessage("Second report should contain gauge_before")
        .that(secondDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    // gauge_middle should be present with NaN value
    assertWithMessage("Second report should contain gauge_middle")
        .that(secondDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("Second report gauge_middle should be NaN")
        .that(Double.isNaN(secondDoc.getDouble(gaugeMiddleKey).getValue()))
        .isTrue();
    assertWithMessage("Second report should contain gauge_after")
        .that(secondDoc.isDouble(gaugeAfterKey))
        .isTrue();

    // Third report - middle gauge throws again, but before and after should work
    reporter.report();
    verify(ftdc, times(3)).addSample(docCaptor.capture(), anyLong());
    BsonDocument thirdDoc = docCaptor.getValue();
    assertWithMessage("Third report should contain gauge_before")
        .that(thirdDoc.isDouble(gaugeBeforeKey))
        .isTrue();
    // gauge_middle should be present with NaN value
    assertWithMessage("Third report should contain gauge_middle")
        .that(thirdDoc.isDouble(gaugeMiddleKey))
        .isTrue();
    assertWithMessage("Third report gauge_middle should be NaN")
        .that(Double.isNaN(thirdDoc.getDouble(gaugeMiddleKey).getValue()))
        .isTrue();
    assertWithMessage("Third report should contain gauge_after")
        .that(thirdDoc.isDouble(gaugeAfterKey))
        .isTrue();
  }

  @Test
  public void scheduledReporter_exceptionInMiddleGaugeSecondReport_otherGaugesContinue()
      throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();

    // Create three gauges: before, middle (will throw on second report), after
    meterRegistry.gauge("gauge_before", 10.0);

    // Create a gauge that will throw exception on the second report
    // Use AtomicInteger to track call count, increment in value() method
    AtomicInteger callCount = new AtomicInteger(0);
    Gauge.Builder<AtomicInteger> gaugeBuilder =
        Gauge.builder(
            "gauge_middle",
            callCount,
            count -> {
              int currentCount = count.incrementAndGet();
              if (currentCount == 2) { // Second call (1-indexed, so 2 means second)
                throw new RuntimeException("Simulated exception in gauge.value()");
              }
              return 20.0;
            });
    gaugeBuilder.register(meterRegistry);

    meterRegistry.gauge("gauge_after", 30.0);

    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(meterRegistry);
    var scheduledReporter =
        FtdcScheduledReporter.create(
            meterRegistry,
            combinedRegistry,
            ftdc,
            false,
            FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    String gaugeBeforeKey = getFormattedKey("gauge_before", Optional.empty());
    String gaugeMiddleKey = getFormattedKey("gauge_middle", Optional.empty());
    String gaugeAfterKey = getFormattedKey("gauge_after", Optional.empty());

    try {
      scheduledReporter.start(10, TimeUnit.MILLISECONDS);

      // Wait for at least 3 reports
      verify(ftdc, timeout(500).atLeast(3)).addSample(docCaptor.capture(), anyLong());

      // Stop the reporter
      scheduledReporter.stop();

      // Get all captured documents
      List<BsonDocument> allDocs = docCaptor.getAllValues();
      assertThat(allDocs.size()).isAtLeast(3);

      // First report - all gauges should work
      BsonDocument firstDoc = allDocs.get(0);
      assertWithMessage("First report should contain gauge_before")
          .that(firstDoc.isDouble(gaugeBeforeKey))
          .isTrue();
      assertWithMessage("First report should contain gauge_middle")
          .that(firstDoc.isDouble(gaugeMiddleKey))
          .isTrue();
      assertWithMessage("First report should contain gauge_after")
          .that(firstDoc.isDouble(gaugeAfterKey))
          .isTrue();

      // Find the second report (where middle gauge throws)
      // Micrometer returns NaN when gauge throws exception
      BsonDocument secondDoc = allDocs.get(1);
      assertWithMessage("Second report should contain gauge_before")
          .that(secondDoc.isDouble(gaugeBeforeKey))
          .isTrue();
      // gauge_middle should be present with NaN value
      // (Micrometer returns NaN when exception occurs)
      assertWithMessage("Second report should contain gauge_middle")
          .that(secondDoc.isDouble(gaugeMiddleKey))
          .isTrue();
      assertWithMessage("Second report gauge_middle should be NaN")
          .that(Double.isNaN(secondDoc.getDouble(gaugeMiddleKey).getValue()))
          .isTrue();
      assertWithMessage("Second report should contain gauge_after")
          .that(secondDoc.isDouble(gaugeAfterKey))
          .isTrue();

      // Third report - all gauges should work again
      BsonDocument thirdDoc = allDocs.get(2);
      assertWithMessage("Third report should contain gauge_before")
          .that(thirdDoc.isDouble(gaugeBeforeKey))
          .isTrue();
      assertWithMessage("Third report should contain gauge_middle")
          .that(thirdDoc.isDouble(gaugeMiddleKey))
          .isTrue();
      assertWithMessage("Third report should contain gauge_after")
          .that(thirdDoc.isDouble(gaugeAfterKey))
          .isTrue();
    } finally {
      scheduledReporter.stop();
    }
  }

  @Test
  public void scheduledReporter_exceptionInMiddleGaugeEveryReport_otherGaugesContinue()
      throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();

    // Create three gauges: before, middle (will always throw), after
    meterRegistry.gauge("gauge_before", 10.0);

    // Create a gauge that will always throw exception
    AtomicInteger throwCounter = new AtomicInteger(0);
    Gauge.Builder<AtomicInteger> gaugeBuilder =
        Gauge.builder(
            "gauge_middle",
            throwCounter,
            count -> {
              throw new RuntimeException("Simulated exception in gauge.value()");
            });
    gaugeBuilder.register(meterRegistry);

    meterRegistry.gauge("gauge_after", 30.0);

    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(meterRegistry);
    var scheduledReporter =
        FtdcScheduledReporter.create(
            meterRegistry,
            combinedRegistry,
            ftdc,
            false,
            FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    ArgumentCaptor<BsonDocument> docCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    String gaugeBeforeKey = getFormattedKey("gauge_before", Optional.empty());
    String gaugeMiddleKey = getFormattedKey("gauge_middle", Optional.empty());
    String gaugeAfterKey = getFormattedKey("gauge_after", Optional.empty());

    try {
      scheduledReporter.start(10, TimeUnit.MILLISECONDS);

      // Wait for at least 3 reports
      verify(ftdc, timeout(500).atLeast(3)).addSample(docCaptor.capture(), anyLong());

      // Stop the reporter
      scheduledReporter.stop();

      // Get all captured documents
      List<BsonDocument> allDocs = docCaptor.getAllValues();
      assertThat(allDocs.size()).isAtLeast(3);

      // Verify that all reports contain before and after
      // gauge_middle should be present with NaN value
      // (Micrometer returns NaN when exception occurs)
      for (BsonDocument doc : allDocs) {
        assertWithMessage("Report should contain gauge_before")
            .that(doc.isDouble(gaugeBeforeKey))
            .isTrue();
        assertWithMessage("Report should contain gauge_middle")
            .that(doc.isDouble(gaugeMiddleKey))
            .isTrue();
        assertWithMessage("Report gauge_middle should be NaN")
            .that(Double.isNaN(doc.getDouble(gaugeMiddleKey).getValue()))
            .isTrue();
        assertWithMessage("Report should contain gauge_after")
            .that(doc.isDouble(gaugeAfterKey))
            .isTrue();
      }
    } finally {
      scheduledReporter.stop();
    }
  }

  @Test
  public void start_twoRegistriesProvided_executorMetricsInExecutorRegistryNotReportingRegistry()
      throws Exception {

    var ftdc = mockFtdc();
    // Create two separate registries
    SimpleMeterRegistry executorRegistry = new SimpleMeterRegistry();
    SimpleMeterRegistry reportingRegistry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(executorRegistry);

    // Use combinedRegistry for executor metrics so they go to executorRegistry
    var reporter =
        FtdcScheduledReporter.create(
            reportingRegistry,
            combinedRegistry,
            ftdc,
            true,
            FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    reporter.start(10, TimeUnit.MILLISECONDS);

    try {
      // Wait for at least one report cycle to ensure executor has run
      verify(ftdc, timeout(1000).atLeast(1)).addSample(any(), anyLong());

      // Verify executor metrics exist in executorRegistry
      assertWithMessage("Executor registry should contain executor.completed metric")
          .that(executorRegistry.find("ftdc-reporter.executor.completed").functionCounter())
          .isNotNull();

      assertWithMessage("Executor registry should contain executor.active metric")
          .that(executorRegistry.find("ftdc-reporter.executor.active").gauge())
          .isNotNull();

      assertWithMessage("Executor registry should contain executor.pool.size metric")
          .that(executorRegistry.find("ftdc-reporter.executor.pool.size").gauge())
          .isNotNull();

      // Verify executor metrics do NOT exist in reportingRegistry
      assertWithMessage("Reporting registry should NOT contain executor.completed metric")
          .that(reportingRegistry.find("ftdc-reporter.executor.completed").functionCounter())
          .isNull();

      assertWithMessage("Reporting registry should NOT contain executor.active metric")
          .that(reportingRegistry.find("ftdc-reporter.executor.active").gauge())
          .isNull();

      assertWithMessage("Reporting registry should NOT contain executor.pool.size metric")
          .that(reportingRegistry.find("ftdc-reporter.executor.pool.size").gauge())
          .isNull();

      // Verify that reportingTimer (ftdc-reporting-time) is registered to reportingRegistry
      assertWithMessage("Reporting registry should contain ftdc-reporting-time timer")
          .that(reportingRegistry.find("ftdc-reporting-time").timer())
          .isNotNull();

      // Verify that reportingTimer is NOT in executorRegistry
      assertWithMessage("Executor registry should NOT contain ftdc-reporting-time timer")
          .that(executorRegistry.find("ftdc-reporting-time").timer())
          .isNull();

    } finally {
      reporter.stop();
    }
  }

  @Test
  public void create_reporterThreadAttributionCounterIncreasesUnderLoad() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(registry);
    var reporter =
        FtdcScheduledReporter.create(
            registry, combinedRegistry, ftdc, false, FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);

    FunctionCounter allocated =
        registry
            .find("executor.thread.allocatedBytes")
            .tag("subsystem", "ftdc")
            .tag("name", "ftdc-reporter")
            .functionCounter();
    assertThat(allocated).isNotNull();

    reporter.start(10, TimeUnit.MILLISECONDS);
    try {
      verify(ftdc, timeout(20000).atLeast(1)).addSample(any(), anyLong());
      double before = allocated.count();
      // JVMs without per-thread allocation tracking report 0; skip rather than flake.
      if (before == 0.0) {
        return;
      }
      assertThat(pollUntil(() -> allocated.count() > before, Duration.ofSeconds(20))).isTrue();
    } finally {
      reporter.stop();
    }
  }

  @Test
  public void create_withMeterLimit_skipsSampleWhenExceeded() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry reportingRegistry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(reportingRegistry);
    var reporter =
        FtdcScheduledReporter.create(reportingRegistry, combinedRegistry, ftdc, false, 1);
    reporter.start(10, TimeUnit.MILLISECONDS);

    try {
      // Wait until the reporting timer proves that report cycles ran and were skipped.
      assertThat(
              pollUntil(
                  () ->
                      reportingRegistry
                          .find(FtdcScheduledReporter.FTDC_MONGOT_REPORTING_TIMER_NAME)
                          .timers()
                          .stream()
                          .anyMatch(t -> t.count() > 0),
                  Duration.ofSeconds(2)))
          .isTrue();
      verifyNoInteractions(ftdc);
    } finally {
      reporter.stop();
    }
  }

  private static boolean pollUntil(java.util.function.BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
  }

  @Test
  public void create_withMeterLimit_reportsSampleWhenBelowLimit() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry reportingRegistry = new SimpleMeterRegistry();
    CompositeMeterRegistry combinedRegistry = new CompositeMeterRegistry();
    combinedRegistry.add(reportingRegistry);

    var reporter =
        FtdcScheduledReporter.create(
            reportingRegistry,
            combinedRegistry,
            ftdc,
            false,
            FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT);
    reporter.start(10, TimeUnit.MILLISECONDS);

    try {
      verify(ftdc, timeout(1000).atLeast(1)).addSample(any(), anyLong());
    } finally {
      reporter.stop();
    }
  }

  @Test
  public void report_meterCountExceedsLimit_skipsSample() throws Exception {
    var ftdc = mockFtdc();
    var executorRegistry = new SimpleMeterRegistry();
    var reportingRegistry = new SimpleMeterRegistry();
    var reporter = new FtdcScheduledReporter.Reporter(executorRegistry, reportingRegistry, ftdc, 1);

    reporter.report();

    // Limit is 1 and the reporter always registers at least one meter (the reporting timer).
    verifyNoInteractions(ftdc);
  }

  @Test
  public void report_meterCountBelowLimit_reportsSample() throws Exception {
    var ftdc = mockFtdc();
    var executorRegistry = new SimpleMeterRegistry();
    var reportingRegistry = new SimpleMeterRegistry();
    // Use a limit well above any plausible internal meter count so the test is not
    // sensitive to how many meters the reporting timer creates internally.
    var reporter =
        new FtdcScheduledReporter.Reporter(executorRegistry, reportingRegistry, ftdc, 10_000);

    reportingRegistry.counter("counter1").increment();
    reportingRegistry.counter("counter2").increment();

    reporter.report();

    verify(ftdc).addSample(any(), anyLong());
  }

  @Test
  public void report_meterLimitDisabled_reportsRegardlessOfCount() throws Exception {
    var ftdc = mockFtdc();
    var meterRegistry = new SimpleMeterRegistry();
    var reporter =
        new FtdcScheduledReporter.Reporter(meterRegistry, meterRegistry, ftdc, Integer.MAX_VALUE);

    for (int i = 0; i < 100; i++) {
      meterRegistry.counter("counter_" + i).increment();
    }

    reporter.report();

    verify(ftdc).addSample(any(), anyLong());
  }

  @Test
  public void report_meterCountDropsBelowLimit_resumesReporting() throws Exception {
    var ftdc = mockFtdc();
    var executorRegistry = new SimpleMeterRegistry();
    var reportingRegistry = new SimpleMeterRegistry();

    // Measure baseline: register the same timer the Reporter creates internally to discover
    // how many meters SimpleMeterRegistry actually allocates for it.
    Timer.builder(FtdcScheduledReporter.FTDC_MONGOT_REPORTING_TIMER_NAME)
        .tags(FtdcScheduledReporter.FTDC_MONGOT_REPORTING_TIMER_TAGS)
        .publishPercentiles(0.5, 0.75, 0.9, 0.99)
        .register(reportingRegistry);
    int baseMeterCount = reportingRegistry.getMeters().size();
    reportingRegistry.clear();

    int limit = baseMeterCount + 1;
    var reporter =
        new FtdcScheduledReporter.Reporter(executorRegistry, reportingRegistry, ftdc, limit);

    // Add 2 counters — puts us at baseMeterCount + 2, which exceeds limit.
    var c1 = reportingRegistry.counter("a");
    var c2 = reportingRegistry.counter("b");
    c1.increment();
    c2.increment();

    assertThat(reportingRegistry.getMeters().size()).isGreaterThan(limit);
    reporter.report();
    verifyNoInteractions(ftdc);

    // Remove one counter to drop to baseMeterCount + 1, which equals the limit (not exceeded).
    reportingRegistry.remove(c2);

    assertThat(reportingRegistry.getMeters().size()).isAtMost(limit);
    reporter.report();
    verify(ftdc).addSample(any(), anyLong());
  }

  @Test
  public void report_assertionErrorDuringMetricCollection_throwsAssertionError() throws Exception {
    var ftdc = mockFtdc();
    // Create a spy registry that throws an AssertionError when getMeters() is called
    MeterRegistry failingRegistry = Mockito.spy(new SimpleMeterRegistry());
    Mockito.when(failingRegistry.getMeters())
        .thenThrow(new AssertionError("Simulated assertion error during getMeters()"));

    SimpleMeterRegistry executorRegistry = new SimpleMeterRegistry();

    // Create a reporter with the failing registry
    var failingReporter =
        new FtdcScheduledReporter.Reporter(executorRegistry, failingRegistry, ftdc);

    // Report should throw AssertionError since it's no longer caught
    Assert.assertThrows(AssertionError.class, failingReporter::report);

    // Verify that getMeters() was called (which threw the AssertionError)
    verify(failingRegistry, times(1)).getMeters();

    // Verify that ftdc.addSample was NOT called (because AssertionError was thrown)
    verifyNoInteractions(ftdc);
  }

  @Test
  public void report_assertionErrorDuringAddToDoc_throwsAssertionError() throws Exception {
    var ftdc = mockFtdc();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SimpleMeterRegistry executorRegistry = new SimpleMeterRegistry();

    // Create a mock meter that will trigger visitMeter (which calls Check.unreachable())
    // This will throw AssertionError
    Meter customMeter = Mockito.mock(Meter.class);
    Meter.Id mockId = Mockito.mock(Meter.Id.class);
    Mockito.when(customMeter.getId()).thenReturn(mockId);
    Mockito.when(mockId.getName()).thenReturn("custom.meter");
    Mockito.when(mockId.getTagsAsIterable()).thenReturn(Tags.empty());

    // Make the meter.use() call the last visitor (visitMeter), which will throw AssertionError
    Mockito.doAnswer(
            invocation -> {
              // The last argument (index 8) is the visitMeter consumer
              @SuppressWarnings("unchecked")
              java.util.function.Consumer<Meter> visitMeter =
                  (java.util.function.Consumer<Meter>) invocation.getArguments()[8];
              visitMeter.accept(customMeter);
              return null;
            })
        .when(customMeter)
        .use(any(), any(), any(), any(), any(), any(), any(), any(), any());

    // Create a registry that returns our custom meter
    MeterRegistry registryWithCustomMeter = Mockito.spy(meterRegistry);
    Mockito.when(registryWithCustomMeter.getMeters())
        .thenReturn(java.util.Collections.singletonList(customMeter));

    var reporter =
        new FtdcScheduledReporter.Reporter(executorRegistry, registryWithCustomMeter, ftdc);

    // Report should throw AssertionError since it's no longer caught
    Assert.assertThrows(AssertionError.class, reporter::report);

    // Verify that getMeters() was called
    verify(registryWithCustomMeter, times(1)).getMeters();

    // Verify that ftdc.addSample was NOT called (because AssertionError was thrown)
    verifyNoInteractions(ftdc);
  }

  private Ftdc mockFtdc() {
    return Mockito.mock(Ftdc.class);
  }

  private String getFormattedKey(String meterName, Optional<Tags> tags) {
    Tags toSerialize = tags.orElseGet(Tags::empty);
    return String.format(
        "%s,%s.%s", FtdcScheduledReporter.FTDC_MONGOT_METER_PREFIX, meterName, toSerialize);
  }
}
