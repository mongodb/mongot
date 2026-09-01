package com.xgen.testing.mongot.metrics;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.metrics.CounterSubjects.assertThatCounter;
import static org.junit.Assert.assertThrows;

import com.google.common.truth.StringSubject;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

/**
 * Tests the assertions reachable from {@link CounterSubjects#assertThatCounter}, both that they
 * pass on a matching counter and that their failure messages name the counter and tags that did not
 * match. Covers {@link SingleCounterEmptyTags} and the {@link SingleCounterDefinedTags} it narrows
 * to, which share one implementation.
 */
public class SingleCounterSubjectTest {

  private static final Tag STATUS_OK = Tag.of("status", "OK");
  private static final Tag STATUS_FAILED = Tag.of("status", "FAILED");
  private static final Tag REASON_TIMEOUT = Tag.of("reason", "timeout");

  private static final Tag INDEX_FOO = Tag.of("index", "foo");

  private final MetricsFactory metricsFactory =
      new MetricsFactory("testNamespace", new SimpleMeterRegistry());

  private final MetricsFactory taggedFactory =
      new MetricsFactory("testNamespace", new SimpleMeterRegistry(), INDEX_FOO);

  @Test
  public void assertThatCounter_noTagsGiven_assertsTheUntaggedCounter() {
    this.metricsFactory.counter("uploads").increment(3);

    assertThatCounter(this.metricsFactory, "uploads").hasCount(3);
  }

  @Test
  public void hasCount_countDiffers_failsReportingBothCounts() {
    this.metricsFactory.counter("uploads").increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(2));

    assertThatFailureMessage(failure).contains("expected: 2.0");
    assertThatFailureMessage(failure).contains("but was: 1.0");
  }

  @Test
  public void hasCount_counterNeverRegistered_failsReportingMissingMeterName() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(1));

    assertThatFailureMessage(failure).contains("Meter not found for name");
    assertThatFailureMessage(failure).contains("meter name: testNamespace.uploads");
  }

  @Test
  public void hasCount_counterNeverRegistered_failsListingExistingMeterNames() {
    this.metricsFactory.counter("uploadCycles", Tags.of(STATUS_OK));
    this.metricsFactory.counter("uploadCycles", Tags.of(STATUS_FAILED));
    this.metricsFactory.counter("downloads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(1));

    assertThatFailureMessage(failure)
        .contains("existing meter names: [testNamespace.downloads, testNamespace.uploadCycles]");
  }

  @Test
  public void hasCount_onlyTaggedCountersRegistered_failsListingExistingTagSets() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(1));

    assertThatFailureMessage(failure).contains("Meters found for name but not with the given tags");
    assertThatFailureMessage(failure).contains("meter name: testNamespace.uploads");
    assertThatFailureMessage(failure).contains("existing tag sets");
    assertThatFailureMessage(failure).contains("status=OK");
  }

  @Test
  public void hasCount_counterOfAnotherFactory_isNotVisible() {
    MetricsFactory otherFactory = new MetricsFactory("otherNamespace", new SimpleMeterRegistry());
    otherFactory.counter("uploads").increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(1));

    assertThatFailureMessage(failure).contains("Meter not found for name");
    assertThatFailureMessage(failure).contains("existing meter names: []");
  }

  @Test
  public void withTags_counterCarryingExactlyThoseTags_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment(2);

    assertThatCounter(this.metricsFactory, "uploads")
        .withTags(STATUS_OK, REASON_TIMEOUT)
        .hasCount(2);
  }

  @Test
  public void withTags_counterCarryingAdditionalTags_failsListingExistingTagSets() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_OK).hasCount(1));

    assertThatFailureMessage(failure).contains("Meters found for name but not with the given tags");
    assertThatFailureMessage(failure).contains("reason=timeout");
  }

  @Test
  public void withTags_onlyOtherTagSetsIncremented_readsZeroRatherThanSumming() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(5);
    this.metricsFactory.counter("uploads", Tags.of(STATUS_FAILED));

    assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_FAILED).hasCount(0);
  }

  @Test
  public void hasCount_failure_namesTheCounterUnderAssertion() {
    this.metricsFactory.counter("uploads").increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(2));

    assertThatFailureMessage(failure).contains("singleCounter was: \"testNamespace.uploads\"");
    assertThatFailureMessage(failure).contains("value of: singleCounter.count()");
  }

  @Test
  public void withTags_failure_reportsTheNarrowingInTheValueOfPath() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_OK).hasCount(2));

    assertThatFailureMessage(failure)
        .contains("value of: singleCounter.withTags([status=OK]).count()");
  }

  @Test
  public void assertThatCounter_factoryWithTags_matchesWithoutRespellingFactoryTags() {
    this.taggedFactory.counter("uploads").increment(3);

    assertThatCounter(this.taggedFactory, "uploads").hasCount(3);
  }

  @Test
  public void withTags_factoryWithTags_matchesFactoryTagsPlusGivenTags() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);

    assertThatCounter(this.taggedFactory, "uploads").withTags(STATUS_OK).hasCount(2);
  }

  @Test
  public void withTags_factoryWithTags_counterCarryingAdditionalTags_fails() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.taggedFactory, "uploads").withTags(STATUS_OK).hasCount(1));

    assertThatFailureMessage(failure).contains("Meters found for name but not with the given tags");
  }

  @Test
  public void hasCount_factoryWithTags_failureReportsTheFactoryTags() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.taggedFactory, "uploads").withTags(STATUS_OK).hasCount(1));

    assertThatFailureMessage(failure).contains("tags: [index=foo, status=OK]");
  }

  @Test
  public void withTags_tagKeyAlreadySetByFactory_throws() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                assertThatCounter(this.taggedFactory, "uploads").withTags(Tag.of("index", "bar")));

    assertThat(failure).hasMessageThat().contains("[index=bar]");
    assertThat(failure).hasMessageThat().contains("[index=foo]");
  }

  @Test
  public void withTags_tagKeyAlreadySetByFactoryToTheSameValue_throws() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> assertThatCounter(this.taggedFactory, "uploads").withTags(INDEX_FOO));

    assertThat(failure).hasMessageThat().contains("index");
  }

  @Test
  public void hasCount_siblingFactorySharingTheRegistry_isNotVisible() {
    SimpleMeterRegistry sharedRegistry = new SimpleMeterRegistry();
    MetricsFactory fooFactory = new MetricsFactory("testNamespace", sharedRegistry, INDEX_FOO);
    MetricsFactory barFactory =
        new MetricsFactory("testNamespace", sharedRegistry, Tag.of("index", "bar"));
    fooFactory.counter("uploads").increment(2);
    barFactory.counter("uploads").increment(7);

    assertThatCounter(fooFactory, "uploads").hasCount(2);
    assertThatCounter(barFactory, "uploads").hasCount(7);
  }

  @Test
  public void isNeverIncremented_counterNeverRegistered_passes() {
    assertThatCounter(this.metricsFactory, "uploads").isNeverIncremented();
    assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_OK).isNeverIncremented();
  }

  @Test
  public void isNeverIncremented_registeredButNotIncremented_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK));

    assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_OK).isNeverIncremented();
  }

  @Test
  public void isNeverIncremented_onlyAnotherTagSetIncremented_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(5);

    assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_FAILED).isNeverIncremented();
  }

  @Test
  public void isNeverIncremented_counterIncremented_failsReportingTheCount() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(3);

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounter(this.metricsFactory, "uploads")
                    .withTags(STATUS_OK)
                    .isNeverIncremented());

    assertThatFailureMessage(failure).contains("expected: 0.0");
    assertThatFailureMessage(failure).contains("but was: 3.0");
  }

  @Test
  public void isNeverIncremented_failure_reportsTheNarrowingInTheValueOfPath() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounter(this.metricsFactory, "uploads")
                    .withTags(STATUS_OK)
                    .isNeverIncremented());

    assertThatFailureMessage(failure)
        .contains("value of: singleCounter.withTags([status=OK]).isNeverIncremented()");
  }

  @Test
  public void hasCount_meterRegisteredWithAnotherType_failsReportingTheType() {
    this.metricsFactory.numGauge("uploads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").hasCount(1));

    assertThatFailureMessage(failure).contains("Meter found for name but not with the given type");
    assertThatFailureMessage(failure).contains("meter name: testNamespace.uploads");
    assertThatFailureMessage(failure).contains("expected type: Counter");
    assertThatFailureMessage(failure).contains("existing types: [GAUGE]");
    assertThatFailureMessage(failure).doesNotContain("Meter not found for name");
  }

  @Test
  public void isNeverIncremented_meterRegisteredWithAnotherType_fails() {
    this.metricsFactory.numGauge("uploads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounter(this.metricsFactory, "uploads").isNeverIncremented());

    assertThatFailureMessage(failure).contains("Meter found for name but not with the given type");
    assertThatFailureMessage(failure).contains("existing types: [GAUGE]");
  }

  @Test
  public void hasCount_counterAndGaugeShareTheName_assertsTheCounter() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);
    this.metricsFactory.numGauge("uploads", Tags.of(STATUS_FAILED)).set(7);

    assertThatCounter(this.metricsFactory, "uploads").withTags(STATUS_OK).hasCount(2);
  }

  /**
   * Truth pads the fact keys of a failure message to a common width, so assert on a normalized copy
   * rather than on the incidental alignment.
   */
  private static StringSubject assertThatFailureMessage(AssertionError failure) {
    return assertThat(failure.getMessage().replaceAll("\\s*:\\s*", ": "));
  }
}
