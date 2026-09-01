package com.xgen.testing.mongot.metrics;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.metrics.CounterSubjects.assertThatCounters;
import static org.junit.Assert.assertThrows;

import com.google.common.truth.StringSubject;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

/**
 * Tests the assertions reachable from {@link CounterSubjects#assertThatCounters}, both that they
 * sum the counters they should and that their failure messages name the counter and tags that did
 * not match. Covers {@link MultiCounterEmptyTags} and the {@link MultiCounterDefinedTags} it
 * narrows to, which share one implementation.
 */
public class MultiCounterSubjectTest {

  private static final Tag STATUS_OK = Tag.of("status", "OK");
  private static final Tag STATUS_FAILED = Tag.of("status", "FAILED");
  private static final Tag REASON_TIMEOUT = Tag.of("reason", "timeout");
  private static final Tag REASON_OTHER = Tag.of("reason", "other");

  private static final Tag INDEX_FOO = Tag.of("index", "foo");

  private final MetricsFactory metricsFactory =
      new MetricsFactory("testNamespace", new SimpleMeterRegistry());

  private final MetricsFactory taggedFactory =
      new MetricsFactory("testNamespace", new SimpleMeterRegistry(), INDEX_FOO);

  @Test
  public void assertThatCounters_noTagsGiven_sumsEveryTagSet() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);
    this.metricsFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);
    this.metricsFactory.counter("uploads").increment();

    assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(10);
  }

  @Test
  public void containingTags_severalTagSetsShareTheGivenTag_sumsTheirCounts() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment(2);
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_OTHER)).increment();
    this.metricsFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);

    assertThatCounters(this.metricsFactory, "uploads")
        .containingTags(STATUS_OK)
        .haveCumulativeCount(3);
  }

  @Test
  public void haveCumulativeCount_counterNeverRegistered_failsListingExistingMeterNames() {
    this.metricsFactory.counter("uploadCycles", Tags.of(STATUS_OK));
    this.metricsFactory.counter("uploadCycles", Tags.of(STATUS_FAILED));
    this.metricsFactory.counter("downloads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(1));

    assertThatFailureMessage(failure).contains("meter name: testNamespace.uploads");
    assertThatFailureMessage(failure)
        .contains("existing meter names: [testNamespace.downloads, testNamespace.uploadCycles]");
  }

  @Test
  public void haveCumulativeCount_counterNeverRegisteredAndZeroExpected_failsReportingName() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(0));

    assertThatFailureMessage(failure).contains("Meter not found for name");
  }

  @Test
  public void
      haveCumulativeCount_narrowedCounterNeverRegisteredAndZeroExpected_failsReportingName() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_OK)
                    .haveCumulativeCount(0));

    assertThatFailureMessage(failure).contains("Meter not found for name");
  }

  @Test
  public void haveCumulativeCount_counterNeverRegisteredAndPositiveExpected_failsReportingName() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(1));

    assertThatFailureMessage(failure).contains("Meter not found for name");
  }

  @Test
  public void haveCumulativeCount_noTagSetMatchesAndZeroExpected_failsListingExistingTagSets() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_FAILED)
                    .haveCumulativeCount(0));

    assertThatFailureMessage(failure).contains("Meters found for name but not with the given tags");
    assertThatFailureMessage(failure).contains("status=OK");
  }

  @Test
  public void haveCumulativeCount_noTagSetMatchesAndPositiveExpected_failsListingExistingTagSets() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_FAILED)
                    .haveCumulativeCount(1));

    assertThatFailureMessage(failure).contains("Meters found for name but not with the given tags");
    assertThatFailureMessage(failure).contains("status=FAILED");
  }

  @Test
  public void haveCumulativeCount_failure_reportBothCounts() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(2));

    assertThatFailureMessage(failure).contains("expected: 2.0");
    assertThatFailureMessage(failure).contains("but was: 1.0");
  }

  @Test
  public void haveCumulativeCount_failure_namesTheCounterUnderAssertion() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(2));

    assertThatFailureMessage(failure).contains("multiCounter was: \"testNamespace.uploads\"");
    assertThatFailureMessage(failure).contains("value of: multiCounter.cumulativeCount()");
  }

  @Test
  public void containingTags_failure_reportsTheNarrowingInTheValueOfPath() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_OK)
                    .haveCumulativeCount(2));

    assertThatFailureMessage(failure)
        .contains("value of: multiCounter.containingTags([status=OK]).cumulativeCount()");
  }

  @Test
  public void haveCumulativeCount_factoryWithTags_sumsAcrossTagSets() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);
    this.taggedFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);

    assertThatCounters(this.taggedFactory, "uploads").haveCumulativeCount(9);
  }

  @Test
  public void containingTags_factoryWithTags_matchesFactoryTagsPlusGivenTags() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment(2);
    this.taggedFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);

    assertThatCounters(this.taggedFactory, "uploads")
        .containingTags(STATUS_OK)
        .haveCumulativeCount(2);
  }

  @Test
  public void haveCumulativeCount_factoryWithTags_failureReportsTheFactoryTags() {
    this.taggedFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.taggedFactory, "uploads")
                    .containingTags(STATUS_FAILED)
                    .haveCumulativeCount(1));

    assertThatFailureMessage(failure).contains("tags: [index=foo, status=FAILED]");
  }

  @Test
  public void areNeverIncremented_counterNeverRegistered_passes() {
    assertThatCounters(this.metricsFactory, "uploads").areNeverIncremented();
    assertThatCounters(this.metricsFactory, "uploads")
        .containingTags(STATUS_OK)
        .areNeverIncremented();
  }

  @Test
  public void areNeverIncremented_noTagSetMatches_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    assertThatCounters(this.metricsFactory, "uploads")
        .containingTags(STATUS_FAILED)
        .areNeverIncremented();
  }

  @Test
  public void areNeverIncremented_registeredButNotIncremented_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK));

    assertThatCounters(this.metricsFactory, "uploads").areNeverIncremented();
  }

  @Test
  public void areNeverIncremented_onlyNonMatchingTagSetIncremented_passes() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(5);
    this.metricsFactory.counter("uploads", Tags.of(STATUS_FAILED));

    assertThatCounters(this.metricsFactory, "uploads")
        .containingTags(STATUS_FAILED)
        .areNeverIncremented();
  }

  @Test
  public void areNeverIncremented_matchingCounterIncremented_failsReportingTheCount() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(3);

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").areNeverIncremented());

    assertThatFailureMessage(failure).contains("expected: 0.0");
    assertThatFailureMessage(failure).contains("but was: 3.0");
  }

  @Test
  public void areNeverIncremented_severalMatchingTagSetsIncremented_failsReportingTheSum() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_TIMEOUT)).increment(2);
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK, REASON_OTHER)).increment();
    this.metricsFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_OK)
                    .areNeverIncremented());

    assertThatFailureMessage(failure).contains("but was: 3.0");
  }

  @Test
  public void areNeverIncremented_failure_reportsTheNarrowingInTheValueOfPath() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment();

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                assertThatCounters(this.metricsFactory, "uploads")
                    .containingTags(STATUS_OK)
                    .areNeverIncremented());

    assertThatFailureMessage(failure)
        .contains("value of: multiCounter.containingTags([status=OK]).areNeverIncremented()");
  }

  @Test
  public void containingTags_tagKeyAlreadySetByFactory_throws() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                assertThatCounters(this.taggedFactory, "uploads")
                    .containingTags(Tag.of("index", "bar")));

    assertThat(failure).hasMessageThat().contains("[index=bar]");
    assertThat(failure).hasMessageThat().contains("[index=foo]");
  }

  @Test
  public void containingTags_tagKeyAlreadySetByFactoryToTheSameValue_throws() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> assertThatCounters(this.taggedFactory, "uploads").containingTags(INDEX_FOO));

    assertThat(failure).hasMessageThat().contains("index");
  }

  @Test
  public void haveCumulativeCount_siblingFactorySharingTheRegistry_isNotVisible() {
    SimpleMeterRegistry sharedRegistry = new SimpleMeterRegistry();
    MetricsFactory fooFactory = new MetricsFactory("testNamespace", sharedRegistry, INDEX_FOO);
    MetricsFactory barFactory =
        new MetricsFactory("testNamespace", sharedRegistry, Tag.of("index", "bar"));
    fooFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);
    barFactory.counter("uploads", Tags.of(STATUS_FAILED)).increment(7);

    assertThatCounters(fooFactory, "uploads").haveCumulativeCount(2);
    assertThatCounters(barFactory, "uploads").haveCumulativeCount(7);
  }

  @Test
  public void haveCumulativeCount_meterRegisteredWithAnotherType_failsReportingTheType() {
    this.metricsFactory.numGauge("uploads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(1));

    assertThatFailureMessage(failure).contains("Meter found for name but not with the given type");
    assertThatFailureMessage(failure).contains("meter name: testNamespace.uploads");
    assertThatFailureMessage(failure).contains("expected type: Counter");
    assertThatFailureMessage(failure).contains("existing types: [GAUGE]");
    assertThatFailureMessage(failure).doesNotContain("Meter not found for name");
  }

  @Test
  public void areNeverIncremented_meterRegisteredWithAnotherType_fails() {
    this.metricsFactory.numGauge("uploads");

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> assertThatCounters(this.metricsFactory, "uploads").areNeverIncremented());

    assertThatFailureMessage(failure).contains("Meter found for name but not with the given type");
    assertThatFailureMessage(failure).contains("existing types: [GAUGE]");
  }

  @Test
  public void haveCumulativeCount_counterAndGaugeShareTheName_sumsOnlyCounters() {
    this.metricsFactory.counter("uploads", Tags.of(STATUS_OK)).increment(2);
    this.metricsFactory.numGauge("uploads", Tags.of(STATUS_FAILED)).set(7);

    assertThatCounters(this.metricsFactory, "uploads").haveCumulativeCount(2);
  }

  /**
   * Truth pads the fact keys of a failure message to a common width, so assert on a normalized copy
   * rather than on the incidental alignment.
   */
  private static StringSubject assertThatFailureMessage(AssertionError failure) {
    return assertThat(failure.getMessage().replaceAll("\\s*:\\s*", ": "));
  }
}
