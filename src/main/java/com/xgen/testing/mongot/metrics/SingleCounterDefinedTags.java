package com.xgen.testing.mongot.metrics;

import io.micrometer.core.instrument.Tags;

/**
 * Asserts on the single counter matching a name and an exact tag set.
 *
 * <p>Obtained from {@link SingleCounterEmptyTags#withTags(Tags)}, which fixes the tag set. This
 * type deliberately has no narrowing method, so tags can never be defined twice.
 */
public interface SingleCounterDefinedTags {

  /**
   * Asserts that the matching counter has not been incremented.
   *
   * <p>If a counter was never registered but another type of meter was registered under the same
   * name, this assertion fails.
   *
   * <p>If a counter was never registered, it is also considered never incremented. To require the
   * matching counter to be registered, use {@link #hasCount(double)} with 0.
   */
  void isNeverIncremented();

  /**
   * Asserts the expected count of the metric.
   *
   * <p>If a counter does not exist but another type of meter was registered under the same name,
   * this assertion fails.
   */
  void hasCount(double expected);
}
