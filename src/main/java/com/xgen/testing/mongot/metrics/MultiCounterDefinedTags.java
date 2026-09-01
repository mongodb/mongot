package com.xgen.testing.mongot.metrics;

import io.micrometer.core.instrument.Tags;

/**
 * Asserts on the counters matching a name and carrying at least a given tag set.
 *
 * <p>Obtained from {@link MultiCounterEmptyTags#containingTags(Tags)}, which fixes the tag set.
 * This type deliberately has no narrowing method, so tags can never be defined twice.
 */
public interface MultiCounterDefinedTags {

  /**
   * Asserts that none of the matching counters have been incremented.
   *
   * <p>If a counter was never registered but another type of meter was registered under the same
   * name, this assertion fails.
   *
   * <p>If a counter was never registered, it is also considered never incremented. To require some
   * matching counters to be registered, use {@link #haveCumulativeCount(double)} with 0.
   */
  void areNeverIncremented();

  /**
   * Asserts the expected sum of the counts of every matching counter.
   *
   * <p>If a counter was never registered but another type of meter was registered under the same
   * name, this assertion fails.
   */
  void haveCumulativeCount(double expected);
}
