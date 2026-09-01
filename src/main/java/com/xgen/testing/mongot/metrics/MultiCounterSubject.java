package com.xgen.testing.mongot.metrics;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Collection;

/**
 * The single implementation behind {@link MultiCounterEmptyTags} and {@link
 * MultiCounterDefinedTags}.
 *
 * <p>Package-private on purpose: callers only ever see the two interfaces, so the narrowing method
 * is unreachable once the tags have been defined, even by casting.
 */
final class MultiCounterSubject extends MeterSubject<Counter> implements MultiCounterEmptyTags {

  private MultiCounterSubject(FailureMetadata metadata, MeterRef counter, Tags tags) {
    super(metadata, counter, tags, Counter.class);
  }

  static Subject.Factory<MultiCounterSubject, MeterRef> factory() {
    return factory(Tags.empty());
  }

  static Subject.Factory<MultiCounterSubject, MeterRef> factory(Tags tags) {
    return (metadata, actual) -> new MultiCounterSubject(metadata, actual, tags);
  }

  @Override
  public MultiCounterDefinedTags containingTags(Tag... tags) {
    return containingTags(Tags.of(tags));
  }

  @Override
  public MultiCounterDefinedTags containingTags(Tags tags) {
    return check("containingTags(%s)", formatTags(tags)).about(factory(tags)).that(meter());
  }

  @Override
  public void areNeverIncremented() {
    Collection<Counter> counters = getMetersContainingTags();
    check("areNeverIncremented()").that(sum(counters)).isEqualTo(0.0);
  }

  @Override
  public void haveCumulativeCount(double expected) {
    Collection<Counter> counters;
    try {
      counters = requireAtLeastOneMeterContainingTags();
    } catch (FailureException e) {
      return;
    }
    check("cumulativeCount()").that(sum(counters)).isEqualTo(expected);
  }

  private double sum(Collection<Counter> counters) {
    return counters.stream().mapToDouble(Counter::count).sum();
  }
}
