package com.xgen.testing.mongot.metrics;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;

/**
 * The single implementation behind {@link SingleCounterEmptyTags} and {@link
 * SingleCounterDefinedTags}.
 *
 * <p>Package-private on purpose: callers only ever see the two interfaces, so the narrowing method
 * is unreachable once the tags have been defined, even by casting.
 */
final class SingleCounterSubject extends MeterSubject<Counter> implements SingleCounterEmptyTags {

  private SingleCounterSubject(FailureMetadata metadata, MeterRef counter, Tags tags) {
    super(metadata, counter, tags, Counter.class);
  }

  static Subject.Factory<SingleCounterSubject, MeterRef> factory() {
    return factory(Tags.empty());
  }

  static Subject.Factory<SingleCounterSubject, MeterRef> factory(Tags tags) {
    return (metadata, actual) -> new SingleCounterSubject(metadata, actual, tags);
  }

  @Override
  public SingleCounterDefinedTags withTags(Tag... tags) {
    return withTags(Tags.of(tags));
  }

  @Override
  public SingleCounterDefinedTags withTags(Tags tags) {
    return check("withTags(%s)", formatTags(tags)).about(factory(tags)).that(meter());
  }

  @Override
  public void isNeverIncremented() {
    Optional<Counter> counter = getMeterWithExactTags();
    if (counter.isEmpty()) {
      return;
    }
    check("isNeverIncremented()").that(counter.get().count()).isEqualTo(0.0);
  }

  @Override
  public void hasCount(double expected) {
    Counter counter;
    try {
      counter = requireMeterWithExactTags();
    } catch (FailureException e) {
      return;
    }
    check("count()").that(counter.count()).isEqualTo(expected);
  }
}
