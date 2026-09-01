package com.xgen.testing.mongot.metrics;

import com.google.common.collect.Sets;
import com.google.common.truth.Fact;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import com.xgen.mongot.util.Check;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Base {@link Subject} for asserting on meters of a given name and tag set. */
abstract class MeterSubject<T extends Meter> extends Subject {
  private final MeterRef meter;
  private final Tags tags;
  private final Class<T> meterType;

  MeterSubject(FailureMetadata metadata, MeterRef meter, Tags tags, Class<T> meterType) {
    super(metadata, meter);
    Tags factoryTags = meter.metricsFactory().factoryTags();
    // Tags.and prefers the argument on a key conflict, so a narrowing tag would silently override
    // the factory's value while the meter search still filters on the factory's -- nothing could
    // ever match. That is a malformed assertion rather than a failed one, hence the exception.
    // For simplicity, we do not allow redefining any tag keys, even if with equal value.
    var redefinedKeys =
        Sets.intersection(
            factoryTags.stream().map(Tag::getKey).collect(Collectors.toSet()),
            tags.stream().map(Tag::getKey).collect(Collectors.toSet()));
    Check.checkArg(
        redefinedKeys.isEmpty(),
        "narrowing tags (%s) shadow the factory tags (%s), redefined keys: %s",
        formatTags(tags),
        formatTags(factoryTags),
        redefinedKeys);
    this.meter = meter;
    this.tags = tags;
    this.meterType = meterType;
  }

  /** The meter this subject asserts on, for building a differently-narrowed sibling subject. */
  protected MeterRef meter() {
    return this.meter;
  }

  /** The tags a matching meter must carry: the factory's tags plus this subject's narrowing. */
  protected Tags expectedTags() {
    return this.meter.metricsFactory().factoryTags().and(this.tags);
  }

  protected T requireMeterWithExactTags() throws FailureException {
    var metersWithType = requireMetersWithNameAndType();
    var meterWithTags = filterExactTags(metersWithType);
    if (meterWithTags.isEmpty()) {
      failNoMatchingTags(metersWithType);
      throw new FailureException();
    }
    return meterWithTags.get();
  }

  protected Collection<T> requireAtLeastOneMeterContainingTags() throws FailureException {
    var metersWithType = requireMetersWithNameAndType();
    var metersWithTags = filterMatchingTags(metersWithType);
    if (metersWithTags.isEmpty()) {
      failNoMatchingTags(metersWithType);
      throw new FailureException();
    }
    return metersWithTags;
  }

  protected Collection<T> getMetersContainingTags() {
    var allMeters = getMetersWithName();
    var metersWithType = filterMeterType(allMeters);
    if (metersWithType.isEmpty() && !allMeters.isEmpty()) {
      failNoMatchingType(allMeters);
    }
    return filterMatchingTags(metersWithType);
  }

  protected Optional<T> getMeterWithExactTags() {
    var allMeters = getMetersWithName();
    var metersWithType = filterMeterType(allMeters);
    if (metersWithType.isEmpty() && !allMeters.isEmpty()) {
      failNoMatchingType(allMeters);
    }
    return filterExactTags(metersWithType);
  }

  /** Renders tags as {@code [key=value, ...]}, which reads better than {@link Tags#toString()}. */
  protected static String formatTags(Tags tags) {
    return tags.stream()
        .map(tag -> "%s=%s".formatted(tag.getKey(), tag.getValue()))
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private Collection<T> requireMetersWithNameAndType() throws FailureException {
    var allMeters = getMetersWithName();
    if (allMeters.isEmpty()) {
      failNoMatchingName();
      throw new FailureException();
    }
    var metersWithType = filterMeterType(allMeters);
    if (metersWithType.isEmpty()) {
      failNoMatchingType(allMeters);
      throw new FailureException();
    }
    return metersWithType;
  }

  private Collection<T> filterMeterType(Collection<Meter> meters) {
    return meters.stream().filter(this.meterType::isInstance).map(this.meterType::cast).toList();
  }

  private Optional<T> filterExactTags(Collection<T> meters) {
    Tags expectedTags = expectedTags();
    var matchingMeters =
        meters.stream().filter(c -> Tags.of(c.getId().getTags()).equals(expectedTags)).toList();
    if (matchingMeters.size() > 1) {
      // This should be unreachable, but we keep this check in order to find any potential bugs in
      // the meter subject implementations.
      throw new IllegalStateException("only one meter with a given set of tags can exist");
    }
    return matchingMeters.stream().findFirst();
  }

  private List<T> filterMatchingTags(Collection<T> meters) {
    List<Tag> expectedTags = expectedTags().stream().toList();
    return meters.stream()
        .filter(c -> new HashSet<>(c.getId().getTags()).containsAll(expectedTags))
        .toList();
  }

  protected void failNoMatchingName() {
    var existingMeterNames =
        this.meter.metricsFactory().getAllRegisteredMeters().stream()
            .map(meter -> meter.getId().getName())
            .distinct()
            .sorted()
            .toList();
    failWithoutActual(
        Fact.simpleFact("Meter not found for name"),
        Fact.fact("meter name", qualifiedMetricName()),
        Fact.fact("existing meter names", existingMeterNames));
  }

  private Collection<Meter> getMetersWithName() {
    try {
      return this.meter.metricsFactory().get(this.meter.metricName()).meters();
    } catch (MeterNotFoundException e) {
      return List.of();
    }
  }

  private void failNoMatchingType(Collection<Meter> metersWithName) {
    var existingTypes = metersWithName.stream().map(m -> m.getId().getType()).distinct().toList();
    failWithoutActual(
        Fact.simpleFact("Meter found for name but not with the given type"),
        Fact.fact("meter name", qualifiedMetricName()),
        Fact.fact("expected type", this.meterType.getSimpleName()),
        Fact.fact("existing types", existingTypes));
  }

  private void failNoMatchingTags(Collection<T> metersWithNameAndType) {
    var existingTagSets =
        metersWithNameAndType.stream().map(c -> formatTags(Tags.of(c.getId().getTags()))).toList();
    failWithoutActual(
        Fact.simpleFact("Meters found for name but not with the given tags"),
        Fact.fact("meter name", qualifiedMetricName()),
        Fact.fact("tags", formatTags(expectedTags())),
        Fact.fact("existing tag sets", existingTagSets));
  }

  /** The name the factory registers this metric under, so it compares with a meter's own name. */
  private String qualifiedMetricName() {
    return this.meter.metricsFactory().metricName(this.meter.metricName());
  }

  /**
   * Exception indicating that a failure was recorded.
   *
   * <p>In practice, a failure already throws, but we need a way to short-circuit methods that fail
   * to resolve required meters. Throwing this error both supports non-throwing strategies and
   * simplifies the code by avoiding returning a complex state.
   */
  static class FailureException extends Exception {}
}
