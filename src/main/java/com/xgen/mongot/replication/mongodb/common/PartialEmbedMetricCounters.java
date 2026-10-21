package com.xgen.mongot.replication.mongodb.common;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.utils.MaterializedViewArrayElementType;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;

/**
 * Counters for the partial-embed optimization (CLOUDP-412617), incremented from the per-batch
 * tallies on {@link DocumentEventBatch}: {@code partialEmbedApplied} for updates kept partial,
 * {@code filterOnlyPartialApplied} for updates that touched only filter fields (the non-view analog
 * of {@code viewFilterOnlyPartialApplied}), and {@code partialEmbedFallback{reason}} for full
 * re-indexes tagged by {@link PartialEmbedFallbackReason}.
 */
public class PartialEmbedMetricCounters {

  private final Counter appliedCounter;
  private final Counter filterOnlyPartialAppliedCounter;
  private final ImmutableMap<PartialEmbedFallbackReason, Counter> fallbackCounters;
  private final ImmutableMap<MaterializedViewArrayElementType, Counter>
      duplicatedArrayElementCounters;

  /** Creates metric counters for the supplied base tags. */
  public PartialEmbedMetricCounters(MetricsFactory metricsFactory, Tags baseTags) {
    this.appliedCounter = metricsFactory.counter("partialEmbedApplied", baseTags);
    this.filterOnlyPartialAppliedCounter =
        metricsFactory.counter("filterOnlyPartialApplied", baseTags);
    EnumMap<PartialEmbedFallbackReason, Counter> byReason =
        new EnumMap<>(PartialEmbedFallbackReason.class);
    for (PartialEmbedFallbackReason reason : PartialEmbedFallbackReason.values()) {
      byReason.put(
          reason,
          metricsFactory.counter(
              "partialEmbedFallback",
              baseTags.and("reason", Enums.convertNameTo(CaseFormat.LOWER_UNDERSCORE, reason))));
    }
    this.fallbackCounters = ImmutableMap.copyOf(byReason);
    EnumMap<MaterializedViewArrayElementType, Counter> byElement =
        new EnumMap<>(MaterializedViewArrayElementType.class);
    for (MaterializedViewArrayElementType element : MaterializedViewArrayElementType.values()) {
      byElement.put(
          element,
          metricsFactory.counter(
              MaterializedViewDuplicatedArrayElementMetricCounters.METRIC_NAME,
              baseTags
                  .and(
                      MaterializedViewDuplicatedArrayElementMetricCounters.ELEMENT_TYPE_TAG,
                      element.tagValue())
                  .and(
                      MaterializedViewDuplicatedArrayElementMetricCounters.SAMPLE_MODULUS_TAG,
                      "1")));
    }
    this.duplicatedArrayElementCounters = ImmutableMap.copyOf(byElement);
  }

  /** Adds one batch's partial-embed tallies to the counters. */
  public void recordBatch(DocumentEventBatch batch) {
    if (batch.partialEmbedApplied > 0) {
      this.appliedCounter.increment(batch.partialEmbedApplied);
    }
    if (batch.filterOnlyPartialApplied > 0) {
      this.filterOnlyPartialAppliedCounter.increment(batch.filterOnlyPartialApplied);
    }
    batch.partialEmbedFallbacks.forEach(
        (reason, count) -> this.fallbackCounters.get(reason).increment(count));
    batch.materializedViewWritesWithDuplicatedArrayElements.forEach(
        (element, count) -> this.duplicatedArrayElementCounters.get(element).increment(count));
  }
}
