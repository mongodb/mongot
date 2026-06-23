package com.xgen.mongot.replication.mongodb.common;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;

/**
 * Counters for the partial-embed optimization (CLOUDP-412617), incremented from the per-batch
 * tallies on {@link DocumentEventBatch}: {@code partialEmbedApplied} for updates kept partial, and
 * {@code partialEmbedFallback{reason}} for full re-indexes tagged by {@link
 * PartialEmbedFallbackReason}.
 */
public class PartialEmbedMetricCounters {

  private final Counter appliedCounter;
  private final ImmutableMap<PartialEmbedFallbackReason, Counter> fallbackCounters;

  public PartialEmbedMetricCounters(MetricsFactory metricsFactory, Tags baseTags) {
    this.appliedCounter = metricsFactory.counter("partialEmbedApplied", baseTags);
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
  }

  /** Adds one batch's partial-embed tallies to the counters. */
  public void recordBatch(DocumentEventBatch batch) {
    if (batch.partialEmbedApplied > 0) {
      this.appliedCounter.increment(batch.partialEmbedApplied);
    }
    batch.partialEmbedFallbacks.forEach(
        (reason, count) -> this.fallbackCounters.get(reason).increment(count));
  }
}
