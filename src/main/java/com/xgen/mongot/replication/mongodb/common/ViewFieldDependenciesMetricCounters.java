package com.xgen.mongot.replication.mongodb.common;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewDerivationFallbackReason;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewFullProcessingReason;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;

/**
 * Counters for view-based auto-embed decisions, fed from the per-batch tallies on {@link
 * DocumentEventBatch}: {@code viewFieldDependenciesDerivationFallback{reason}} (once per index when
 * derivation can't bound dependencies), {@code viewFieldDependenciesDecision{result, reason}} (per
 * selected UPDATE write route), and {@code viewFilterOnlyPartialApplied} (view filter-only
 * partials, the view analog of {@code partialEmbedApplied}).
 */
public class ViewFieldDependenciesMetricCounters {

  private final ImmutableMap<ViewDerivationFallbackReason, Counter> derivationFallbackCounters;
  private final ImmutableMap<ViewFullProcessingReason, Counter> decisionCounters;
  private final Counter filterOnlyPartialAppliedCounter;

  /** Pre-creates one counter per reason (and result) so {@link #recordBatch} is a lookup. */
  public ViewFieldDependenciesMetricCounters(MetricsFactory metricsFactory, Tags baseTags) {
    EnumMap<ViewDerivationFallbackReason, Counter> byDerivation =
        new EnumMap<>(ViewDerivationFallbackReason.class);
    for (ViewDerivationFallbackReason reason : ViewDerivationFallbackReason.values()) {
      byDerivation.put(
          reason,
          metricsFactory.counter(
              "viewFieldDependenciesDerivationFallback",
              baseTags.and("reason", Enums.convertNameTo(CaseFormat.LOWER_UNDERSCORE, reason))));
    }
    this.derivationFallbackCounters = ImmutableMap.copyOf(byDerivation);

    EnumMap<ViewFullProcessingReason, Counter> byDecision =
        new EnumMap<>(ViewFullProcessingReason.class);
    for (ViewFullProcessingReason reason : ViewFullProcessingReason.values()) {
      byDecision.put(
          reason,
          metricsFactory.counter(
              "viewFieldDependenciesDecision",
              baseTags
                  .and("result", reason.result())
                  .and("reason", Enums.convertNameTo(CaseFormat.LOWER_UNDERSCORE, reason))));
    }
    this.decisionCounters = ImmutableMap.copyOf(byDecision);

    this.filterOnlyPartialAppliedCounter =
        metricsFactory.counter("viewFilterOnlyPartialApplied", baseTags);
  }

  /** Adds one batch's view-decision tallies to the counters. */
  public void recordBatch(DocumentEventBatch batch) {
    // Derivation fallback rides the batch only on the first batch per index (see owner one-shot).
    batch.viewDerivationFallbackReason.ifPresent(
        reason -> this.derivationFallbackCounters.get(reason).increment());
    batch.viewDecisions.forEach(
        (reason, count) -> this.decisionCounters.get(reason).increment(count));
    if (batch.viewFilterOnlyPartialApplied > 0) {
      this.filterOnlyPartialAppliedCounter.increment(batch.viewFilterOnlyPartialApplied);
    }
  }
}
