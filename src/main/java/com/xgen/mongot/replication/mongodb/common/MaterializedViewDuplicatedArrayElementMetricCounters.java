package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.utils.MaterializedViewArrayElementType;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;
import java.util.Map;

/**
 * Counts materialized-view documents that contain array elements the pre-CLOUDP-459586 writer
 * duplicated.
 *
 * <p>An increment means one MV document (or partial-update $set body) contained a UUID or
 * vector-parseable element inside a passthrough array. The metric is emitted from the fixed writer
 * as well, where it counts the documents the pre-fix writer would have corrupted.
 *
 * <p>The metric name ends in {@code Estimate} because the full-build path samples one document in
 * {@code EmbeddingIndexingWorkScheduler#DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS} and scales each
 * hit by that factor. The {@code sampleModulus} tag carries the factor ({@code 1} for the exact
 * partial-update path).
 */
public final class MaterializedViewDuplicatedArrayElementMetricCounters {
  public static final String METRIC_NAME = "mvWritesWithDuplicatedArrayElementsEstimate";
  static final String ELEMENT_TYPE_TAG = "elementType";
  static final String SAMPLE_MODULUS_TAG = "sampleModulus";

  private final ImmutableMap<MaterializedViewArrayElementType, Counter> counters;

  public MaterializedViewDuplicatedArrayElementMetricCounters(
      MetricsFactory metricsFactory, Tags baseTags) {
    EnumMap<MaterializedViewArrayElementType, Counter> byElement =
        new EnumMap<>(MaterializedViewArrayElementType.class);
    for (MaterializedViewArrayElementType element : MaterializedViewArrayElementType.values()) {
      byElement.put(
          element,
          metricsFactory.counter(METRIC_NAME, baseTags.and(ELEMENT_TYPE_TAG, element.tagValue())));
    }
    this.counters = ImmutableMap.copyOf(byElement);
  }

  /** Records {@code count} estimated documents for {@code element}. */
  public void record(MaterializedViewArrayElementType element, double count) {
    this.counters.get(element).increment(count);
  }

  public void recordBatch(Map<MaterializedViewArrayElementType, Integer> counts) {
    counts.forEach((element, count) -> this.counters.get(element).increment(count));
  }
}
