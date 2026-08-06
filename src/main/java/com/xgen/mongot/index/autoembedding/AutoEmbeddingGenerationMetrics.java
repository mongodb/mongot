package com.xgen.mongot.index.autoembedding;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import io.micrometer.core.instrument.Tags;

/**
 * Generation-scoped metrics that remain available even when materialized-view resolution fails.
 *
 * <p>The factory's generation tag is the raw auto-embedding generation, which is also the derived
 * Lucene generation. Resolved generations add the materialized-view generation and lease ID as an
 * explicit mapping.
 */
class AutoEmbeddingGenerationMetrics implements AutoCloseable {
  static final String NAMESPACE = "autoEmbedding.stats";
  static final String RESOLUTION_STATE = "resolutionState";
  static final String GENERATION_MAPPING = "generationMapping";

  private static final String STATE_TAG = "state";
  private static final String REASON_TAG = "reason";
  private static final String NO_REASON = "NONE";
  private static final String MATERIALIZED_VIEW_GENERATION_ID_TAG = "materializedViewGenerationId";
  private static final String LEASE_ID_TAG = "leaseId";

  enum ResolutionState {
    RESOLVED,
    UNRESOLVED
  }

  private final PerIndexMetricsFactory metricsFactory;

  AutoEmbeddingGenerationMetrics(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      IndexDefinitionGeneration rawDefinitionGeneration) {
    this.metricsFactory =
        new PerIndexMetricsFactory(
            NAMESPACE,
            meterAndFtdcRegistry,
            rawDefinitionGeneration.getGenerationId().uniqueString(),
            rawDefinitionGeneration.getIndexDefinition().getIndexId().toHexString(),
            true);
  }

  void recordResolved(MaterializedViewGenerationId materializedViewGenerationId, String leaseId) {
    recordResolution(ResolutionState.RESOLVED, NO_REASON);
    this.metricsFactory
        .perIndexNumGauge(
            GENERATION_MAPPING,
            Tags.of(
                MATERIALIZED_VIEW_GENERATION_ID_TAG,
                materializedViewGenerationId.uniqueString(),
                LEASE_ID_TAG,
                leaseId))
        .set(1);
  }

  void recordCollectionNotFound() {
    recordResolution(ResolutionState.UNRESOLVED, IndexStatus.Reason.COLLECTION_NOT_FOUND.name());
  }

  void recordUnresolved(MaterializedViewTransientException.Reason reason) {
    recordResolution(ResolutionState.UNRESOLVED, reason.name());
  }

  private void recordResolution(ResolutionState state, String reason) {
    this.metricsFactory
        .perIndexNumGauge(RESOLUTION_STATE, Tags.of(STATE_TAG, state.name(), REASON_TAG, reason))
        .set(1);
  }

  @Override
  public void close() {
    this.metricsFactory.close();
  }
}
