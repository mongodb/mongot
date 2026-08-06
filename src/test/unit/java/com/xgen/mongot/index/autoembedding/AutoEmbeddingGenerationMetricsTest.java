package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AutoEmbeddingGenerationMetricsTest {

  @Test
  public void unresolvedMetric_reportsBoundedTransientReason() {
    MeterAndFtdcRegistry registries = MeterAndFtdcRegistry.createWithSimpleRegistries();
    VectorIndexDefinitionGeneration rawGeneration = autoEmbeddingGeneration();
    AutoEmbeddingGenerationMetrics metrics =
        new AutoEmbeddingGenerationMetrics(registries, rawGeneration);

    metrics.recordUnresolved(MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);

    assertEquals(
        1,
        resolutionGauge(
            registries.meterRegistry(), rawGeneration, "UNRESOLVED", "BULK_WRITE_ERROR"),
        0);
  }

  @Test
  public void resolvedMetric_mapsLuceneGenerationToMaterializedViewLease() {
    MeterAndFtdcRegistry registries = MeterAndFtdcRegistry.createWithSimpleRegistries();
    VectorIndexDefinitionGeneration rawGeneration = autoEmbeddingGeneration();
    MaterializedViewGenerationId materializedViewGenerationId =
        AutoEmbeddingIndexGenerationFactory.createMaterializedViewIndexDefinitionGeneration(
                rawGeneration)
            .getGenerationId();
    AutoEmbeddingGenerationMetrics metrics =
        new AutoEmbeddingGenerationMetrics(registries, rawGeneration);

    metrics.recordResolved(materializedViewGenerationId, "lease-id");

    assertEquals(
        1, resolutionGauge(registries.meterRegistry(), rawGeneration, "RESOLVED", "NONE"), 0);
    assertEquals(
        1,
        registries
            .meterRegistry()
            .get(metricName(AutoEmbeddingGenerationMetrics.GENERATION_MAPPING))
            // The raw generation is also the derived Lucene generation.
            .tag("generationId logString", rawGeneration.getGenerationId().uniqueString())
            .tag("materializedViewGenerationId", materializedViewGenerationId.uniqueString())
            .tag("leaseId", "lease-id")
            .gauge()
            .value(),
        0);
  }

  @Test
  public void close_unregistersGenerationMetrics() {
    MeterAndFtdcRegistry registries = MeterAndFtdcRegistry.createWithSimpleRegistries();
    VectorIndexDefinitionGeneration rawGeneration = autoEmbeddingGeneration();
    AutoEmbeddingGenerationMetrics metrics =
        new AutoEmbeddingGenerationMetrics(registries, rawGeneration);
    metrics.recordCollectionNotFound();

    assertEquals(1, countGenerationMetrics(registries.meterRegistry()));

    metrics.close();

    assertEquals(0, countGenerationMetrics(registries.meterRegistry()));
  }

  private static double resolutionGauge(
      MeterRegistry meterRegistry,
      VectorIndexDefinitionGeneration rawGeneration,
      String state,
      String reason) {
    return meterRegistry
        .get(metricName(AutoEmbeddingGenerationMetrics.RESOLUTION_STATE))
        .tag("generationId logString", rawGeneration.getGenerationId().uniqueString())
        .tag("state", state)
        .tag("reason", reason)
        .gauge()
        .value();
  }

  private static long countGenerationMetrics(MeterRegistry meterRegistry) {
    return meterRegistry.getMeters().stream()
        .filter(
            meter ->
                meter.getId().getName().startsWith(AutoEmbeddingGenerationMetrics.NAMESPACE + "."))
        .count();
  }

  private static String metricName(String name) {
    return AutoEmbeddingGenerationMetrics.NAMESPACE + "." + name;
  }

  private static VectorIndexDefinitionGeneration autoEmbeddingGeneration() {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    return new VectorIndexDefinitionGeneration(definition, Generation.CURRENT);
  }
}
