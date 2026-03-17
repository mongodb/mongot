package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link InitializedMaterializedViewIndex}, including that closing the index unregisters
 * the leader-status gauge (Bug: gauge remained after auto-embedding index drop).
 */
public class InitializedMaterializedViewIndexTest {

  private static final ObjectId INDEX_ID = new ObjectId();
  private static final String NAMESPACE = "embedding.materializedView.stats";

  @Test
  public void close_whenCalled_unregistersLeaderStatusGauge() throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MeterRegistry meterRegistry = meterAndFtdcRegistry.meterRegistry();

    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID);
    MaterializedViewGenerationId generationId = defGen.getGenerationId();
    String uniqueString = generationId.uniqueString();
    String collectionName = "matview-" + INDEX_ID.toHexString();

    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(NAMESPACE, meterAndFtdcRegistry, uniqueString, collectionName);
    IndexMetricValuesSupplier metricValuesSupplier =
        IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
    IndexMetricsUpdater indexMetricsUpdater =
        new IndexMetricsUpdater(defGen.getIndexDefinition(), metricValuesSupplier, metricsFactory);

    MaterializedViewWriter writer = mock(MaterializedViewWriter.class);
    LeaseManager leaseManager = mock(LeaseManager.class);
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(0, java.util.Map.of());

    InitializedMaterializedViewIndex index =
        new InitializedMaterializedViewIndex(
            defGen, writer, indexMetricsUpdater, statusRef, leaseManager, schemaMetadata);

    long leaderStatusGaugesBefore =
        meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().contains("leaderStatus"))
            .count();
    assertTrue(
        "leaderStatus gauge should be registered after construction",
        leaderStatusGaugesBefore >= 1);

    index.close();

    long leaderStatusGaugesAfter =
        meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().contains("leaderStatus"))
            .count();
    assertEquals(
        "leaderStatus gauge should be unregistered after close()", 0, leaderStatusGaugesAfter);
  }
}
