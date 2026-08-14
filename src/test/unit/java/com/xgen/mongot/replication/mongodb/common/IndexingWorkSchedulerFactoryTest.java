package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_AUTO_EMBEDDING_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_DEFINITION;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.base.Supplier;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSearchEngine;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

public class IndexingWorkSchedulerFactoryTest {

  @Test
  public void testContainsSchedulerForEveryIndexingStrategy() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.create(
            2, mock(Supplier.class), new SimpleMeterRegistry(), true);

    // create() only provides DEFAULT and EMBEDDING; EMBEDDING_MATERIALIZED_VIEW is handled
    // exclusively by MaterializedViewManager via createEmbeddingIndexingSchedulerOnly().
    assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers())
        .containsKey(IndexingWorkSchedulerFactory.IndexingStrategy.DEFAULT);
    assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers())
        .containsKey(IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING);
    assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers())
        .doesNotContainKey(
            IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW);
  }

  @Test
  public void testCreateWithoutEmbeddingStrategy() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(
            2, new SimpleMeterRegistry(), true);

    for (IndexingWorkSchedulerFactory.IndexingStrategy strategy :
        IndexingWorkSchedulerFactory.IndexingStrategy.values()) {
      if (strategy == IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING
          || strategy
              == IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW) {
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers())
            .doesNotContainKey(strategy);
      } else {
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).containsKey(strategy);
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers().get(strategy))
            .isInstanceOf(IndexingWorkScheduler.class);
      }
    }
  }

  @Test
  public void testCreateMaterializedViewEmbeddingStrategyOnly() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createEmbeddingIndexingSchedulerOnly(
            2,
            mock(Supplier.class),
            new MaterializedViewCollectionMetadataCatalog(),
            new SimpleMeterRegistry(),
            100,
            100);
    for (IndexingWorkSchedulerFactory.IndexingStrategy strategy :
        IndexingWorkSchedulerFactory.IndexingStrategy.values()) {
      assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).containsKey(strategy);
      assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers().get(strategy))
          .isInstanceOf(EmbeddingIndexingWorkScheduler.class);
    }
  }

  @Test
  public void testReturnsProperScheduler() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.create(
            2, mock(Supplier.class), new SimpleMeterRegistry(), true);

    IndexingWorkScheduler embeddingIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_AUTO_EMBEDDING_INDEX_DEFINITION);
    IndexingWorkScheduler defaultIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_INDEX_DEFINITION);
    IndexingWorkScheduler luceneVectorIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_VECTOR_DEFINITION);
    IndexingWorkScheduler customVectorEngineIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(mockCustomVectorEngineDefinition());

    assertThat(embeddingIndexingWorkScheduler).isInstanceOf(EmbeddingIndexingWorkScheduler.class);
    assertThat(defaultIndexingWorkScheduler).isInstanceOf(DefaultIndexingWorkScheduler.class);
    assertThat(luceneVectorIndexingWorkScheduler).isInstanceOf(DefaultIndexingWorkScheduler.class);
    assertThat(customVectorEngineIndexingWorkScheduler)
        .isInstanceOf(CustomVectorEngineIndexingWorkScheduler.class);
  }

  @Test
  public void testThrowsIfIndexingStrategyNotPresent() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(
            2, new SimpleMeterRegistry(), true);

    assertThrows(
        "EMBEDDING indexing strategy is not supported. "
            + "Auto-embedding indexes require the EMBEDDING indexing strategy.",
        IllegalStateException.class,
        () ->
            indexingWorkSchedulerFactory.getIndexingWorkScheduler(
                MOCK_AUTO_EMBEDDING_INDEX_DEFINITION));
  }

  @Test
  public void create_lifecycleAttributionDisabled_doesNotRegisterResourceMetrics() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    IndexingWorkSchedulerFactory.create(2, mock(Supplier.class), registry, Optional.empty());

    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "replication")
                .functionCounter())
        .isNull();
  }

  @Test
  public void create_lifecycleAttributionEnabled_registersIndexingPoolCounters() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    IndexingWorkSchedulerFactory.create(
        2, mock(Supplier.class), registry, Optional.of(new ThreadPoolResourceMetrics.Bindings()));

    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "replication")
                .tag("name", "indexing-work")
                .functionCounter())
        .isNotNull();
    assertThat(
            registry
                .find("executor.thread.cpuTime")
                .tag("subsystem", "replication")
                .tag("name", "indexing-work")
                .functionCounter())
        .isNotNull();
  }

  private static VectorIndexDefinition mockCustomVectorEngineDefinition() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(new ObjectId())
        .name("custom_vector_engine_index")
        .database("mock_database")
        .lastObservedCollectionName("mock_collection")
        .collectionUuid(UUID.randomUUID())
        .setFields(
            List.<VectorIndexFieldDefinition>of(
                new VectorDataFieldDefinition(
                    FieldPath.parse("vector.path"),
                    new VectorFieldSpecification(
                        3,
                        VectorSimilarity.COSINE,
                        VectorQuantization.NONE,
                        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
                        VectorSearchEngine.CUSTOM))))
        .build();
  }
}
