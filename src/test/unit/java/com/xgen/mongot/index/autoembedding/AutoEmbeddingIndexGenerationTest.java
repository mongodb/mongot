package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link AutoEmbeddingIndexGeneration}: specifically that the derived generation carries
 * a supplier that reflects the materialized view lease's per-definition-version queryable ratchet,
 * so replication persists the correct flag into the derived Lucene commit.
 */
public class AutoEmbeddingIndexGenerationTest {

  @Test
  public void derivedGeneration_commitMetadataSupplier_isLiveDelegateToLeaseRatchet() {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    when(matViewIndex.getMaterializedViewDatabaseName()).thenReturn("__mdb_internal_search");
    when(matViewIndex.getMaterializedViewCollectionUuid()).thenReturn(UUID.randomUUID());
    when(matViewIndex.getSchemaMetadata())
        .thenReturn(new MaterializedViewSchemaMetadata(0, Map.of()));
    when(matViewIndex.isCompatibleWith(any())).thenReturn(true);
    Index derivedIndex = mock(Index.class);
    when(derivedIndex.isCompatibleWith(any())).thenReturn(true);
    AutoEmbeddingCompositeIndex composite =
        new AutoEmbeddingCompositeIndex(matViewIndex, derivedIndex);

    ObjectId indexId = new ObjectId();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .indexId(indexId)
                .name("mock")
                .database("mock")
                .lastObservedCollectionName("mock")
                .collectionUuid(UUID.randomUUID())
                .withAutoEmbedField("title")
                .build(),
            Generation.CURRENT);
    IndexDefinitionGeneration derivedGen =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .indexId(indexId)
                .name("derived")
                .database("__mdb_internal_search")
                .lastObservedCollectionName("123")
                .collectionUuid(UUID.randomUUID())
                .withCosineVectorField("embeddings", 4)
                .build(),
            Generation.CURRENT);

    AutoEmbeddingIndexGeneration generation =
        new AutoEmbeddingIndexGeneration(composite, rawGen, derivedGen);

    Supplier<AutoEmbeddingCommitMetadata> supplier =
        ((DerivedLocalIndexGeneration) generation.getDerivedIndexGeneration())
            .getCommitMetadataSupplier();
    // The persisted commit metadata must track the lease's per-definition-version ratchet live, not
    // a value captured once at construction: flipping the lease result flips the supplier. This
    // guards against the wiring silently degrading to a constant that never reflects the lease.
    when(matViewIndex.isCurrentVersionQueryablePerLease()).thenReturn(true);
    assertTrue(supplier.get().wasMatViewReady());
    when(matViewIndex.isCurrentVersionQueryablePerLease()).thenReturn(false);
    assertFalse(supplier.get().wasMatViewReady());
  }
}
