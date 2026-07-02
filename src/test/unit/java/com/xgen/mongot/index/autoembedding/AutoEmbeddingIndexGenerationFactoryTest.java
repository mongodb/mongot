package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link AutoEmbeddingIndexGenerationFactory}: the source-collection gate in {@link
 * AutoEmbeddingIndexGenerationFactory#getAutoEmbeddingIndexGeneration} and {@link
 * AutoEmbeddingIndexGenerationFactory#derivedIndexDefinitionGeneration} (vector and search
 * auto-embed source definitions).
 */
public class AutoEmbeddingIndexGenerationFactoryTest {

  private static final String MV_DATABASE_NAME = "__mdb_internal_search";
  private static final UUID MV_COLLECTION_UUID = UUID.randomUUID();

  @Test
  public void derivedIndexDefinitionGeneration_vectorIndex_buildsVectorDerivedDefinition() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(vectorDef, Generation.CURRENT);
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();

    IndexDefinitionGeneration derived =
        AutoEmbeddingIndexGenerationFactory.derivedIndexDefinitionGeneration(rawGen, matViewIndex);

    assertNotNull(derived);
    assertTrue(
        "Vector source must derive a vector index definition generation",
        derived instanceof VectorIndexDefinitionGeneration);
    assertEquals(MV_DATABASE_NAME, derived.getIndexDefinition().getDatabase());
    assertEquals(MV_COLLECTION_UUID, derived.getIndexDefinition().getCollectionUuid());
  }

  @Test
  public void derivedIndexDefinitionGeneration_searchIndex_buildsSearchDerivedDefinition() {
    ObjectId indexId = new ObjectId();
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
                    .build())
            .build();
    SearchIndexDefinitionGeneration rawGen =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(searchDef, Collections.emptyList()),
            Generation.CURRENT);
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();

    IndexDefinitionGeneration derived =
        AutoEmbeddingIndexGenerationFactory.derivedIndexDefinitionGeneration(rawGen, matViewIndex);

    assertNotNull(derived);
    assertTrue(
        "Search source must derive a search index definition generation",
        derived instanceof SearchIndexDefinitionGeneration);
    assertEquals(MV_DATABASE_NAME, derived.getIndexDefinition().getDatabase());
    assertEquals(MV_COLLECTION_UUID, derived.getIndexDefinition().getCollectionUuid());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_sourceCollectionAbsent_skipsMaterializedView()
      throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory = mock(IndexFactory.class);
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(true);

    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(
        "Absent source collection must yield an unresolved generation",
        result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    IndexStatus status = result.getIndex().getStatus();
    assertEquals(IndexStatus.StatusCode.DOES_NOT_EXIST, status.getStatusCode());
    assertEquals(Optional.of(IndexStatus.Reason.COLLECTION_NOT_FOUND), status.getReason());
    // The unresolved generation must carry the source collection UUID so the recovery stager can
    // re-stage it once the collection reappears on this shard (see IndexRecoveryStagerTest).
    assertEquals(
        rawGen.getIndexDefinition().getCollectionUuid(),
        result.getDefinition().getCollectionUuid());
    // No materialized view collection, lease, or leaderStatus gauge is created on a shard that does
    // not host the source collection.
    verify(matViewIndexFactory, never()).getIndex(any());
    verify(indexFactory, never()).getIndex(any());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_sourceCollectionPresent_buildsCompositeGeneration()
      throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory = mock(IndexFactory.class);
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    // Build the mat-view mock before stubbing — matViewIndexMock() does its own stubbing, which
    // Mockito disallows nested inside a thenReturn(...).
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();
    when(matViewIndex.isCompatibleWith(any())).thenReturn(true);
    Index derivedIndex = mock(Index.class);
    when(derivedIndex.isCompatibleWith(any())).thenReturn(true);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(false);
    when(matViewIndexFactory.getIndex(any())).thenReturn(matViewIndex);
    when(indexFactory.getIndex(any())).thenReturn(derivedIndex);

    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(
        "Present source collection must build a composite auto-embedding generation",
        result instanceof AutoEmbeddingIndexGeneration);
    verify(matViewIndexFactory).getIndex(any());
  }

  private static VectorIndexDefinitionGeneration autoEmbedVectorGen() {
    VectorIndexDefinition def =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    return new VectorIndexDefinitionGeneration(def, Generation.CURRENT);
  }

  private static InitializedMaterializedViewIndex matViewIndexMock() {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    when(matViewIndex.getMaterializedViewDatabaseName()).thenReturn(MV_DATABASE_NAME);
    when(matViewIndex.getMaterializedViewCollectionUuid()).thenReturn(MV_COLLECTION_UUID);
    when(matViewIndex.getSchemaMetadata())
        .thenReturn(new MaterializedViewSchemaMetadata(0, Map.of()));
    return matViewIndex;
  }
}
