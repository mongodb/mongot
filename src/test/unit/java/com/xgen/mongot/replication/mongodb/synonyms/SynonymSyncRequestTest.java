package com.xgen.mongot.replication.mongodb.synonyms;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.SearchIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingCompositeIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that synonym sync reads the synonym source collection from the user's database, including
 * for an auto-embedding search index, whose derived Lucene index definition points at the
 * materialized view database.
 */
public class SynonymSyncRequestTest {
  private static final String USER_DATABASE_NAME = "userDb";
  private static final String MV_DATABASE_NAME = "__mdb_internal_search";
  private static final String SYNONYM_SOURCE_COLLECTION_NAME = "product_synonyms";
  private static final MongoNamespace EXPECTED_SYNONYM_SOURCE_NAMESPACE =
      new MongoNamespace(USER_DATABASE_NAME, SYNONYM_SOURCE_COLLECTION_NAME);

  private static final SynonymMappingDefinition SYNONYM_MAPPING_DEFINITION =
      SynonymMappingDefinitionBuilder.builder()
          .name("deviceSyn")
          .analyzer("lucene.standard")
          .synonymSourceDefinition(SYNONYM_SOURCE_COLLECTION_NAME)
          .build();

  @Before
  public void registerTestModels() {
    EmbeddingModelCatalogTestUtils.resetCatalogAndRegisterAutoEmbedModel("voyage-3-large");
  }

  @Test
  public void getNamespace_autoEmbeddingCollectionScan_usesUserDatabase() {
    SynonymCollectionScanRequest request =
        SynonymCollectionScanRequest.create(
            mock(SynonymDocumentIndexer.class),
            autoEmbeddingDerivedIndexGeneration(),
            SYNONYM_MAPPING_DEFINITION,
            () -> {},
            (client, scanRequest) -> mock(SynonymCollectionScanner.class),
            synonymSyncMetrics());

    assertEquals(EXPECTED_SYNONYM_SOURCE_NAMESPACE, request.getNamespace());
  }

  @Test
  public void getNamespace_autoEmbeddingChangeStream_usesUserDatabase() {
    SynonymChangeStreamRequest request =
        SynonymChangeStreamRequest.create(
            mock(ResettableChangeStreamClient.class),
            autoEmbeddingDerivedIndexGeneration(),
            SYNONYM_MAPPING_DEFINITION,
            synonymSyncMetrics());

    assertEquals(EXPECTED_SYNONYM_SOURCE_NAMESPACE, request.getNamespace());
  }

  @Test
  public void getNamespace_searchIndexCollectionScan_usesDefinitionDatabase() {
    SynonymCollectionScanRequest request =
        SynonymCollectionScanRequest.create(
            mock(SynonymDocumentIndexer.class),
            searchIndexGeneration(),
            SYNONYM_MAPPING_DEFINITION,
            () -> {},
            (client, scanRequest) -> mock(SynonymCollectionScanner.class),
            synonymSyncMetrics());

    assertEquals(EXPECTED_SYNONYM_SOURCE_NAMESPACE, request.getNamespace());
  }

  private static IndexGeneration searchIndexGeneration() {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(new ObjectId())
            .name("default")
            .database(USER_DATABASE_NAME)
            .lastObservedCollectionName("products")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .synonyms(List.of(SYNONYM_MAPPING_DEFINITION))
            .build();
    Index index = mock(Index.class);
    when(index.isCompatibleWith(any())).thenReturn(true);
    return new IndexGeneration(index, searchDefinitionGeneration(definition));
  }

  /**
   * Returns the derived Lucene index generation that replication runs synonym sync against, built
   * the same way production builds it from a user search definition with an autoEmbed field.
   */
  private static IndexGeneration autoEmbeddingDerivedIndexGeneration() {
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(new ObjectId())
            .name("default")
            .database(USER_DATABASE_NAME)
            .lastObservedCollectionName("products")
            .collectionUuid(UUID.randomUUID())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "description",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large"))
                            .build())
                    .build())
            .synonyms(List.of(SYNONYM_MAPPING_DEFINITION))
            .build();
    MaterializedViewSchemaMetadata schemaMetadata = new MaterializedViewSchemaMetadata(0, Map.of());
    UUID matViewCollectionUuid = UUID.randomUUID();
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, matViewCollectionUuid, schemaMetadata);

    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    when(matViewIndex.getMaterializedViewDatabaseName()).thenReturn(MV_DATABASE_NAME);
    when(matViewIndex.getMaterializedViewCollectionMetadata())
        .thenReturn(
            new MaterializedViewCollectionMetadata(
                schemaMetadata, matViewCollectionUuid, rawDefinition.getIndexId().toHexString()));
    when(matViewIndex.isCompatibleWith(any())).thenReturn(true);
    SearchIndex derivedSearchIndex = mock(SearchIndex.class);
    when(derivedSearchIndex.getSynonymRegistry()).thenReturn(mock(SynonymRegistry.class));
    Index derivedIndex = mock(Index.class);
    when(derivedIndex.isCompatibleWith(any())).thenReturn(true);
    when(derivedIndex.asSearchIndex()).thenReturn(derivedSearchIndex);

    AutoEmbeddingIndexGeneration generation =
        new AutoEmbeddingIndexGeneration(
            new AutoEmbeddingCompositeIndex(matViewIndex, derivedIndex),
            searchDefinitionGeneration(rawDefinition),
            searchDefinitionGeneration(derivedDefinition));
    return generation.getDerivedIndexGeneration();
  }

  private static SearchIndexDefinitionGeneration searchDefinitionGeneration(
      SearchIndexDefinition definition) {
    return new SearchIndexDefinitionGeneration(
        AnalyzerBoundSearchIndexDefinition.create(definition, Collections.emptyList()),
        Generation.CURRENT);
  }

  private static SynonymSyncMetrics synonymSyncMetrics() {
    return new SynonymSyncMetrics(new MetricsFactory("synonymSync", new SimpleMeterRegistry()));
  }
}
