package com.xgen.testing.mongot.mock.index;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

// TODO(CLOUDP-386265): Avoid constructing indexes with mocks dependency.
public class MaterializedViewIndex {
  /** Creates mock InitializedMaterializedViewIndex from a definition generation. */
  public static InitializedMaterializedViewIndex mockIndex(
      MaterializedViewIndexDefinitionGeneration definitionGeneration) {
    return mockIndex(definitionGeneration.definition());
  }

  /** Creates mock InitializedMaterializedViewIndex for a vector or search index definition. */
  public static InitializedMaterializedViewIndex mockIndex(IndexDefinition indexDefinition) {
    var writer = mock(MaterializedViewWriter.class);
    when(writer.dropMaterializedViewCollection()).thenReturn(COMPLETED_FUTURE);
    var index = mock(InitializedMaterializedViewIndex.class);
    Mockito.when(index.isCompatibleWith(any(IndexDefinition.class))).thenReturn(true);
    Mockito.lenient().when(index.getDefinition()).thenReturn(indexDefinition);
    Mockito.when(index.getWriter()).thenReturn(writer);
    // Actually keep track of the status updates to this mock index.
    AtomicReference<IndexStatus> statusContainer = new AtomicReference<>(IndexStatus.steady());
    Answer<Void> setStatus =
        invocation -> {
          statusContainer.set(invocation.getArgument(0));
          return null;
        };
    Mockito.lenient().doAnswer(setStatus).when(index).setStatus(any());
    Mockito.lenient().when(index.getStatus()).then(ignored -> statusContainer.get());
    Mockito.lenient().when(index.getIndexSize()).thenReturn(0L);
    UUID collectionUuid = UUID.randomUUID();
    var collectionMetadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            collectionUuid,
            indexDefinition.getIndexId().toHexString());
    Mockito.when(index.getMaterializedViewCollectionUuid()).thenReturn(collectionUuid);
    Mockito.when(index.getSchemaMetadata()).thenReturn(collectionMetadata.schemaMetadata());
    Mockito.when(index.getMaterializedViewCollectionMetadata()).thenReturn(collectionMetadata);
    return index;
  }

  public static VectorIndexDefinition mockAutoEmbeddingVectorDefinition(
      ObjectId indexId, long indexDefinitionVersion) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name("mock")
        .database("mock")
        .lastObservedCollectionName("mock")
        .collectionUuid(UUID.randomUUID())
        .withAutoEmbedField("field")
        .withDefinitionVersion(Optional.of(indexDefinitionVersion))
        .build();
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewDefinitionGeneration(
      ObjectId indexId) {
    return mockMatViewDefinitionGeneration(indexId, 0);
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewDefinitionGeneration(
      ObjectId indexId, long indexDefinitionVersion) {
    return new MaterializedViewIndexDefinitionGeneration(
        mockAutoEmbeddingVectorDefinition(indexId, indexDefinitionVersion),
        new MaterializedViewGeneration(Generation.CURRENT));
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewDefinitionGeneration(
      MaterializedViewGenerationId genId) {
    return mockMatViewDefinitionGeneration(genId, 0);
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewDefinitionGeneration(
      MaterializedViewGenerationId genId, long indexDefinitionVersion) {
    return new MaterializedViewIndexDefinitionGeneration(
        mockAutoEmbeddingVectorDefinition(genId.indexId, indexDefinitionVersion), genId.generation);
  }

  public static MaterializedViewIndexGeneration mockMatViewIndexGeneration(
      MaterializedViewIndexDefinitionGeneration materializedViewIndexDefinitionGeneration) {
    return new MaterializedViewIndexGeneration(
        mockIndex(materializedViewIndexDefinitionGeneration),
        materializedViewIndexDefinitionGeneration);
  }

  /** Builds a search index definition with one auto-embed field for mock-based tests. */
  public static SearchIndexDefinition mockAutoEmbeddingSearchDefinition(
      ObjectId indexId, long indexDefinitionVersion) {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("field"));
    return SearchIndexDefinitionBuilder.builder()
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
                    "field_embed",
                    FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
                .build())
        .definitionVersion(indexDefinitionVersion)
        .build();
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewSearchDefinitionGeneration(
      ObjectId indexId) {
    return mockMatViewSearchDefinitionGeneration(indexId, 0);
  }

  public static MaterializedViewIndexDefinitionGeneration mockMatViewSearchDefinitionGeneration(
      ObjectId indexId, long indexDefinitionVersion) {
    return new MaterializedViewIndexDefinitionGeneration(
        mockAutoEmbeddingSearchDefinition(indexId, indexDefinitionVersion),
        new MaterializedViewGeneration(Generation.CURRENT));
  }
}
