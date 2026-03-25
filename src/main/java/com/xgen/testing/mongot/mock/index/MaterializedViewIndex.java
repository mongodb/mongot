package com.xgen.testing.mongot.mock.index;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

// TODO(CLOUDP-386265): Avoid constructing indexes with mocks dependency.
public class MaterializedViewIndex {
  /** Creates mock VectorIndex. */
  public static InitializedMaterializedViewIndex mockIndex(
      MaterializedViewIndexDefinitionGeneration definitionGeneration) {
    // TODO(CLOUDP-353553): Handle search index version - definition() now returns IndexDefinition
    //  which may be a SearchIndexDefinition.
    return mockIndex(definitionGeneration.definition().asVectorDefinition());
  }

  /** Creates mock InitializedMaterializedViewIndex. */
  public static InitializedMaterializedViewIndex mockIndex(
      VectorIndexDefinition vectorIndexDefinition) {
    var writer = mock(MaterializedViewWriter.class);
    when(writer.dropMaterializedViewCollection()).thenReturn(COMPLETED_FUTURE);
    var index = mock(InitializedMaterializedViewIndex.class);
    Mockito.when(index.isCompatibleWith(any(VectorIndexDefinition.class))).thenReturn(true);
    Mockito.lenient().when(index.getDefinition()).thenReturn(vectorIndexDefinition);
    Mockito.lenient().when(index.asVectorIndex()).thenCallRealMethod();
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
    Mockito.when(index.getMaterializedViewCollectionUuid()).thenReturn(UUID.randomUUID());
    Mockito.when(index.getSchemaMetadata())
        .thenReturn(MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO);
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
}
