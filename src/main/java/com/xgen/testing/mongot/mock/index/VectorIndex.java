package com.xgen.testing.mongot.mock.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class VectorIndex {

  public static final ObjectId MOCK_INDEX_ID = new ObjectId();
  public static final String MOCK_INDEX_NAME = "default";
  public static final String MOCK_INDEX_DATABASE_NAME = "mock_database";
  public static final String MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME = "mock_collection";
  public static final UUID MOCK_INDEX_COLLECTION_UUID = UUID.randomUUID();
  public static final VectorIndexDefinition MOCK_VECTOR_DEFINITION =
      mockVectorDefinitionBuilder().build();
  public static final VectorIndexDefinition MOCK_SCALAR_QUANTIZED_VECTOR_DEFINITION =
      mockScalarQuantizedVectorDefinitionBuilder().build();
  public static final VectorIndexDefinition MOCK_BINARY_QUANTIZED_VECTOR_DEFINITION =
      mockBinaryQuantizedVectorDefinitionBuilder().build();
  public static final VectorIndexDefinition MOCK_ALL_QUANTIZED_VECTOR_DEFINITION =
      mockAllQuantizedVectorDefinitionBuilder().build();
  public static final VectorIndexDefinitionGeneration MOCK_VECTOR_INDEX_DEFINITION_GENERATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_VECTOR_DEFINITION);
  public static final VectorIndexDefinition MOCK_AUTO_EMBEDDING_INDEX_DEFINITION =
      mockAutoEmbeddingVectorDefinition(MOCK_INDEX_ID);
  public static final VectorIndexDefinition MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION =
      mockMaterializedViewAutoEmbeddingVectorDefinition(MOCK_INDEX_ID);
  public static final VectorIndexDefinitionGeneration
      MOCK_AUTO_EMBEDDING_INDEX_DEFINITION_GENERATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_AUTO_EMBEDDING_INDEX_DEFINITION);
  public static final GenerationId MOCK_AUTO_EMBEDDING_INDEX_GENERATION_ID =
      MOCK_AUTO_EMBEDDING_INDEX_DEFINITION_GENERATION.getGenerationId();
  public static final IndexDefinitionGeneration MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT =
      new VectorIndexDefinitionGeneration(MOCK_VECTOR_DEFINITION, Generation.CURRENT);
  public static final VectorIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_VECTOR_DEFINITION);
  public static final VectorIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION_SCALAR =
      IndexGeneration.mockDefinitionGeneration(MOCK_SCALAR_QUANTIZED_VECTOR_DEFINITION);
  public static final VectorIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION_BINARY =
      IndexGeneration.mockDefinitionGeneration(MOCK_BINARY_QUANTIZED_VECTOR_DEFINITION);
  public static final VectorIndexDefinitionGeneration
      MOCK_INDEX_DEFINITION_GENERATION_ALL_QUANTIZATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_ALL_QUANTIZED_VECTOR_DEFINITION);

  public static final GenerationId MOCK_INDEX_GENERATION_ID =
      MOCK_INDEX_DEFINITION_GENERATION.getGenerationId();

  public static final int NUM_PARTITIONS = 8;
  public static final VectorIndexDefinition MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION =
      mockVectorDefinitionBuilder().numPartitions(NUM_PARTITIONS).build();
  public static final VectorIndexDefinitionGeneration
      MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION_GENERATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION);

  /** Creates mock VectorIndex. */
  public static com.xgen.mongot.index.VectorIndex mockIndex(
      VectorIndexDefinitionGeneration definitionGeneration
  ) {
    var index = Mockito.mock(com.xgen.mongot.index.VectorIndex.class);
    var definition = definitionGeneration.getIndexDefinition();

    Mockito.when(index.isCompatibleWith(any(VectorIndexDefinition.class))).thenReturn(true);
    Mockito.lenient().when(index.getDefinition()).thenReturn(definition);
    Mockito.lenient().when(index.asVectorIndex()).thenCallRealMethod();

    // Actually keep track of the status updates to this mock index.
    AtomicReference<IndexStatus> statusContainer = new AtomicReference<>(IndexStatus.steady());

    Answer<Void> setStatus =
        invocation -> {
          statusContainer.set(invocation.getArgument(0));
          return null;
        };

    Mockito.lenient().doAnswer(setStatus).when(index).setStatus(any());
    Mockito.lenient().when(index.getStatus()).then(ignored -> statusContainer.get());

    return index;
  }

  public static com.xgen.mongot.index.InitializedVectorIndex mockInitializedIndex(
      VectorIndexDefinitionGeneration definitionGeneration
  ) {
    return mockInitializedIndex(
        new com.xgen.mongot.index.IndexGeneration(
            mockIndex(definitionGeneration), definitionGeneration));
  }

  public static com.xgen.mongot.index.InitializedVectorIndex mockInitializedIndex(
      com.xgen.mongot.index.IndexGeneration indexGeneration
  ) {
    return mockInitializedIndex(
        indexGeneration.getIndex().asVectorIndex(), indexGeneration.getGenerationId());
  }

  /** Returns an InitializedIndex that can be used in tests that require one. */
  public static InitializedVectorIndex mockInitializedIndex(
      com.xgen.mongot.index.VectorIndex index, GenerationId generationId) {
    var initializedIndex = mock(InitializedVectorIndex.class);
    Mockito.lenient().when(initializedIndex.getGenerationId()).thenReturn(generationId);
    var indexReader = VectorIndexReader.mockIndexReader();
    Mockito.lenient().when(initializedIndex.getReader()).thenReturn(indexReader);

    var indexWriter = IndexWriter.mockIndexWriter();
    Mockito.lenient().when(indexWriter.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
    Mockito.lenient().when(initializedIndex.getWriter()).thenReturn(indexWriter);

    var metricsFactory =
        new PerIndexMetricsFactory(
            IndexMetricsUpdater.NAMESPACE,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            generationId);

    IndexMetricValuesSupplier indexMetricValuesSupplier = mock(IndexMetricValuesSupplier.class);
    Mockito.lenient().when(indexMetricValuesSupplier.computeIndexSize()).thenReturn(0L);
    Mockito.lenient().when(indexMetricValuesSupplier.getCachedIndexSize()).thenReturn(0L);

    Mockito.lenient()
        .doAnswer(ignored -> index.getStatus())
        .when(indexMetricValuesSupplier)
        .getIndexStatus();

    Mockito.lenient().when(indexMetricValuesSupplier.getNumFields()).thenReturn(0);
    Mockito.lenient()
        .when(indexMetricValuesSupplier.getDocCounts())
        .thenReturn(new DocCounts(0, 0, 0, 0L));

    IndexMetricsUpdater indexMetricsUpdater =
        spy(
            new IndexMetricsUpdater(
                index.getDefinition(), indexMetricValuesSupplier, metricsFactory));

    IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater =
        spy(
            new IndexMetricsUpdater.IndexingMetricsUpdater(
                metricsFactory.childMetricsFactory(
                    IndexMetricsUpdater.IndexingMetricsUpdater.NAMESPACE),
                index.getDefinition().getType()));

    // if replicationLagMs is not Optional.empty(), replace it with a set value because it will be
    // hard to compare during testing
    ReplicationOpTimeInfo replicationOpTimeInfo = spy(new ReplicationOpTimeInfo());
    Mockito.lenient()
        .doAnswer(
            (invocationOnMock) ->
                invocationOnMock.callRealMethod().equals(Optional.empty())
                    ? Optional.empty()
                    : Optional.of(new ReplicationOpTimeInfo.Snapshot(10L, 20L, 10L)))
        .when(replicationOpTimeInfo)
        .snapshot();

    Mockito.lenient()
        .doReturn(replicationOpTimeInfo)
        .when(indexingMetricsUpdater)
        .getReplicationOpTimeInfo();
    Mockito.lenient()
        .doReturn(indexingMetricsUpdater)
        .when(indexMetricsUpdater)
        .getIndexingMetricsUpdater();
    IndexMetrics metrics = indexMetricsUpdater.getMetrics();
    Mockito.lenient().when(initializedIndex.getMetrics()).thenReturn(metrics);
    Mockito.lenient().when(initializedIndex.getMetricsUpdater()).thenReturn(indexMetricsUpdater);
    Mockito.lenient()
        .when(initializedIndex.getIndexSize())
        .thenAnswer(invocation -> indexMetricsUpdater.getIndexSize());

    Mockito.lenient().doReturn(index.getDefinition()).when(initializedIndex).getDefinition();

    // Mock wrapper methods of the InitializedIndex, with the same functionality as the
    // underlying Index.
    Mockito.lenient().doAnswer(ignored -> index.getStatus()).when(initializedIndex).getStatus();
    Mockito.lenient()
        .doAnswer(
            invocation -> {
              index.setStatus(invocation.getArgument(0));
              return null;
            })
        .when(initializedIndex)
        .setStatus(any());
    Mockito.lenient()
        .when(initializedIndex.isCompatibleWith(any(VectorIndexDefinition.class)))
        .thenReturn(true);
    Mockito.lenient().doReturn(index.isClosed()).when(initializedIndex).isClosed();
    return initializedIndex;
  }

  public static VectorIndexDefinition mockVectorDefinition(ObjectId indexId) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withCosineVectorField("vector", 1024)
        .withFilterPath("filter")
        .build();
  }

  public static VectorIndexDefinition mockAutoEmbeddingVectorDefinition(ObjectId indexId) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withTextField("field")
        .build();
  }

  public static VectorIndexDefinition mockMaterializedViewAutoEmbeddingVectorDefinition(
      ObjectId indexId
  ) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withAutoEmbedField("field")
        .build();
  }

  public static PerIndexMetricsFactory mockMetricsFactory() {
    return new PerIndexMetricsFactory(
        IndexMetricsUpdater.NAMESPACE,
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        GenerationIdBuilder.create());
  }

  public static IndexMetricsUpdater.QueryingMetricsUpdater mockQueryMetricsUpdater() {
    return new IndexMetricsUpdater.QueryingMetricsUpdater(
        mockMetricsFactory()
            .childMetricsFactory(IndexMetricsUpdater.QueryingMetricsUpdater.NAMESPACE),
        IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH,
        VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
        true);
  }

  public static IndexMetricsUpdater.IndexingMetricsUpdater mockIndexingMetricsUpdater() {
    return new IndexMetricsUpdater.IndexingMetricsUpdater(
        mockMetricsFactory()
            .childMetricsFactory(IndexMetricsUpdater.IndexingMetricsUpdater.NAMESPACE),
        IndexDefinition.Type.VECTOR_SEARCH);
  }

  private static VectorIndexDefinitionBuilder mockVectorDefinitionBuilder() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withFilterPath("filter.path")
        .withCosineVectorField("vector.path", 3);
  }

  private static VectorIndexDefinitionBuilder mockScalarQuantizedVectorDefinitionBuilder() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withFilterPath("filter.path")
        .withScalarQuantizedCosineVectorField("vector.path", 3);
  }

  private static VectorIndexDefinitionBuilder mockBinaryQuantizedVectorDefinitionBuilder() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withFilterPath("filter.path")
        .withBinaryQuantizedCosineVectorField("vector.path", 3);
  }

  private static VectorIndexDefinitionBuilder mockAllQuantizedVectorDefinitionBuilder() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withFilterPath("filter.path")
        .withCosineVectorField("unquantized.vector.path", 3)
        .withScalarQuantizedCosineVectorField("scalar.vector.path", 3)
        .withBinaryQuantizedCosineVectorField("binary.vector.path", 3);
  }
}
