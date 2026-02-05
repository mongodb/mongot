package com.xgen.testing.mongot.mock.index;

import static com.xgen.testing.mongot.mock.index.SearchIndexReader.mockIndexReader;
import static com.xgen.testing.mongot.mock.index.SearchIndexReader.mockIndexReaderWithMetaProducer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class SearchIndex {

  public static final ObjectId MOCK_INDEX_ID = new ObjectId();
  public static final String MOCK_INDEX_NAME = "default";
  public static final String MOCK_INDEX_DATABASE_NAME = "mock_database";
  public static final String MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME = "mock_collection";
  public static final String MOCK_SEARCH_ANALYZER_NAME = "my_analyzer";
  public static final UUID MOCK_INDEX_COLLECTION_UUID = UUID.randomUUID();
  public static final DocumentFieldDefinition MOCK_INDEX_MAPPINGS =
      DocumentFieldDefinitionBuilder.builder().dynamic(true).build();

  public static final DocumentFieldDefinitionBuilder MOCK_FACET_INDEX_MAPPINGS =
      DocumentFieldDefinitionBuilder.builder()
          .field(
              "f",
              FieldDefinitionBuilder.builder()
                  .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                  .build());

  public static final String MOCK_SYNONYM_MAPPING_DEFINITION_NAME = "mock_mapping_name";
  public static final String MOCK_SYNONYM_SOURCE_COLLECTION_NAME = "my_synonyms";
  public static final String MOCK_SYNONYM_ANALYZER_NAME = "lucene.standard";
  public static final SynonymMappingDefinition MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION =
      SynonymMappingDefinitionBuilder.builder()
          .name(MOCK_SYNONYM_MAPPING_DEFINITION_NAME)
          .analyzer(MOCK_SYNONYM_ANALYZER_NAME)
          .synonymSourceDefinition(MOCK_SYNONYM_SOURCE_COLLECTION_NAME)
          .build();
  public static final List<SynonymMappingDefinition> MOCK_SYNONYM_MAPPING_DEFINITIONS =
      List.of(MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION);

  public static final SearchIndexDefinition MOCK_INDEX_DEFINITION = mockDefinitionBuilder().build();
  public static final SearchIndexDefinition MOCK_INDEX_DEFINITION_EDIT =
      mockDefinitionBuilder().analyzerName("lucene.english").build();
  public static final int NUM_PARTITIONS = 8;
  public static final SearchIndexDefinition MOCK_MULTI_INDEX_PARTITION_DEFINITION =
      mockDefinitionBuilder().numPartitions(NUM_PARTITIONS).build();
  public static final SearchIndexDefinitionGeneration
      MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION =
          IndexGeneration.mockDefinitionGeneration(MOCK_MULTI_INDEX_PARTITION_DEFINITION);
  public static final GenerationId MOCK_MULTI_INDEX_PARTITION_GENERATION_ID =
      MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION.getGenerationId();
  public static final SearchIndexDefinition MOCK_FACET_INDEX_DEFINITION =
      mockFacetDefinitionBuilder(false).build();
  public static final SearchIndexDefinition MOCK_MULTI_FACET_INDEX_DEFINITION =
      mockFacetDefinitionBuilder(false).numPartitions(NUM_PARTITIONS).build();
  public static final SearchIndexDefinitionGeneration
      MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION =
          IndexGeneration.mockDefinitionGeneration(
              SearchIndex.mockFacetDefinitionBuilder(true).numPartitions(NUM_PARTITIONS).build());

  public static final SearchIndexDefinitionGeneration
      MOCK_DYNAMIC_MULTI_FACET_DEFINITION_GENERATION =
          IndexGeneration.mockDefinitionGeneration(
              SearchIndex.mockFacetDefinitionBuilder(true).numPartitions(1).build());

  public static final SearchIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION =
      IndexGeneration.mockDefinitionGeneration(MOCK_INDEX_DEFINITION);
  public static final GenerationId MOCK_INDEX_GENERATION_ID =
      MOCK_INDEX_DEFINITION_GENERATION.getGenerationId();
  public static final SynonymMappingId MOCK_SYNONYM_MAPPING_ID =
      SynonymMappingId.from(MOCK_INDEX_GENERATION_ID, MOCK_SYNONYM_MAPPING_DEFINITION_NAME);

  public static final IndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION_CURRENT =
      SearchIndexDefinitionGenerationBuilder.create(
          MOCK_INDEX_DEFINITION, Generation.CURRENT, Collections.emptyList());

  public static final SearchIndexDefinition MOCK_INDEX_DEFINITION_WITH_ANALYZER =
      SearchIndexDefinitionBuilder.builder()
          .indexId(MOCK_INDEX_ID)
          .name(MOCK_INDEX_NAME)
          .database(MOCK_INDEX_DATABASE_NAME)
          .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
          .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
          .mappings(MOCK_INDEX_MAPPINGS)
          .analyzerName(MOCK_SEARCH_ANALYZER_NAME)
          .build();

  public static final IndexStatus MOCK_INDEX_STATUS = IndexStatus.steady();
  public static final Map<String, SynonymStatus> MOCK_SYNONYM_STATUS =
      Map.ofEntries(Map.entry(MOCK_SYNONYM_MAPPING_DEFINITION_NAME, SynonymStatus.READY));
  public static final Map<String, SynonymDetailedStatus> MOCK_SYNONYM_DETAILED_STATUS =
      Map.ofEntries(
          Map.entry(
              MOCK_SYNONYM_MAPPING_DEFINITION_NAME,
              new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty())));
  public static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(mock(IndexMetricValuesSupplier.class))
          .build();

  /** Returns an Index that can be used in tests that require one. */
  public static com.xgen.mongot.index.SearchIndex mockIndex() {
    return mockIndex(MOCK_INDEX_DEFINITION_GENERATION);
  }

  /** Returns an Index with the specified indexId that can be used in tests that require one. */
  public static com.xgen.mongot.index.Index mockIndex(ObjectId indexId) {
    SearchIndexDefinitionGeneration definition =
        IndexGeneration.mockDefinitionGeneration(mockSearchDefinition(indexId));
    return mockIndex(definition);
  }

  /** Returns an Index with the specified indexId that can be used in tests that require one. */
  public static com.xgen.mongot.index.Index mockIndex(GenerationId generationId) {
    SearchIndexDefinition indexDefinition = mockSearchDefinition(generationId.indexId);
    List<OverriddenBaseAnalyzerDefinition> inferredAnalyzers =
        mockAnalyzerDefinitions(indexDefinition);
    SearchIndexDefinitionGeneration definition =
        SearchIndexDefinitionGenerationBuilder.create(
            indexDefinition, generationId.generation, inferredAnalyzers);
    return mockIndex(definition);
  }

  public static com.xgen.mongot.index.SearchIndex mockIndex(SearchIndexDefinition definition) {
    return mockIndex(IndexGeneration.mockDefinitionGeneration(definition));
  }

  public static com.xgen.mongot.index.Index mockIndex(
      SearchIndexDefinition definition, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    var definitionGeneration = IndexGeneration.mockDefinitionGeneration(definition, analyzers);
    return mockIndex(definitionGeneration);
  }

  /**
   * Returns an Index that can be used in tests that require one. The Index returned is also
   * initialized.
   */
  public static com.xgen.mongot.index.SearchIndex mockIndex(
      SearchIndexDefinitionGeneration definitionGeneration) {
    var index = Mockito.mock(com.xgen.mongot.index.SearchIndex.class);
    var definition = definitionGeneration.getIndexDefinition();

    Mockito.lenient().when(index.getDefinition()).thenReturn(definition);
    when(index.isCompatibleWith(any(SearchIndexDefinition.class))).thenReturn(true);
    Mockito.lenient().when(index.asSearchIndex()).thenCallRealMethod();

    // Actually keep track of the status updates to this mock index.
    AtomicReference<IndexStatus> statusContainer = new AtomicReference<>(IndexStatus.steady());

    Answer<Void> setStatus =
        invocation -> {
          statusContainer.set(invocation.getArgument(0));
          return null;
        };

    SynonymRegistry synonymRegistry = mock(SynonymRegistry.class);
    Map<String, SynonymStatus> synonymStatusMap = new HashMap<>();
    Map<String, SynonymDetailedStatus> synonymDetailedStatusMap = new HashMap<>();
    for (var synonymMapping :
        definitionGeneration.getIndexDefinition().getSynonymMap().entrySet()) {
      try {
        SynonymMapping mockSynonymMapping = mock(SynonymMapping.class);
        Mockito.lenient()
            .when(synonymRegistry.get(eq(synonymMapping.getKey())))
            .thenReturn(mockSynonymMapping);
        synonymStatusMap.put(synonymMapping.getKey(), SynonymStatus.READY);
        synonymDetailedStatusMap.put(
            synonymMapping.getKey(),
            new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
      } catch (SynonymMappingException e) {
        Assert.fail("should not throw");
      }
    }

    Mockito.lenient().when(synonymRegistry.getStatuses()).thenReturn(synonymStatusMap);
    Mockito.lenient()
        .when(synonymRegistry.getDetailedStatuses())
        .thenReturn(synonymDetailedStatusMap);
    Mockito.lenient().when(index.getSynonymRegistry()).thenReturn(synonymRegistry);

    Mockito.lenient().doAnswer(setStatus).when(index).setStatus(any());
    Mockito.lenient().when(index.getStatus()).then(ignored -> statusContainer.get());

    return index;
  }

  public static com.xgen.mongot.index.InitializedSearchIndex mockInitializedIndex(
      com.xgen.mongot.index.IndexGeneration indexGeneration) {
    return mockInitializedIndex(
        indexGeneration.getIndex().asSearchIndex(), indexGeneration.getGenerationId());
  }

  /** Returns an InitializedIndex that can be used in tests that require one. */
  public static com.xgen.mongot.index.InitializedSearchIndex mockInitializedIndex(
      com.xgen.mongot.index.SearchIndex index, GenerationId generationId) {
    var initializedIndex = mock(InitializedSearchIndex.class);
    Mockito.lenient().when(initializedIndex.asSearchIndex()).thenCallRealMethod();
    Mockito.lenient().when(initializedIndex.getGenerationId()).thenReturn(generationId);
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
    Mockito.lenient()
        .when(initializedIndex.getMetrics())
        .thenAnswer(invocation -> indexMetricsUpdater.getMetrics());
    Mockito.lenient()
        .when(initializedIndex.getMetricsUpdater())
        .then(
            invocationOnMock -> {
              if (initializedIndex.isClosed()) {
                throw new IllegalStateException("cannot call getMetricsUpdater() after to close()");
              }
              return indexMetricsUpdater;
            });
    Mockito.lenient()
        .when(initializedIndex.getIndexSize())
        .thenAnswer(invocation -> indexMetricsUpdater.getIndexSize());

    com.xgen.mongot.index.SearchIndexReader indexReader = mockIndexReader();
    Mockito.lenient().when(initializedIndex.getReader()).thenReturn(indexReader);

    com.xgen.mongot.index.IndexWriter indexWriter = IndexWriter.mockIndexWriter();
    Mockito.lenient().when(indexWriter.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
    Mockito.lenient().when(initializedIndex.getWriter()).thenReturn(indexWriter);

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
        .when(initializedIndex.isCompatibleWith(any(SearchIndexDefinition.class)))
        .thenReturn(true);
    Mockito.lenient().doReturn(index.isClosed()).when(initializedIndex).isClosed();
    return initializedIndex;
  }

  public static com.xgen.mongot.index.InitializedSearchIndex mockInitializedIndex(
      SearchIndexDefinitionGeneration definitionGeneration) {
    return mockInitializedIndex(
        new com.xgen.mongot.index.IndexGeneration(
            mockIndex(definitionGeneration), definitionGeneration));
  }

  /** With a certain number of batches returned for searches over the index. */
  public static InitializedIndex mockInitializedIndex(
      com.xgen.mongot.index.IndexGeneration indexGeneration, int numFullBatches) {
    var index = mockInitializedIndex(indexGeneration);
    com.xgen.mongot.index.SearchIndexReader indexReader = mockIndexReader(numFullBatches);
    Mockito.lenient().when(index.getReader()).thenReturn(indexReader);
    return index;
  }

  /** With a certain number of batches returned for searches over the index. */
  public static InitializedIndex mockInitializedIndexWithMetaProducer(
      com.xgen.mongot.index.IndexGeneration indexGeneration,
      int numFullBatches,
      int numMetaBatches) {
    var index = mockInitializedIndex(indexGeneration);
    var indexReader =
        mockIndexReaderWithMetaProducer(Optional.of(numFullBatches), Optional.of(numMetaBatches));
    Mockito.lenient().when(index.getReader()).thenReturn(indexReader);
    return index;
  }

  /** Returns a multi partition Index that can be used in tests that require one. */
  public static com.xgen.mongot.index.SearchIndex mockMultiPartitionIndex() {
    return mockIndex(MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION);
  }

  /** Creates a mock index definition. */
  public static SearchIndexDefinition mockSearchDefinition(ObjectId indexId) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .mappings(MOCK_INDEX_MAPPINGS)
        .synonyms(MOCK_SYNONYM_MAPPING_DEFINITIONS)
        .build();
  }

  /** Creates a mock auto embedding vector search index definition. */
  public static VectorIndexDefinition mockAutoEmbeddingVectorSearchDefinition(ObjectId indexId) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withTextField("")
        .build();
  }

  public static SearchIndexDefinitionBuilder mockFacetDefinitionBuilder(boolean dynamic) {
    return mockDefinitionBuilder(MOCK_FACET_INDEX_MAPPINGS.dynamic(dynamic).build());
  }

  /** Creates a mock index definition builder. */
  public static SearchIndexDefinitionBuilder mockDefinitionBuilder() {
    return mockDefinitionBuilder(MOCK_INDEX_MAPPINGS);
  }

  public static SearchIndexDefinitionBuilder mockDefinitionBuilder(
      DocumentFieldDefinition mappings) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .mappings(mappings)
        .synonyms(MOCK_SYNONYM_MAPPING_DEFINITIONS);
  }

  /** Produce mock analyzer definitions for this index (by the required names). */
  public static List<OverriddenBaseAnalyzerDefinition> mockAnalyzerDefinitions(
      SearchIndexDefinition definition) {
    return definition.getNonStockAnalyzerNames().stream()
        .map(SearchIndex::mockAnalyzerDefinition)
        .collect(Collectors.toList());
  }

  /** create an overridden analyzer definition. */
  public static OverriddenBaseAnalyzerDefinition mockAnalyzerDefinition(String analyzerName) {
    return OverriddenBaseAnalyzerDefinitionBuilder.builder()
        .baseAnalyzerName("lucene.standard")
        .name(analyzerName)
        .build();
  }

  /** create an metricsFactory for the Mock Index class. */
  public static PerIndexMetricsFactory mockMetricsFactory() {
    return new PerIndexMetricsFactory(
        IndexMetricsUpdater.NAMESPACE,
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        GenerationIdBuilder.create());
  }

  /** create a IndexMetricsUpdater.IndexingMetricsUpdater for the Mock Index class. */
  public static IndexMetricsUpdater.IndexingMetricsUpdater mockIndexingMetricsUpdater(
      IndexDefinition.Type type) {
    return new IndexMetricsUpdater.IndexingMetricsUpdater(
        SearchIndex.mockMetricsFactory()
            .childMetricsFactory(IndexMetricsUpdater.IndexingMetricsUpdater.NAMESPACE),
        type);
  }

  public static IndexMetricsUpdater.QueryingMetricsUpdater mockQueryMetricsUpdater(
      IndexDefinition.Type type) {
    return new IndexMetricsUpdater.QueryingMetricsUpdater(
        SearchIndex.mockMetricsFactory()
            .childMetricsFactory(IndexMetricsUpdater.QueryingMetricsUpdater.NAMESPACE),
        type == IndexDefinition.Type.VECTOR_SEARCH
            ? IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH
            : IndexTypeData.IndexTypeTag.TAG_SEARCH,
        type == IndexDefinition.Type.VECTOR_SEARCH
            ? VectorIndexCapabilities.CURRENT_FEATURE_VERSION
            : SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
        true);
  }

  /** create a IndexMetricsUpdater.ReplicationMetricsUpdater for the Mock Index class. */
  public static IndexMetricsUpdater.ReplicationMetricsUpdater mockReplicationMetricsUpdater(
      IndexDefinition.Type type) {
    IndexDefinition fakeDefinition;
    if (type == IndexDefinition.Type.VECTOR_SEARCH) {
      fakeDefinition = mockAutoEmbeddingVectorSearchDefinition(new ObjectId());
    } else {
      fakeDefinition = mockSearchDefinition(new ObjectId());
    }
    return new IndexMetricsUpdater.ReplicationMetricsUpdater(
        SearchIndex.mockMetricsFactory()
            .childMetricsFactory(IndexMetricsUpdater.ReplicationMetricsUpdater.NAMESPACE),
        fakeDefinition);
  }
}
