package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.MOCK_SEARCH_CURSOR_ID;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatch;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.server.command.search.definition.request.SearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Drives the real {@link SearchCommand} path for a $search text query on an auto-embedding index
 * served offline (mongod unreachable, view unresolved): the reconstructed derived index is
 * registered in the initialized-index catalog under the raw generation id, so the command resolves
 * it like any other index, reports STALE, and embeds/remaps the query using the mapping persisted
 * with the commit rather than the (unresolvable) materialized view.
 */
public class SearchCommandOfflineServeTest {

  private static final String MV_DATABASE_NAME = "acme___mdb_internal_search";
  private static final UUID MV_COLLECTION_UUID = UUID.randomUUID();
  private static final Vector QUERY_VECTOR =
      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE);
  private static final Map<FieldPath, FieldPath> FIELDS_MAPPING =
      Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content"));

  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());

  @Before
  public void clearCatalogBeforeTest() {
    EmbeddingModelCatalog.clear();
  }

  @After
  public void clearCatalogAfterTest() {
    EmbeddingModelCatalog.clear();
  }

  @Test
  public void run_textQuery_servedOfflineWithPersistedMappingAndStaleStatus() throws Exception {
    SearchIndexDefinition rawDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    SearchIndexDefinitionGeneration rawGen =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(rawDef, Collections.emptyList()),
            Generation.CURRENT);
    MaterializedViewSchemaMetadata schema = new MaterializedViewSchemaMetadata(1L, FIELDS_MAPPING);
    SearchIndexDefinition derivedDef =
        getDerivedSearchIndexDefinition(rawDef, MV_DATABASE_NAME, MV_COLLECTION_UUID, schema);

    // The reconstructed derived index: STALE as of the last committed optime, exposing the derived
    // (non-auto-embedding) definition, as offline reconstruction produces.
    InitializedSearchIndex reconstructed = mock(InitializedSearchIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", new BsonTimestamp(7, 1)));
    when(reconstructed.getDefinition()).thenReturn(derivedDef);
    IndexGeneration unresolvedGeneration =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(rawGen, reconstructed, FIELDS_MAPPING);

    // The offline shape: the unresolved generation in the index catalog, and the reconstructed
    // derived index registered in the initialized-index catalog under the raw generation id.
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndex(any(), any(), any(), any()))
        .thenReturn(Optional.of(unresolvedGeneration));
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    when(initializedIndexCatalog.getIndex(any())).thenReturn(Optional.of(reconstructed));

    MongotCursorManager cursorManager = stubbedCursorManager();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(
                new MetricsFactory("mockNamespace", new SimpleMeterRegistry())),
            cursorManager,
            indexCatalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(),
            SearchCommandDefinitionBuilder.builder()
                .db(rawDef.getDatabase())
                .collectionName(rawDef.getLastObservedCollectionName())
                .collectionUuid(rawDef.getCollectionUuid())
                .query(
                    new BsonDocument()
                        .append("index", new BsonString(rawDef.getName()))
                        .append(
                            "vectorSearch",
                            new BsonDocument()
                                .append("path", new BsonString("embedding"))
                                .append("limit", new BsonInt32(5))
                                .append("numCandidates", new BsonInt32(10))
                                .append("query", new BsonString("find me offline"))))
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR)));

    BsonDocument result = command.run();

    assertThat(result.getInt32("ok").getValue()).isEqualTo(1);
    // Status reporting: the unresolved generation delegates to the reconstructed index -> STALE.
    assertThat(unresolvedGeneration.getIndex().getStatus().getStatusCode())
        .isEqualTo(IndexStatus.StatusCode.STALE);

    // The query handed to the cursor manager was embedded and retargeted using the mapping
    // persisted with the commit: text replaced by the embedded vector, path remapped to the
    // materialized-view path the derived index stores the embedding under.
    ArgumentCaptor<SearchQuery> queryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
    verify(cursorManager)
        .newCursor(any(), any(), any(), any(), queryCaptor.capture(), any(), any(), any());
    OperatorQuery ranQuery = (OperatorQuery) queryCaptor.getValue();
    VectorSearchCriteria criteria = ((VectorSearchOperator) ranQuery.operator()).criteria();
    assertThat(criteria.path()).isEqualTo(FieldPath.parse("_autoEmbed.content"));
    assertThat(criteria.queryVector()).hasValue(QUERY_VECTOR);
    assertThat(criteria.query()).isEmpty();
  }

  private static MongotCursorManager stubbedCursorManager() throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    MongotCursorBatch batch = mockInitialMongotCursorBatch();
    when(cursorManager.newCursor(any(), any(), any(), any(), any(Query.class), any(), any(), any()))
        .thenReturn(
            new SearchCursorInfo(
                MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000))));
    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .thenReturn(
            new MongotCursorResultInfo(
                batch.getCursorExpected().getCursorId() == MongotCursorResult.EXHAUSTED_CURSOR_ID,
                batch.getCursorExpected().getBatch(),
                Optional.empty(),
                batch.getCursorExpected().getNamespace()));
    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn((timer) -> {});
    return cursorManager;
  }

  private static Supplier<EmbeddingServiceManager> mockEmbeddingServiceManager(
      VectorOrError vectorOrError) {
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty())));
    EmbeddingServiceManager manager = mock(EmbeddingServiceManager.class);
    doReturn(List.of(vectorOrError))
        .when(manager)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));
    return Suppliers.ofInstance(manager);
  }
}
