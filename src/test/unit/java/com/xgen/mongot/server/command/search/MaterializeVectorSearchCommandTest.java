package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.search.definition.request.MaterializeVectorSearchCommandDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class MaterializeVectorSearchCommandTest {

  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "plot_index";
  private static final FieldPath PATH = FieldPath.parse("plot");
  private static final String VIEW_NAME = "movies_view";
  private static final String QUERY_TEXT = "espionage thriller set in Berlin";
  private static final String MODEL = "voyage-3-large";
  private static final String OTHER_MODEL = "voyage-3-lite";
  // A model family whose members accept each other's query vectors, and one outside it.
  private static final String FAMILY_INDEX_MODEL = "voyage-4-lite";
  private static final String FAMILY_QUERY_MODEL = "voyage-4";
  private static final String UNRELATED_MODEL = "voyage-code-3";
  private static final int NUM_CANDIDATES = 150;
  private static final int LIMIT = 10;
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      metadata(/* materializeEnabled= */ true);

  /** Metadata with the command's rollout flag set; the only dynamic flag these tests care about. */
  private static SearchCommandsRegister.BootstrapperMetadata metadata(boolean materializeEnabled) {
    return new SearchCommandsRegister.BootstrapperMetadata(
        "testVersion",
        "localhost",
        () -> MongoDbServerInfo.EMPTY,
        FeatureFlags.getDefault(),
        new DynamicFeatureFlagRegistry(
            Optional.of(
                List.of(
                    new DynamicFeatureFlagConfig(
                        DynamicFeatureFlags.AUTO_EMBEDDING_MATERIALIZE_VECTOR_SEARCH.getName(),
                        materializeEnabled
                            ? DynamicFeatureFlagConfig.Phase.ENABLED
                            : DynamicFeatureFlagConfig.Phase.DISABLED,
                        List.of(),
                        List.of(),
                        0,
                        DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER))),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId())));
  }

  private IndexCatalog indexCatalog;
  private EmbeddingServiceManager embeddingService;
  private MaterializeVectorSearchCommand.Factory factory;

  @Before
  public void setUp() {
    registerModel(MODEL);
    registerModel(OTHER_MODEL);
    registerModel(FAMILY_INDEX_MODEL);
    registerModel(FAMILY_QUERY_MODEL);
    registerModel(UNRELATED_MODEL);

    this.indexCatalog = mock(IndexCatalog.class);
    this.embeddingService = mock(EmbeddingServiceManager.class);
    doReturn(List.of(new VectorOrError(QUERY_VECTOR)))
        .when(this.embeddingService)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));

    this.factory =
        new MaterializeVectorSearchCommand.Factory(
            this.indexCatalog,
            Suppliers.ofInstance(this.embeddingService),
            BOOTSTRAPPER_METADATA,
            metricsFactory());
  }

  @Test
  public void textQuery_embedsOnceAndReturnsRewrittenCommandWithModel() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(1);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    assertThat(response.getString("model").getValue()).isEqualTo(MODEL);

    BsonDocument command = response.getDocument("command");
    // The rewritten command is a vectorSearch wire command, addressed by collection name.
    assertThat(command.getFirstKey()).isEqualTo("vectorSearch");
    assertThat(command.getString("vectorSearch").getValue()).isEqualTo(COLLECTION_NAME);
    assertThat(command.getString("$db").getValue()).isEqualTo(DATABASE_NAME);
    assertThat(command.getString("index").getValue()).isEqualTo(INDEX_NAME);
    // Text replaced by the embedded vector; the path stays as the user wrote it.
    assertThat(command.containsKey("queryVector")).isTrue();
    assertThat(command.containsKey("query")).isFalse();
    assertThat(command.getString("path").getValue()).isEqualTo(PATH.toString());
    // Each shard resolves its own UUID; the router never learns one.
    assertThat(command.containsKey("collectionUUID")).isFalse();
    assertThat(command.containsKey("viewName")).isFalse();
    assertThat(command.containsKey("deadlineTimestampMs")).isFalse();
  }

  @Test
  public void textQuery_forwardsViewNameAndDeadline() throws Exception {
    catalogHolds(
        generation(
            autoEmbedDefinition(COLLECTION_UUID, MODEL)
                .view(ViewDefinition.existing(VIEW_NAME, List.of()))
                .build(),
            /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.of(VIEW_NAME), Optional.of(1234L));

    verifyEmbedCalls(1);
    BsonDocument command = response.getDocument("command");
    assertThat(command.getString("viewName").getValue()).isEqualTo(VIEW_NAME);
    assertThat(command.getInt64("deadlineTimestampMs").getValue()).isEqualTo(1234L);
  }

  @Test
  public void textQuery_keepsUserFacingPathAndFilter() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    VectorSearchFilter filter = costFilter();

    BsonDocument response = run(textQueryWithFilter(filter), Optional.empty(), Optional.empty());

    // Shards rewrite the path and filter to their index's internal fields themselves; the command
    // handed back to the router must not carry those internal names.
    BsonDocument command = response.getDocument("command");
    assertThat(command.getString("path").getValue()).isEqualTo(PATH.toString());
    assertThat(command.getDocument("filter")).isEqualTo(filter.toBson().getDocument("filter"));
  }

  @Test
  public void compatibleQueryModel_embedsWithTheQueryModelAndReportsIt() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, FAMILY_INDEX_MODEL, /* queryable= */ true));

    BsonDocument response =
        run(textQueryWithModel(FAMILY_QUERY_MODEL), Optional.empty(), Optional.empty());

    // Same rule as a shard: a query model that is compatible with the index model wins.
    ArgumentCaptor<EmbeddingModelConfig> modelUsed =
        ArgumentCaptor.forClass(EmbeddingModelConfig.class);
    verify(this.embeddingService)
        .embed(
            Mockito.anyList(),
            modelUsed.capture(),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));
    assertThat(modelUsed.getValue().name()).isEqualTo(FAMILY_QUERY_MODEL);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    // The reported model is the one that produced the vector, which is what shards compare against.
    assertThat(response.getString("model").getValue()).isEqualTo(FAMILY_QUERY_MODEL);
    assertThat(response.getDocument("command").containsKey("query")).isFalse();
  }

  @Test
  public void incompatibleQueryModel_isUserError() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, FAMILY_INDEX_MODEL, /* queryable= */ true));

    BsonDocument response =
        run(textQueryWithModel(UNRELATED_MODEL), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getString("errmsg").getValue()).contains("not compatible");
    assertThat(response.containsKey("errorLabels")).isFalse();
  }

  @Test
  public void unknownQueryModel_isUserError() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, FAMILY_INDEX_MODEL, /* queryable= */ true));

    BsonDocument response =
        run(textQueryWithModel("no-such-model"), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getString("errmsg").getValue()).contains("not allowed");
  }

  @Test
  public void queryVectorAlreadyPresent_doesNotEmbed() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));

    BsonDocument response = run(vectorQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    assertThat(response.getDocument("command").containsKey("queryVector")).isTrue();
    // No embedding ran, so there is no model to report.
    assertThat(response.containsKey("model")).isFalse();
  }

  @Test
  public void unknownIndex_returnsIndexNotFoundCode() throws Exception {
    catalogHolds();

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getInt32("code").getValue()).isEqualTo(Errors.INDEX_NOT_FOUND.code);
    assertThat(response.getString("codeName").getValue()).isEqualTo(Errors.INDEX_NOT_FOUND.name);
    assertThat(response.getString("errmsg").getValue())
        .isEqualTo("Index 'plot_index' not found for collection 'testDb.testCollection'");
  }

  @Test
  public void collectionNotFoundGeneration_stillEmbeds() throws Exception {
    // A mongot whose shard holds no data of the collection reports DOES_NOT_EXIST with reason
    // COLLECTION_NOT_FOUND, locally indistinguishable from a collection dropped everywhere, and
    // the router may land the materialize call on exactly such a mongot. The definition is all
    // embedding needs, so the generation stays a candidate.
    catalogHolds(droppedGeneration(autoEmbedDefinition(COLLECTION_UUID, MODEL).build()));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(1);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    assertThat(response.getString("model").getValue()).isEqualTo(MODEL);
  }

  @Test
  public void droppedNewerGeneration_winsTheTieBreakLikeAnyOther() throws Exception {
    // Local status no longer narrows candidates, so id order alone breaks ties, same rule as the
    // drop-and-recreate case, where the newest generation is the recreated collection's.
    ObjectId older = objectIdWithTime(1_000);
    ObjectId newer = objectIdWithTime(2_000);
    catalogHolds(
        generation(
            autoEmbedDefinition(UUID.randomUUID(), MODEL).indexId(older).build(),
            /* queryable= */ true),
        droppedGeneration(
            autoEmbedDefinition(UUID.randomUUID(), OTHER_MODEL).indexId(newer).build()));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(1);
    assertThat(response.getString("model").getValue()).isEqualTo(OTHER_MODEL);
  }

  @Test
  public void invalidQueryWrappedInRuntimeException_isUserError() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    doThrow(new RuntimeException(new InvalidQueryException("text is not embeddable")))
        .when(this.embeddingService)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getString("errmsg").getValue()).isEqualTo("text is not embeddable");
    assertThat(response.containsKey("errorLabels")).isFalse();
  }

  @Test
  public void indexOnOtherCollectionOrDatabase_isNotMatched() throws Exception {
    catalogHolds(
        generation(
            VectorIndexDefinitionBuilder.builder()
                .database(DATABASE_NAME)
                .lastObservedCollectionName("otherCollection")
                .name(INDEX_NAME)
                .collectionUuid(UUID.randomUUID())
                .withAutoEmbedField(PATH.toString(), MODEL)
                .build(),
            /* queryable= */ true),
        generation(
            VectorIndexDefinitionBuilder.builder()
                .database("otherDb")
                .lastObservedCollectionName(COLLECTION_NAME)
                .name(INDEX_NAME)
                .collectionUuid(UUID.randomUUID())
                .withAutoEmbedField(PATH.toString(), MODEL)
                .build(),
            /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getInt32("code").getValue()).isEqualTo(Errors.INDEX_NOT_FOUND.code);
  }

  @Test
  public void requestNamesView_indexWithoutThatViewIsNotMatched() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.of("someView"), Optional.empty());

    assertThat(response.getInt32("code").getValue()).isEqualTo(Errors.INDEX_NOT_FOUND.code);
  }

  @Test
  public void textQueryOnNonAutoEmbeddingIndex_isUserError() throws Exception {
    catalogHolds(
        generation(
            VectorIndexDefinitionBuilder.builder()
                .database(DATABASE_NAME)
                .lastObservedCollectionName(COLLECTION_NAME)
                .name(INDEX_NAME)
                .collectionUuid(COLLECTION_UUID)
                .withDotProductVectorField(PATH.toString(), 3)
                .build(),
            /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.containsKey("code")).isFalse();
    assertThat(response.getString("errmsg").getValue()).contains("queryVector must be present");
  }

  @Test
  public void localIndexNotQueryable_stillEmbeds() throws Exception {
    // Embedding needs the definition and the credentials, not a queryable local index. A mongot
    // that is still building its copy must be able to embed for the shards that are ready.
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ false));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(1);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    assertThat(response.getString("model").getValue()).isEqualTo(MODEL);
  }

  @Test
  public void collectionRecreatedUnderSameName_usesNewestIndex() throws Exception {
    // Two generations match by name: the dropped collection's index (older id, possibly still
    // queryable until cleaned up) and the recreated collection's (newer id, possibly still
    // building). Shards will search the recreated collection, so its model must be used.
    var older = objectIdWithTime(1_000);
    var newer = objectIdWithTime(2_000);
    catalogHolds(
        generation(
            autoEmbedDefinition(UUID.randomUUID(), OTHER_MODEL).indexId(older).build(),
            /* queryable= */ true),
        generation(
            autoEmbedDefinition(UUID.randomUUID(), MODEL).indexId(newer).build(),
            /* queryable= */ false));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getString("model").getValue()).isEqualTo(MODEL);
  }

  @Test
  public void collectionRecreatedUnderSameName_choiceDoesNotDependOnCatalogOrder()
      throws Exception {
    var older = objectIdWithTime(1_000);
    var newer = objectIdWithTime(2_000);
    catalogHolds(
        generation(
            autoEmbedDefinition(UUID.randomUUID(), MODEL).indexId(newer).build(),
            /* queryable= */ true),
        generation(
            autoEmbedDefinition(UUID.randomUUID(), OTHER_MODEL).indexId(older).build(),
            /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getString("model").getValue()).isEqualTo(MODEL);
  }

  @Test
  public void indexDefinedWithNamespace_queryWithoutNamespace_isUserError() throws Exception {
    catalogHolds(
        generation(
            autoEmbedDefinition(COLLECTION_UUID, MODEL).indexNamespace("tenant").build(),
            /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    verifyEmbedCalls(0);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getString("errmsg").getValue())
        .contains("index is defined with indexNamespace");
  }

  @Test
  public void transientEmbeddingFailure_isLabeledRetryable() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    doThrow(
            new EmbeddingProviderTransientException(
                "Embedding service not initialized.",
                EmbeddingProviderTransientException.Reason.SERVICE_NOT_INITIALIZED))
        .when(this.embeddingService)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.getString("errmsg").getValue()).contains("not initialized");
    assertThat(
            response.getArray("errorLabels").getValues().stream()
                .map(BsonValue::asString)
                .map(v -> v.getValue())
                .toList())
        .containsExactly("RetryableError");
  }

  @Test
  public void nonTransientEmbeddingFailure_isPlainError() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    doThrow(new EmbeddingProviderNonTransientException("model rejected the input"))
        .when(this.embeddingService)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.containsKey("errorLabels")).isFalse();
  }

  @Test
  public void embeddingServiceNotInitialized_isRetryable() throws Exception {
    // No embedding configuration has reached this node yet (for example right after a restart).
    // Another node may already have it, so the caller should try elsewhere.
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    MaterializeVectorSearchCommand.Factory uninitialized =
        new MaterializeVectorSearchCommand.Factory(
            this.indexCatalog, () -> null, BOOTSTRAPPER_METADATA, metricsFactory());

    BsonDocument response =
        uninitialized
            .create(
                MaterializeVectorSearchCommandDefinition.create(
                    textQuery(),
                    DATABASE_NAME,
                    COLLECTION_NAME,
                    Optional.empty(),
                    Optional.empty()))
            .run();

    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(
            response.getArray("errorLabels").getValues().stream()
                .map(v -> v.asString().getValue())
                .toList())
        .containsExactly("RetryableError");
  }

  @Test
  public void modelNotRegisteredOnInitializedNode_isNotRetryable() throws Exception {
    // Configuration has arrived and does not include the index's model: every node shares that
    // configuration, so retrying elsewhere would not help.
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    doThrow(
            new EmbeddingProviderNonTransientException(
                "CanonicalModel: " + MODEL + " not registered yet",
                EmbeddingProviderNonTransientException.Reason.MODEL_NOT_REGISTERED))
        .when(this.embeddingService)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.empty());

    assertThat(response.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(response.containsKey("errorLabels")).isFalse();
  }

  @Test
  public void rolloutFlagOff_answersCommandNotFoundWithoutEmbedding() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    MaterializeVectorSearchCommand.Factory gated =
        new MaterializeVectorSearchCommand.Factory(
            this.indexCatalog,
            Suppliers.ofInstance(this.embeddingService),
            metadata(/* materializeEnabled= */ false),
            metricsFactory());

    BsonDocument response =
        gated
            .create(
                MaterializeVectorSearchCommandDefinition.create(
                    textQuery(),
                    DATABASE_NAME,
                    COLLECTION_NAME,
                    Optional.empty(),
                    Optional.empty()))
            .run();

    // Indistinguishable from an old mongot that lacks the command, so the router's fallback
    // applies.
    verifyEmbedCalls(0);
    assertThat(response.getInt32("code").getValue()).isEqualTo(Errors.COMMAND_NOT_FOUND.code);
    assertThat(response.getString("errmsg").getValue())
        .isEqualTo("no command registered for materializeVectorSearch");
  }

  @Test
  public void expiredDeadline_isIgnoredWhileQueryTimeoutFlagIsOff() throws Exception {
    // Mirrors vectorSearch: deadlines are enforced only behind the query-timeout flag, which the
    // default registry leaves off.
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));

    BsonDocument response = run(textQuery(), Optional.empty(), Optional.of(1L));

    verifyEmbedCalls(1);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
  }

  @Test
  public void isDeadlineExpired() {
    assertThat(MaterializeVectorSearchCommand.isDeadlineExpired(Optional.empty(), 1_000L))
        .isFalse();
    assertThat(MaterializeVectorSearchCommand.isDeadlineExpired(Optional.of(2_000L), 1_000L))
        .isFalse();
    assertThat(MaterializeVectorSearchCommand.isDeadlineExpired(Optional.of(1_000L), 1_000L))
        .isTrue();
    assertThat(MaterializeVectorSearchCommand.isDeadlineExpired(Optional.of(500L), 1_000L))
        .isTrue();
  }

  @Test
  public void factory_parsesWireCommand() throws Exception {
    catalogHolds(autoEmbedGeneration(COLLECTION_UUID, MODEL, /* queryable= */ true));
    BsonDocument wireCommand =
        new BsonDocument()
            .append(MaterializeVectorSearchCommandDefinition.NAME, new BsonString(COLLECTION_NAME))
            .append("$db", new BsonString(DATABASE_NAME))
            .append("index", new BsonString(INDEX_NAME))
            .append("path", new BsonString(PATH.toString()))
            .append("query", new BsonDocument("text", new BsonString(QUERY_TEXT)))
            .append("numCandidates", new BsonInt32(NUM_CANDIDATES))
            .append("limit", new BsonInt32(LIMIT))
            .append("deadlineTimestampMs", new BsonInt64(99L))
            // Fields this mongot does not know must not break parsing (forward compatibility).
            .append("someFutureField", new BsonString("ignored"));

    Command command = this.factory.create(wireCommand);
    BsonDocument response = command.run();

    assertThat(command.name()).isEqualTo(MaterializeVectorSearchCommandDefinition.NAME);
    assertThat(response.getInt32("ok").getValue()).isEqualTo(1);
    assertThat(response.getDocument("command").getInt64("deadlineTimestampMs").getValue())
        .isEqualTo(99L);
    verifyEmbedCalls(1);
  }

  @Test
  public void command_isAsyncAndExemptFromLoadShedding() {
    Command command =
        this.factory.create(
            MaterializeVectorSearchCommandDefinition.create(
                textQuery(), DATABASE_NAME, COLLECTION_NAME, Optional.empty(), Optional.empty()));

    assertThat(command.getExecutionPolicy()).isEqualTo(Command.ExecutionPolicy.ASYNC);
    assertThat(command.maybeLoadShed()).isFalse();
    assertThat(command.dependOnCursors()).isFalse();
  }

  // ---- helpers ----

  private BsonDocument run(
      VectorSearchQuery query, Optional<String> viewName, Optional<Long> deadlineTimestampMs) {
    return this.factory
        .create(
            MaterializeVectorSearchCommandDefinition.create(
                query, DATABASE_NAME, COLLECTION_NAME, viewName, deadlineTimestampMs))
        .run();
  }

  private void catalogHolds(IndexGeneration... generations) {
    when(this.indexCatalog.getIndexes()).thenReturn(List.of(generations));
  }

  private void verifyEmbedCalls(int count) {
    verify(this.embeddingService, count == 0 ? never() : times(count))
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));
  }

  private static VectorSearchQuery textQuery() {
    return VectorQueryBuilder.builder()
        .index(INDEX_NAME)
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .limit(LIMIT)
                .numCandidates(NUM_CANDIDATES)
                .query(new VectorSearchQueryInput.Text(QUERY_TEXT))
                .path(PATH)
                .build())
        .build();
  }

  private static VectorSearchQuery textQueryWithModel(String model) {
    return VectorQueryBuilder.builder()
        .index(INDEX_NAME)
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .limit(LIMIT)
                .numCandidates(NUM_CANDIDATES)
                .query(new VectorSearchQueryInput.Text(QUERY_TEXT, Optional.of(model)))
                .path(PATH)
                .build())
        .build();
  }

  private static VectorSearchQuery vectorQuery() {
    return VectorQueryBuilder.builder()
        .index(INDEX_NAME)
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .limit(LIMIT)
                .numCandidates(NUM_CANDIDATES)
                .queryVector(QUERY_VECTOR)
                .path(PATH)
                .build())
        .build();
  }

  private static VectorSearchQuery textQueryWithFilter(VectorSearchFilter filter) {
    return VectorQueryBuilder.builder()
        .index(INDEX_NAME)
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .limit(LIMIT)
                .numCandidates(NUM_CANDIDATES)
                .query(new VectorSearchQueryInput.Text(QUERY_TEXT))
                .path(PATH)
                .filter(filter)
                .build())
        .build();
  }

  private static VectorSearchFilter costFilter() throws BsonParseException {
    return new VectorSearchFilter.ClauseFilter(
        ClauseBuilder.simpleClause()
            .path(FieldPath.parse("cost"))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(50)).build())
            .build());
  }

  private static VectorIndexDefinitionBuilder autoEmbedDefinition(
      UUID collectionUuid, String model) {
    return VectorIndexDefinitionBuilder.builder()
        .database(DATABASE_NAME)
        .lastObservedCollectionName(COLLECTION_NAME)
        .name(INDEX_NAME)
        .collectionUuid(collectionUuid)
        .withAutoEmbedField(PATH.toString(), model);
  }

  private static IndexGeneration autoEmbedGeneration(
      UUID collectionUuid, String model, boolean queryable) throws IndexUnavailableException {
    return generation(autoEmbedDefinition(collectionUuid, model).build(), queryable);
  }

  private static IndexGeneration generation(VectorIndexDefinition definition, boolean queryable)
      throws IndexUnavailableException {
    Index index = mock(Index.class);
    when(index.getStatus())
        .thenReturn(queryable ? IndexStatus.steady() : IndexStatus.initialSync());
    if (!queryable) {
      doThrow(new IndexUnavailableException("index is building"))
          .when(index)
          .throwIfUnavailableForQuerying();
    }
    IndexGeneration generation = mock(IndexGeneration.class);
    when(generation.getDefinition()).thenReturn(definition);
    when(generation.getIndex()).thenReturn(index);
    return generation;
  }

  /** A generation whose collection was dropped: still in the catalog, but on its way out. */
  private static IndexGeneration droppedGeneration(VectorIndexDefinition definition) {
    Index index = mock(Index.class);
    when(index.getStatus())
        .thenReturn(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    IndexGeneration generation = mock(IndexGeneration.class);
    when(generation.getDefinition()).thenReturn(definition);
    when(generation.getIndex()).thenReturn(index);
    return generation;
  }

  private static ObjectId objectIdWithTime(int epochSeconds) {
    return new ObjectId(epochSeconds, 0);
  }

  private static MetricsFactory metricsFactory() {
    return new MetricsFactory("mockNamespace", new SimpleMeterRegistry());
  }

  private static void registerModel(String model) {
    EmbeddingModelCatalog.registerModelConfig(
        model,
        EmbeddingModelConfig.create(
            model,
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
                Optional.empty(),
                false,
                Optional.empty())));
    EmbeddingModelCatalog.registerCompatibleModels(model, Set.of());
  }
}
