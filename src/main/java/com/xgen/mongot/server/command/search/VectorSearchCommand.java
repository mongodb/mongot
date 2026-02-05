package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Type.VECTOR_SEARCH;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.NamespaceBuilder;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.EmptyExplainInformation;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.UserFacingException;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.mongot.util.retry.ActionRetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs $vectorSearch over an vector index. */
public class VectorSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(VectorSearchCommand.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final MetricsFactory metricsFactory;
  private final VectorSearchCommandDefinition definition;
  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Metrics metrics;

  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;
  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

  VectorSearchCommand(
      VectorSearchCommandDefinition definition,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      Metrics metrics) {
    this.metricsFactory = metrics.metricsFactory;
    this.definition = definition;
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.metadata = metadata;
    this.metrics = metrics;
    this.searchEnvoyMetadata = Optional.empty();
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
  }

  @Override
  public String name() {
    return VectorSearchCommandDefinition.NAME;
  }

  @Override
  public void handleSearchEnvoyMetadata(SearchEnvoyMetadata searchEnvoyMetadata) {
    this.searchEnvoyMetadata = Optional.of(searchEnvoyMetadata);
  }

  private static final MongoDbVersion VECTOR_STORED_SOURCE_MIN_VERSION_MONGODB =
      new MongoDbVersion(8, 2, 0);

  private static boolean isVectorStoredSourceVersionOk(MongoDbVersion mongoDbVersion) {
    int cmp = mongoDbVersion.compareTo(VECTOR_STORED_SOURCE_MIN_VERSION_MONGODB);
    return cmp >= 0;
  }

  static void checkSupportForVectorStoredSource(
      SearchCommandsRegister.BootstrapperMetadata metadata,
      VectorSearchQuery query,
      Optional<InitializedIndex> index)
      throws InvalidQueryException {
    if (!query.userReturnStoredSource()) {
      return; // Vector Stored Source not requested by user.
    }

    if (index.isEmpty()) {
      return; // Let missing index information be handled somewhere else, if needed.
    }

    Optional<MongoDbVersion> mongoDbVersionOptional =
        metadata.mongoDbServerInfoProvider().getCachedMongoDbServerInfo().mongoDbVersion();

    if (mongoDbVersionOptional.isEmpty()) {
      if (!metadata.featureFlags().isEnabled(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE)) {
        return; // Vector Stored Source requested by mistake on a legacy query (ignored).
      }
      throw new InvalidQueryException(
          "The returnStoredSource setting requires MongoDB server version "
              + VECTOR_STORED_SOURCE_MIN_VERSION_MONGODB.toVersionString()
              + " or higher for vector indexes.");
      // Vector Stored Source assumed to be unsupported by unknown version of MongoDB.
    }

    if (!isVectorStoredSourceVersionOk(mongoDbVersionOptional.get())) {
      if (!metadata.featureFlags().isEnabled(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE)) {
        return; // Vector Stored Source requested by mistake on a legacy query (ignored).
      }
      throw new InvalidQueryException(
          "The returnStoredSource setting requires MongoDB server version "
              + VECTOR_STORED_SOURCE_MIN_VERSION_MONGODB.toVersionString()
              + " or higher for vector indexes but got version "
              + mongoDbVersionOptional.get().toVersionString()
              + " instead.");
      // Vector Stored Source unsupported by this version of MongoDB.
    }

    IndexDefinition indexDefinition = index.get().getDefinition();
    StoredSourceDefinition storedSourceDefinition = indexDefinition.getStoredSource();

    if (storedSourceDefinition.isAllExcluded()) { // storedSource: false
      throw new InvalidQueryException(
          "Before using returnStoredSource, index must be created with storedSource defined.");
      // Vector Stored Source requested by user but not defined.
    }

    // Vector Stored Source defined and requested by user, supported by MongoDB, query can proceed.
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", VectorSearchCommandDefinition.NAME)
        .log("Received command");

    @Var Optional<VectorSearchCriteria.Type> queryTypeOptional = Optional.empty();
    try {
      this.metrics.totalCount.increment();
      var query = this.definition.getQuery();
      if (query.userReturnStoredSource()) {
        this.metrics.vectorStoredSourceQueries.increment();
      }

      VectorSearchCriteria criteria = query.criteria();
      if (criteria.queryVector().isPresent()) {
        // AutoEmbedding query should already pass counter updating method since they are all
        // native floats.
        updateBsonVectorQueryCounters(criteria.queryVector().get());
      }
      updateVectorSearchQueryCounters(criteria);
      queryTypeOptional = Optional.of(criteria.getVectorSearchType());
      increaseConcurrentQueriesGauge(criteria.getVectorSearchType());

      // It's possible that right after we pulled the index from the catalog, it got closed. This
      // could happen when index is replaced with a newer generation or got cleared.
      // Querying it would result in ReaderClosedException, so we optimistically retry to
      // get a new version of index from the catalog and query it again. Note: this does not
      // guarantee that re-try will necessarily be successful, the intention is to minimize
      // chances of an error when this rare race condition is hit.
      return ActionRetry.onException(
          () -> {
            var index = getIndexFromCatalog(query);
            try (var unusedExplain =
                    Explain.setup(
                        this.definition.explain().map(ExplainDefinition::verbosity),
                        index.map(idx -> idx.getDefinition().getNumPartitions()));
                var unusedFeatureFlags = DynamicFeatureFlagsMetricsRecorder.setup()) {
              addMetadataIfExplain(query);
              checkSupportForVectorStoredSource(this.metadata, query, index);
              return getSearchResults(query, index);
            }
          },
          ReaderClosedException.class,
          1);

    } catch (BsonParseException | InvalidQueryException e) {
      return handleInvalidQueryException(e);
    } catch (Exception e) {
      // Unwrap RuntimeExceptions to check if they mask a user-facing InvalidQueryException.
      if (e instanceof RuntimeException && e.getCause() instanceof InvalidQueryException) {
        this.metrics.wrappedInvalidQueryExceptions.increment();
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
            "InvalidQueryException wrapped in RuntimeException"
                + " throw InvalidQueryException directly");
        return handleInvalidQueryException((Exception) e.getCause());
      }

      boolean isUserFacing = UserFacingException.isUserFacing(e);
      this.metricsFactory
          .counter(
              "vectorSearchCommandInternalFailures",
              Tags.of(
                  "exceptionName",
                  e.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  isUserFacing
                      ? ErrorType.USER_FACING_ERROR.toString()
                      : ErrorType.INTERNAL_ERROR.toString()))
          .increment();

      if (!isUserFacing) {
        // Mongot internal processing error should be rare, log with a low limit. We still need a
        // limit because the warning log level may cause a flush().
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Failed to process a vectorSearchCommand.");
      }
      return MessageUtils.createErrorBody(e);
    } catch (Throwable e) {
      this.metricsFactory
          .counter(
              "vectorSearchCommandInternalFailures",
              Tags.of("throwableName", e.getClass().getSimpleName()))
          .increment();
      throw e;
    } finally {
      queryTypeOptional.ifPresent(this::decreaseConcurrentQueriesGauge);
    }
  }

  // Utility method to handle invalid query exceptions that are caught explicitly or wrapped.
  private BsonDocument handleInvalidQueryException(Exception e) {
    this.metrics.invalidQueries.increment();
    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
        "vectorSearchCommand query is invalid");
    return MessageUtils.createErrorBody(e);
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }

  @VisibleForTesting
  public Counter getBsonFloatVectorQueries() {
    return this.metrics.bsonFloatVectorQueries;
  }

  @VisibleForTesting
  public Counter getBsonByteVectorQueries() {
    return this.metrics.bsonByteVectorQueries;
  }

  @VisibleForTesting
  public Counter getBsonBitVectorQueries() {
    return this.metrics.bsonBitVectorQueries;
  }

  @VisibleForTesting
  public Counter getVectorStoredSourceQueries() {
    return this.metrics.vectorStoredSourceQueries;
  }

  @VisibleForTesting
  public Counter getExactVectorSearchQueries() {
    return this.metrics.exactVectorSearchQueries;
  }

  @VisibleForTesting
  public Counter getApproximateVectorSearchQueries() {
    return this.metrics.approximateVectorSearchQueries;
  }

  @VisibleForTesting
  public Counter getAutoEmbeddingVectorSearchQueries() {
    return this.metrics.autoEmbeddingVectorSearchQueries;
  }

  @VisibleForTesting
  public Counter getAutoEmbeddingTextQueries() {
    return this.metrics.autoEmbeddingTextQueries;
  }

  @VisibleForTesting
  public Counter getAutoEmbeddingMultiModalQueries() {
    return this.metrics.autoEmbeddingMultiModalQueries;
  }

  private BsonDocument getSearchResults(
      VectorSearchQuery vectorSearchQuery, Optional<InitializedIndex> optionalIndex)
      throws IndexUnavailableException, InvalidQueryException, ReaderClosedException, IOException {
    if (optionalIndex.isEmpty()) {
      // the index does not exist, respond with empty information. The result is not cached,
      // so when this index does get created, we will answer queries correctly.
      return createExhaustedCursorBatch(new BsonArray()).toBson();
    }

    if (isEnvoyMetadataPresent()) {
      LOG.atTrace()
          .addKeyValue("envoyMetadata", this.searchEnvoyMetadata.get())
          .log("Returning empty batch because of envoy metadata");
      if (Explain.isEnabled()) {
        return new EmptyExplainInformation().toBson();
      } else {
        return createExhaustedCursorBatch(new BsonArray()).toBson();
      }
    }

    try (var guard =
        Tracing.simpleSpanGuard("VectorSearchCommand.getSearchResults", Tracing.TOGGLE_OFF)) {
      var timer = Timer.start();
      var commandTimer = Timer.start();
      InitializedIndex index = optionalIndex.get();
      var reader = index.asVectorIndex().getReader();
      var materializedQuery =
          maybeEmbed(
              vectorSearchQuery,
              findEmbeddingModelName(index.getDefinition(), vectorSearchQuery),
              index.getDefinition());
      var results = reader.query(materializedQuery);
      var serializedBatch = createExhaustedCursorBatch(results).toBson();

      var metrics = index.getMetricsUpdater().getQueryingMetricsUpdater();
      var durationNs = timer.stop(metrics.getVectorResultLatencyTimer());
      metrics.recordDynamicFeatureFlagLatencyTimer(durationNs);
      metrics.getVectorCommandCounter().increment();

      // Record command-level latency with index size and quantization tags
      recordVectorSearchCommandLatency(commandTimer, index, vectorSearchQuery);

      if (Explain.isEnabled() && BsonUtils.isOversized(serializedBatch)) {
        // Explain must be the problem since results are capped at 10k for vector search
        // (CLOUDP-264685)
        throw new ExplainTooLargeException("Explain is too large in vector search query response");
      }

      return serializedBatch;
    }
  }

  @VisibleForTesting
  Optional<String> findEmbeddingModelName(
      IndexDefinition regularIndexDefinition, VectorSearchQuery vectorSearchQuery)
      throws InvalidQueryException {
    if (vectorSearchQuery.criteria().query().isEmpty()) {
      // No need to find model for queryVector.
      return Optional.empty();
    }
    if (regularIndexDefinition.getType() != VECTOR_SEARCH) {
      return Optional.empty();
    }
    @Var var vectorIndexDef = regularIndexDefinition.asVectorDefinition();
    // Non-autoembedding index but has query string, could be derived index definition from
    // auto-embedding index.
    if (!regularIndexDefinition.isAutoEmbeddingIndex()) {
      // For mat view based index, needs to use raw index definition to look up model names.
      Optional<IndexDefinition> rawAutoEmbeddingDefinition =
          this.indexCatalog
              .getIndex(
                  this.definition.db(),
                  this.definition.collectionUuid(),
                  this.definition.viewName(),
                  vectorSearchQuery.index())
              .map(IndexGeneration::getDefinition)
              .filter(IndexDefinition::isAutoEmbeddingIndex);
      if (rawAutoEmbeddingDefinition.isPresent()) {
        vectorIndexDef = rawAutoEmbeddingDefinition.get().asVectorDefinition();
      } else {
        return Optional.empty();
      }
    }
    var fieldPath = vectorSearchQuery.criteria().path();
    Optional<String> indexModel =
        Optional.ofNullable(vectorIndexDef.getModelNamePerPath().get(fieldPath));

    Optional<String> queryModel =
        vectorSearchQuery.criteria().query().flatMap(VectorSearchQueryInput::getModel);

    // If the user didn't specify a model, use the indexing model.
    if (queryModel.isEmpty()) {
      LOG.atDebug()
          .addKeyValue("indexModel", indexModel.orElse("none"))
          .addKeyValue("fieldPath", fieldPath.toString())
          .log("Using indexing model for embedding query (no query model specified)");
      return indexModel;
    }

    // Check if the query model is allowed
    if (!EmbeddingModelCatalog.isQueryModelAllowed(queryModel.get())) {
      throw new InvalidQueryException(
          String.format(
              "model '%s' is not allowed. Allowed models are: %s",
              queryModel.get(), EmbeddingModelCatalog.getAllowedQueryModels()));
    }

    // Then, check if the query model is compatible with the index's model
    if (indexModel.isEmpty()) {
      throw new InvalidQueryException(
          String.format(
              "cannot specify query model '%s' for an index without an embedding model",
              queryModel.get()));
    }
    Set<String> compatibleIndexModels =
        EmbeddingModelCatalog.getCompatibleIndexModels(queryModel.get());
    if (!compatibleIndexModels.contains(indexModel.get())) {
      throw new InvalidQueryException(
          String.format(
              "query model '%s' is not compatible with the index model '%s'",
              queryModel.get(), indexModel.get()));
    }
    LOG.atDebug()
        .addKeyValue("queryModel", queryModel.get())
        .addKeyValue("indexModel", indexModel.get())
        .addKeyValue("fieldPath", fieldPath.toString())
        .log("Using user-specified model for embedding (compatible with indexing model)");
    return queryModel;
  }

  private MongotCursorBatch createExhaustedCursorBatch(BsonArray results)
      throws ExplainTooLargeException {
    var explainResult = Explain.collect();
    var info =
        new MongotCursorResultInfo(
            true,
            results,
            NamespaceBuilder.build(
                this.definition.db(),
                this.definition.collectionName(),
                this.definition.viewName()));

    boolean populateCursorResult =
        Explain.getExplainQueryState()
            .map(
                state ->
                    state
                        .getQueryInfo()
                        .getVerbosity()
                        .isGreaterThan(Explain.Verbosity.QUERY_PLANNER))
            .orElse(true); // if explain isn't present, default to true

    Optional<MongotCursorResult> cursorResult =
        populateCursorResult
            ? Optional.of(info.toCursorResult(MongotCursorResult.EXHAUSTED_CURSOR_ID))
            : Optional.empty();

    return new MongotCursorBatch(cursorResult, explainResult, Optional.empty());
  }

  private Optional<InitializedIndex> getIndexFromCatalog(Query query)
      throws IndexUnavailableException {
    Optional<IndexGeneration> indexGeneration =
        this.indexCatalog.getIndex(
            this.definition.db(),
            this.definition.collectionUuid(),
            this.definition.viewName(),
            query.index());

    if (indexGeneration.isEmpty()) {
      return Optional.empty();
    }
    var index = indexGeneration.get();
    index.getIndex().throwIfUnavailableForQuerying();
    InitializedIndex initializedIndex =
        this.initializedIndexCatalog
            .getIndex(index.getGenerationId())
            .orElseThrow(
                () ->
                    new IndexUnavailableException(
                        String.format(
                            "Index %s not initialized", index.getDefinition().getName())));
    if (initializedIndex.getDefinition().getView().isPresent()) {
      if (this.definition.viewName().isPresent()) {
        this.metrics.queriesAgainstView.increment(); // GA version
      } else {
        this.metrics.queriesAgainstViewSourceCollection.increment(); // preview version
      }
    }

    return Optional.of(initializedIndex);
  }

  private void addMetadataIfExplain(VectorSearchQuery vectorSearchQuery) {
    Explain.getExplainQueryState()
        .map(ExplainQueryState::getQueryInfo)
        .map(
            queryInfo ->
                queryInfo.getFeatureExplainer(
                    MetadataFeatureExplainer.class, MetadataFeatureExplainer::new))
        .ifPresent(
            metadataExplainer -> {
              metadataExplainer.setIndexName(vectorSearchQuery.index());
              metadataExplainer.setMongotVersion(this.metadata.mongotVersion());
              metadataExplainer.setMongotHostName(this.metadata.mongotHostName());
            });
  }

  private boolean isEnvoyMetadataPresent() {
    return this.searchEnvoyMetadata.isPresent()
        && this.searchEnvoyMetadata.get().getRoutedFromAnotherShard();
  }

  private void updateBsonVectorQueryCounters(Vector queryVector) {
    switch (queryVector.getVectorType()) {
      case FLOAT -> {
        if (queryVector.asFloatVector().getOriginalType() == FloatVector.OriginalType.BSON) {
          this.metrics.bsonFloatVectorQueries.increment();
        }
      }
      case BYTE -> this.metrics.bsonByteVectorQueries.increment();
      case BIT -> this.metrics.bsonBitVectorQueries.increment();
    }
  }

  private MaterializedVectorSearchQuery maybeEmbed(
      VectorSearchQuery vectorSearchQuery,
      Optional<String> canonicalModel,
      IndexDefinition indexDefinition)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    VectorSearchCriteria criteria = vectorSearchQuery.criteria();
    Optional<String> textToEmbed = criteria.query().flatMap(VectorSearchQueryInput::getText);
    if (textToEmbed.isEmpty()) {
      // If query field was present but text is empty, throw a descriptive error
      if (criteria.query().isPresent()) {
        throw new InvalidQueryException(
            "'query' field cannot be empty for auto-embedding vector search");
      }
      return new MaterializedVectorSearchQuery(
          vectorSearchQuery, Check.isPresent(criteria.queryVector(), "queryVector"));
    }
    if (canonicalModel.isEmpty()) {
      throw new InvalidQueryException(
          "Vector index for this text based query is invalid due to missing model");
    }
    if (this.embeddingServiceManagerSupplier.get() == null) {
      throw new EmbeddingProviderTransientException("Embedding service not initialized.");
    }

    // Create EmbeddingRequestContext from IndexDefinition
    EmbeddingRequestContext context =
        new EmbeddingRequestContext(
            indexDefinition.getDatabase(),
            indexDefinition.getIndexId(),
            indexDefinition.getCollectionUuid());

    List<VectorOrError> results;
    try {
      results =
          this.embeddingServiceManagerSupplier
              .get()
              .embed(
                  List.of(textToEmbed.get()),
                  EmbeddingModelCatalog.getModelConfig(canonicalModel.get()),
                  EmbeddingServiceConfig.ServiceTier.QUERY,
                  context);
    } catch (Exception e) {
      LOG.atError()
          .addKeyValue("model", canonicalModel.get())
          .addKeyValue("errorType", e.getClass().getSimpleName())
          .addKeyValue("errorMessage", e.getMessage())
          .log("Failed to embed query text");
      throw e;
    }
    var result = Check.hasSingleElement(results, "vectorized query");
    if (result.errorMessage.isPresent()) {
      LOG.atError()
          .addKeyValue("model", canonicalModel.get())
          .addKeyValue("errorMessage", result.errorMessage.get())
          .log("Embedding service returned error for query");
    }
    Check.checkState(
        result.errorMessage.isEmpty(), "Got error when embedding query: %s", result.errorMessage);

    return new MaterializedVectorSearchQuery(
        vectorSearchQuery, Check.isPresent(result.vector, "vector"));
  }

  private void updateVectorSearchQueryCounters(VectorSearchCriteria criteria) {
    switch (criteria.getVectorSearchType()) {
      case EXACT -> this.metrics.exactVectorSearchQueries.increment();
      case APPROXIMATE -> this.metrics.approximateVectorSearchQueries.increment();
      case AUTO_EMBEDDING -> {
        this.metrics.autoEmbeddingVectorSearchQueries.increment();
        criteria
            .query()
            .ifPresent(
                queryInput -> {
                  switch (queryInput) {
                    case VectorSearchQueryInput.Text t ->
                        this.metrics.autoEmbeddingTextQueries.increment();
                    case VectorSearchQueryInput.MultiModal m ->
                        this.metrics.autoEmbeddingMultiModalQueries.increment();
                  }
                });
      }
    }
  }

  private void increaseConcurrentQueriesGauge(VectorSearchCriteria.Type queryType) {
    switch (queryType) {
      case EXACT -> this.metrics.concurrentExactQueries.incrementAndGet();
      case APPROXIMATE -> this.metrics.concurrentApproximateQueries.incrementAndGet();
      case AUTO_EMBEDDING -> this.metrics.concurrentAutoEmbeddingQueries.incrementAndGet();
    }
  }

  private void decreaseConcurrentQueriesGauge(VectorSearchCriteria.Type queryType) {
    switch (queryType) {
      case EXACT -> this.metrics.concurrentExactQueries.decrementAndGet();
      case APPROXIMATE -> this.metrics.concurrentApproximateQueries.decrementAndGet();
      case AUTO_EMBEDDING -> this.metrics.concurrentAutoEmbeddingQueries.decrementAndGet();
    }
  }

  /**
   * Records the vector search command total latency with index size and quantization tags.
   *
   * <p>This metric is guarded by the INDEX_SIZE_QUANTIZATION_METRICS feature flag. When disabled,
   * no metrics are recorded.
   *
   * @param timer The timer that was started at the beginning of the command execution
   * @param index The initialized index being queried
   * @param vectorSearchQuery The vector search query being executed
   */
  private void recordVectorSearchCommandLatency(
      Timer.Sample timer, InitializedIndex index, VectorSearchQuery vectorSearchQuery) {

    if (!this.metadata.featureFlags().isEnabled(Feature.INDEX_SIZE_QUANTIZATION_METRICS)) {
      return;
    }

    long indexSizeBytes = index.getIndexSize();
    String indexSizeCategory = categorizeIndexSize(indexSizeBytes);
    String quantizationType = determineQuantizationType(index.getDefinition(), vectorSearchQuery);

    Tags tags = Tags.of(
        Tag.of("indexSizeCategory", indexSizeCategory),
        Tag.of("quantizationType", quantizationType)
    );

    Timer commandTimer = this.metricsFactory.timer(
        "vectorSearchCommandTotalLatencyByIndexSize", tags, 0.5, 0.75, 0.9, 0.99);
    timer.stop(commandTimer);
  }

  /**
   * Categorizes index size into predefined buckets.
   *
   * @param indexSizeBytes The index size in bytes
   * @return The index size category: "small", "medium", "large", or "xlarge"
   */
  private String categorizeIndexSize(long indexSizeBytes) {
    long oneGiB = 1024L * 1024L * 1024L;
    long tenGiB = 10L * oneGiB;
    long hundredGiB = 100L * oneGiB;

    if (indexSizeBytes < oneGiB) {
      return "small";
    } else if (indexSizeBytes < tenGiB) {
      return "medium";
    } else if (indexSizeBytes < hundredGiB) {
      return "large";
    } else {
      return "xlarge";
    }
  }

  /**
   * Determines the quantization type from the index definition (index-side quantization).
   *
   * <p>This returns how vectors are quantized and stored in the index, not how the query vector
   * is encoded by the client (client-side quantization). Index-side quantization affects search
   * performance and is a key characteristic for latency metrics.
   *
   * @param definition The index definition
   * @param vectorSearchQuery The vector search query (used to identify the field path)
   * @return The quantization type: "unquantized", "scalar_quantized", "binary_quantized",
   *     or "unknown"
   */
  private String determineQuantizationType(
      IndexDefinition definition, VectorSearchQuery vectorSearchQuery) {
    if (definition.getType() == VECTOR_SEARCH) {
      VectorIndexDefinition vectorIndexDef = definition.asVectorDefinition();
      FieldPath queryPath = vectorSearchQuery.criteria().path();

      return vectorIndexDef.getMappings()
          .getQuantizationForField(queryPath)
          .map(quantization -> switch (quantization) {
            case NONE -> "unquantized";
            case SCALAR -> "scalar_quantized";
            case BINARY -> "binary_quantized";
          })
          .orElse("unknown");
    }
    return "unknown";
  }

  public static class Factory implements CommandFactory {

    private final IndexCatalog indexCatalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
    private final Metrics metrics;

    public Factory(
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory) {
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.metadata = metadata;
      this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
      this.metrics = new Metrics(metricsFactory);
    }

    @Override
    public Command create(BsonDocument args) {
      try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
        var definition =
            VectorSearchCommandDefinition.fromBson(
                parser, this.metadata.featureFlags().isEnabled(Feature.VECTOR_STORED_SOURCE));
        return new VectorSearchCommand(
            definition,
            this.indexCatalog,
            this.initializedIndexCatalog,
            this.metadata,
            this.embeddingServiceManagerSupplier,
            this.metrics);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    public Command create(VectorSearchCommandDefinition definition) {
      return new VectorSearchCommand(
          definition,
          this.indexCatalog,
          this.initializedIndexCatalog,
          this.metadata,
          this.embeddingServiceManagerSupplier,
          this.metrics);
    }
  }

  /**
   * This class caches Counter/Gauge references in the CommandFactory to avoid repeated lookups per
   * query.
   */
  static class Metrics {

    private final Counter totalCount;
    private final Counter invalidQueries;
    private final Counter wrappedInvalidQueryExceptions;
    private final Counter queriesAgainstView;
    private final Counter queriesAgainstViewSourceCollection;
    private final Counter exactVectorSearchQueries;
    private final Counter approximateVectorSearchQueries;
    private final Counter autoEmbeddingVectorSearchQueries;
    private final Counter autoEmbeddingTextQueries;
    private final Counter autoEmbeddingMultiModalQueries;
    private final Counter bsonFloatVectorQueries;
    private final Counter bsonByteVectorQueries;
    private final Counter bsonBitVectorQueries;
    private final Counter vectorStoredSourceQueries;
    private final AtomicLong concurrentExactQueries;
    private final AtomicLong concurrentApproximateQueries;
    private final AtomicLong concurrentAutoEmbeddingQueries;
    private final MetricsFactory metricsFactory;

    Metrics(MetricsFactory metricsFactory) {
      // The counter for $vectorSearch queries over vector indexes.
      this.totalCount = metricsFactory.counter("vectorSearchCommandTotalCount");
      this.invalidQueries = metricsFactory.counter("vectorSearchCommandInvalidQueries");
      this.wrappedInvalidQueryExceptions =
          metricsFactory.counter("vectorSearchCommandWrappedInvalidQueryException");
      this.queriesAgainstView = metricsFactory.counter("queriesAgainstView");
      this.queriesAgainstViewSourceCollection =
          metricsFactory.counter("queriesAgainstViewSourceCollection");
      this.exactVectorSearchQueries = metricsFactory.counter("exactVectorSearchQueries");
      this.approximateVectorSearchQueries =
          metricsFactory.counter("approximateVectorSearchQueries");
      this.autoEmbeddingVectorSearchQueries =
          metricsFactory.counter("autoEmbeddingVectorSearchQueries");
      // TODO(CLOUDP-371805): remove autoEmbeddingTextQueries and autoEmbeddingMultiModalQueries
      // after deprecating TEXT-typed queries
      this.autoEmbeddingTextQueries =
          metricsFactory.counter("autoEmbeddingTextVectorSearchQueries");
      this.autoEmbeddingMultiModalQueries =
          metricsFactory.counter("autoEmbeddingMultiModalVectorSearchQueries");
      this.bsonFloatVectorQueries = metricsFactory.counter("bsonFloatVectorQueries");
      this.bsonByteVectorQueries = metricsFactory.counter("bsonByteVectorQueries");
      this.bsonBitVectorQueries = metricsFactory.counter("bsonBitVectorQueries");
      this.vectorStoredSourceQueries = metricsFactory.counter("vectorStoredSourceQueries");
      this.concurrentExactQueries = metricsFactory.numGauge("concurrentExactQueries");
      this.concurrentApproximateQueries = metricsFactory.numGauge("concurrentApproximateQueries");
      this.concurrentAutoEmbeddingQueries =
          metricsFactory.numGauge("concurrentAutoEmbeddingQueries");
      this.metricsFactory = metricsFactory;
    }
  }
}
