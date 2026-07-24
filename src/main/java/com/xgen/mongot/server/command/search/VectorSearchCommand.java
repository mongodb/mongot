package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Type.VECTOR_SEARCH;
import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.AUTO_EMBEDDING;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.NamespaceBuilder;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
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
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.EmptyExplainInformation;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.index.query.AutoEmbedQueryVectorReconciler;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.UserFacingException;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.mongot.util.retry.ActionRetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs $vectorSearch over an vector index. */
public class VectorSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(VectorSearchCommand.class);

  /**
   * Bundles the resolved embedding model name together with the raw (source) auto-embedding index
   * definition. The source definition carries the original user-facing database name which is
   * needed by {@link EmbeddingRequestContext} for multi-tenant credential lookup.
   */
  @VisibleForTesting
  record EmbedRequestInfo(Optional<String> modelName, IndexDefinition sourceDefinition) {}

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final MetricsFactory metricsFactory;
  private final VectorSearchCommandDefinition definition;
  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Metrics metrics;

  private final MongotCursorManager cursorManager;
  private final Bytes bsonSizeSoftLimit;
  private final List<Long> createdCursorIds;

  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;
  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

  VectorSearchCommand(
      VectorSearchCommandDefinition definition,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      Metrics metrics,
      MongotCursorManager cursorManager,
      Bytes bsonSizeSoftLimit) {
    this.metricsFactory = metrics.metricsFactory;
    this.cursorManager = cursorManager;
    this.bsonSizeSoftLimit = bsonSizeSoftLimit;
    this.createdCursorIds = new ArrayList<>();
    this.definition = definition;
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.metadata = metadata;
    this.metrics = metrics;
    this.searchEnvoyMetadata = Optional.empty();
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
  }

  @Override
  public List<Long> getCreatedCursorIds() {
    return List.copyOf(this.createdCursorIds);
  }

  @Override
  public String name() {
    return VectorSearchCommandDefinition.NAME;
  }

  @Override
  public void handleSearchEnvoyMetadata(SearchEnvoyMetadata searchEnvoyMetadata) {
    this.searchEnvoyMetadata = Optional.of(searchEnvoyMetadata);
  }

  @Override
  public boolean maybeLoadShed() {
    // Auto-embedding queries call an external embedding service; delaying them in the
    // load-shedded queue doesn't reduce load on the search node, so skip load shedding.
    // The instanceof check is required because InvalidVectorQuery.vectorSearchQuery() throws.
    if (this.definition.vectorSearchQueryOrUserError()
        instanceof
        VectorSearchCommandDefinition.VectorSearchQueryOrUserError.ValidVectorQuery validQuery) {
      return validQuery.vectorSearchQuery().criteria().getVectorSearchType()
          != VectorSearchCriteria.Type.AUTO_EMBEDDING;
    }
    return true;
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

    if (mongoDbVersionOptional.isPresent()
        && !isVectorStoredSourceVersionOk(mongoDbVersionOptional.get())) {
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
      throw new InvalidQueryException("storedSource is not configured for this index.");
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
      this.definition
          .deadlineTimestampMs()
          .ifPresent(ignored -> this.metrics.queriesWithDeadline.increment());

      var query = this.definition.getQuery();
      if (query.userReturnStoredSource()) {
        this.metrics.vectorStoredSourceQueries.increment();
      }
      QueryCursorOptions queryCursorOptions =
          this.definition
              .cursorOptions()
              .map(CursorOptionsDefinition::toQueryCursorOptions)
              .orElse(QueryCursorOptions.empty());
      validateQueryAndCursorOptions(query, queryCursorOptions);

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
              return getSearchResults(query, index, queryCursorOptions);
            }
          },
          ReaderClosedException.class,
          1);

    } catch (BsonParseException | InvalidQueryException e) {
      return handleInvalidQueryException(e);
    } catch (DeadlineExceededException e) {
      this.metrics.vectorQueriesTimedOut.increment();
      return MessageUtils.createError(
          Errors.MAX_TIME_MS_EXPIRED, Objects.requireNonNull(e.getMessage()));
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
      // Use the same tag keys as the Exception branch above so all registrations of
      // vectorSearchCommandInternalFailures share one tag-key set (Prometheus requirement). A raw
      // Throwable is not a UserFacingException, so it is always an internal error.
      this.metricsFactory
          .counter(
              "vectorSearchCommandInternalFailures",
              Tags.of(
                  "exceptionName",
                  e.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  ErrorType.INTERNAL_ERROR.toString()))
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

  private void validateQueryAndCursorOptions(
      VectorSearchQuery vectorSearchQuery, QueryCursorOptions queryCursorOptions)
      throws InvalidQueryException {
    if (queryCursorOptions.requireSequenceTokens()) {
      throw new InvalidQueryException(
          "Pagination is not supported with the 'vectorSearch' command. "
              + "Use $skip and $limit stages instead.");
    }

    if (!vectorSearchQuery.returnStoredSource()
        && !queryCursorOptions.equals(QueryCursorOptions.empty())) {
      throw new InvalidQueryException(
          "cursor options are only supported for vector search when returnStoredSource is true");
    }
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
      VectorSearchQuery vectorSearchQuery,
      Optional<InitializedIndex> optionalIndex,
      QueryCursorOptions queryCursorOptions)
      throws IndexUnavailableException,
          InvalidQueryException,
          ReaderClosedException,
          IOException,
          MongotCursorNotFoundException,
          InterruptedException {
    try (var guard = Tracing.vectorSearchSpanGuard("VectorSearchCommand.getSearchResults")) {
      if (optionalIndex.isEmpty()) {
        // the index does not exist, respond with empty information. The result is not cached,
        // so when this index does get created, we will answer queries correctly.
        return createExhaustedCursorBatch(new BsonArray()).toBson();
      }

      // Compute once and reuse for both the counter and the latency metric tag.
      boolean isNested =
          isNestedVectorSearch(vectorSearchQuery, optionalIndex.get().getDefinition());

      // Record nested vector search query counter at command receipt, consistent with other
      // counters.
      if (isNested) {
        this.metrics.nestedVectorSearchQueries.increment();
        recordNestedVectorSearchTags(vectorSearchQuery);
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

      var timer = Timer.start();
      var commandTimer = Timer.start();
      var materializedQuery =
          materializeQuery(
              vectorSearchQuery,
              findEmbedRequestInfo(optionalIndex.get().getDefinition(), vectorSearchQuery));
      var context = buildQueryExecutionContext();
      if (vectorSearchQuery.returnStoredSource()) {
        return getBatch(
            materializedQuery,
            context,
            queryCursorOptions,
            optionalIndex.get(),
            timer,
            commandTimer,
            isNested);
      } else {
        // TODO(CLOUDP-383074): consider using vector cursors even without stored source
        return getExhaustedBatch(
            materializedQuery, context, optionalIndex.get(), timer, commandTimer, isNested);
      }
    }
  }

  /* getBatch() uses cursors (but getExhaustedBatch() does not) */
  private BsonDocument getBatch(
      MaterializedVectorSearchQuery materializedQuery,
      QueryExecutionContext context,
      QueryCursorOptions queryCursorOptions,
      InitializedIndex index,
      Timer.Sample timer,
      Timer.Sample commandTimer,
      boolean isNested)
      throws MongotCursorNotFoundException,
          IndexUnavailableException,
          InvalidQueryException,
          IOException,
          InterruptedException,
          ReaderClosedException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.getBatch")) {
      boolean populateCursorResult =
          SearchCommand.determinePopulateCursor(
              Explain.getExplainQueryState(),
              this.metadata
                  .mongoDbServerInfoProvider()
                  .getCachedMongoDbServerInfo()
                  .mongoDbVersion());

      try (var cursorGuard = new CursorGuard(this.createdCursorIds, this.cursorManager)) {
        var sample = Timer.start();

        var cursorInfo =
            this.cursorManager.newCursor(
                this.definition.db(),
                this.definition.collectionName(),
                this.definition.collectionUuid(),
                this.definition.viewName(),
                new CursorQuery.Vector(materializedQuery, context),
                queryCursorOptions,
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                this.searchEnvoyMetadata);

        long cursorId = cursorInfo.cursorId;
        this.createdCursorIds.add(cursorId);

        // Get the timer consumer first, as the cursor may be killed when the next batch is
        // retrieved.
        QueryBatchTimerRecorder queryBatchTimerRecorder =
            this.cursorManager.getIndexQueryBatchTimerRecorder(cursorId);

        Optional<BsonValue> variables =
            Optional.of(new Variables(cursorInfo.metaResults).toRawBson());
        MongotCursorResultInfo cursorResultInfo =
            this.cursorManager.getNextBatch(
                cursorId,
                this.bsonSizeSoftLimit.subtract(
                    MongotCursorBatch.calculateEmptyBatchSize(variables, Optional.empty())),
                queryCursorOptions);

        Optional<MongotCursorResult> cursorResult =
            populateCursorResult
                ? Optional.of(cursorResultInfo.toCursorResult(cursorId, Optional.empty()))
                : Optional.empty();

        MongotCursorBatch batch =
            new MongotCursorBatch(
                cursorResult,
                cursorResultInfo.explainResult,
                populateCursorResult ? variables : Optional.empty());

        queryBatchTimerRecorder.recordSample(sample);
        var serializedBatch = batch.toBson();
        var metrics = index.getMetricsUpdater().getQueryingMetricsUpdater();
        timer.stop(metrics.getVectorResultLatencyTimer());
        metrics.getVectorCommandCounter().increment();

        // Record command-level latency with index size, quantization, and nested tags
        recordVectorSearchCommandLatency(
            commandTimer, index, materializedQuery.materializedCriteria(), isNested);

        if (Explain.isEnabled() && BsonUtils.isOversized(serializedBatch)) {
          // Explain must be the problem since cursor output should not have been oversized
          // (CLOUDP-264685)
          throw new ExplainTooLargeException(
              "Explain is too large in vector search query response");
        }

        if (populateCursorResult) {
          cursorGuard.keepCursors();
        }
        return serializedBatch;
      }
    }
  }

  /* getExhaustedBatch() does NOT use cursors (see getBatch() for cursor support) */
  private BsonDocument getExhaustedBatch(
      MaterializedVectorSearchQuery materializedQuery,
      QueryExecutionContext context,
      InitializedIndex index,
      Timer.Sample timer,
      Timer.Sample commandTimer,
      boolean isNested)
      throws IOException, InvalidQueryException, ReaderClosedException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.getExhaustedBatch")) {
      VectorIndexReader reader;
      if (index instanceof InitializedVectorIndex vectorIndex) {
        reader = vectorIndex.getReader();
      } else if (index instanceof InitializedMaterializedViewIndex matViewIndex) {
        // TODO(CLOUDP-353553): split when MV can hold a SearchIndexDefinition.
        reader = matViewIndex.getReader();
      } else {
        throw new InvalidQueryException(
            "Cannot execute $vectorSearch over search index '%s'"
                .formatted(materializedQuery.vectorSearchQuery().index()),
            InvalidQueryException.Type.STRICT);
      }
      var results = reader.query(materializedQuery, context);
      var serializedBatch = createExhaustedCursorBatch(results).toBson();

      var metrics = index.getMetricsUpdater().getQueryingMetricsUpdater();
      var durationNs = timer.stop(metrics.getVectorResultLatencyTimer());
      metrics.recordDynamicFeatureFlagLatencyTimer(durationNs);
      metrics.getVectorCommandCounter().increment();

      // Record command-level latency with index size and quantization tags
      recordVectorSearchCommandLatency(
          commandTimer, index, materializedQuery.materializedCriteria(), isNested);

      if (Explain.isEnabled() && BsonUtils.isOversized(serializedBatch)) {
        // Explain must be the problem since results are capped at 10k for vector search
        // (CLOUDP-264685)
        throw new ExplainTooLargeException("Explain is too large in vector search query response");
      }

      return serializedBatch;
    }
  }

  // EmbedRequestInfo for an auto-embedded path: with a model for text queries, model-less for
  // queryVector (whose source definition is still needed to reconcile the vector dtype).
  @VisibleForTesting
  Optional<EmbedRequestInfo> findEmbedRequestInfo(
      IndexDefinition regularIndexDefinition, VectorSearchQuery vectorSearchQuery)
      throws InvalidQueryException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.findEmbedRequestInfo")) {
      boolean isTextQuery = vectorSearchQuery.criteria().query().isPresent();
      // TODO(CLOUDP-353553): Handle search index with autoEmbed field.
      if (regularIndexDefinition.getType() != VECTOR_SEARCH) {
        return Optional.empty();
      }
      @Var var vectorIndexDef = regularIndexDefinition.asVectorDefinition();
      // For the EmbedRequestInfo we need the source definition that carries the original
      // user-facing database name. For direct auto-embedding indexes this is regularIndexDefinition
      // itself; for mat-view derived indexes (whose queried definition is a plain vector spec) it
      // is the raw auto-embedding definition resolved from the catalog below, used for both the
      // model (text queries) and the quantization (queryVector reconciliation).
      if (!regularIndexDefinition.isAutoEmbeddingIndex()) {
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
      boolean isAutoEmbeddedPath = vectorIndexDef.getModelNamePerPath().containsKey(fieldPath);

      // queryVector needs the source definition only to reconcile against an auto-embedded path;
      // on any other path it carries no embedding request (regular vector field semantics).
      if (!isTextQuery) {
        return isAutoEmbeddedPath
            ? Optional.of(new EmbedRequestInfo(Optional.empty(), vectorIndexDef))
            : Optional.empty();
      }

      // Text query path: the path must be auto-embedded.
      if (!isAutoEmbeddedPath) {
        throw new InvalidQueryException(
            "Path '%s' is not defined in Automated Embedding index '%s'"
                .formatted(fieldPath, vectorSearchQuery.index()));
      }
      String indexModelName = vectorIndexDef.getModelNamePerPath().get(fieldPath);
      Optional<String> queryModel =
          vectorSearchQuery.criteria().query().flatMap(VectorSearchQueryInput::getModel);
      // Shared with the $search path: resolves to the index model when no query model is supplied,
      // otherwise the query model after validating it is allowed and compatible with the index
      // model.
      String resolvedModel =
          SearchAutoEmbeddingHelper.resolveQueryModel(indexModelName, queryModel);
      return Optional.of(new EmbedRequestInfo(Optional.of(resolvedModel), vectorIndexDef));
    }
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
        SearchCommand.determinePopulateCursor(
            Explain.getExplainQueryState(),
            this.metadata
                .mongoDbServerInfoProvider()
                .getCachedMongoDbServerInfo()
                .mongoDbVersion());

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
            // Offline fallback: an unresolved auto-embedding index (mongod unreachable) is absent
            // from the catalog but may carry a derived Lucene reconstructed from its last commit.
            .or(() -> offlineReconstructedIndex(index))
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

  /**
   * The derived Lucene index reconstructed offline from an unresolved auto-embedding index's
   * persisted commit metadata, when present, so a $vectorSearch can serve from the last-committed
   * data while mongod is unreachable.
   *
   * <p>TODO(CLOUDP-363302): stored source is not supported for auto-embedding (the derived
   * definition carries no {@code storedSource}), so {@code returnStoredSource: true} is rejected
   * upstream rather than served offline; enable it here once auto-embedding supports stored source.
   */
  static Optional<InitializedIndex> offlineReconstructedIndex(IndexGeneration indexGeneration) {
    if (indexGeneration.getIndex() instanceof UnresolvedAutoEmbeddingIndex unresolved) {
      return unresolved.getReconstructedDerivedIndex();
    }
    return Optional.empty();
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

  private QueryExecutionContext buildQueryExecutionContext() {
    Optional<Long> deadlineTimestampMs =
        this.metadata
                .dynamicFeatureFlagRegistry()
                .evaluateClusterInvariant(DynamicFeatureFlags.VECTOR_SEARCH_QUERY_TIMEOUT)
            ? this.definition.deadlineTimestampMs()
            : Optional.empty();
    return QueryExecutionContext.withDeadline(deadlineTimestampMs);
  }

  // Produces the vector to search with. Exactly one of query/queryVector is present (enforced by
  // VectorSearchCriteria): a user-supplied queryVector is reconciled against the index encoding; a
  // text query is embedded into a vector via the provider.
  private MaterializedVectorSearchQuery materializeQuery(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.materializeQuery")) {
      if (vectorSearchQuery.criteria().queryVector().isPresent()) {
        return materializeUserQueryVector(vectorSearchQuery, embedRequestInfo);
      }
      return embedQueryText(vectorSearchQuery, embedRequestInfo);
    }
  }

  // Builds the query from a user-supplied queryVector, reconciling it against the index encoding.
  private MaterializedVectorSearchQuery materializeUserQueryVector(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException {
    return new MaterializedVectorSearchQuery(
        vectorSearchQuery,
        reconcileUserQueryVector(vectorSearchQuery, embedRequestInfo),
        findAutoEmbeddingFieldsMapping(vectorSearchQuery),
        findAutoEmbedVectorSourcePaths(embedRequestInfo));
  }

  // Reconciles a user-supplied queryVector against the index's autoEmbed encoding: returns it
  // unchanged for regular type:vector (BYOV) and matching autoEmbed dtypes (dtype/dimension
  // validation happens later in VectorSearchQueryFactory); throws on a mismatch.
  private Vector reconcileUserQueryVector(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.reconcileUserQueryVector")) {
      VectorSearchCriteria criteria = vectorSearchQuery.criteria();
      Vector queryVector = criteria.queryVector().get();
      Optional<VectorAutoEmbedQuantization> autoEmbedQuantization =
          embedRequestInfo
              .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
              .flatMap(
                  info ->
                      findAutoEmbedQuantization(
                          info.sourceDefinition().asVectorDefinition(), criteria.path()));
      if (autoEmbedQuantization.isEmpty()) {
        return queryVector;
      }
      return AutoEmbedQueryVectorReconciler.reconcile(
          queryVector, autoEmbedQuantization.get(), criteria.path());
    }
  }

  // Embeds a text query into a vector via the auto-embedding provider. Only reached when there is
  // no queryVector, so a non-auto-embedding (BYOV) index here means the user omitted the required
  // queryVector.
  private MaterializedVectorSearchQuery embedQueryText(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchCommand.embedQueryText")) {
      VectorSearchCriteria criteria = vectorSearchQuery.criteria();
      Optional<String> textToEmbed = criteria.query().flatMap(VectorSearchQueryInput::getText);

      if (embedRequestInfo
          .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
          .isEmpty()) {
        throw new InvalidQueryException("queryVector must be present");
      }

      // Text query path: requires a valid text query.
      if (criteria.query().isPresent() && textToEmbed.isEmpty()) {
        throw new InvalidQueryException(
            "'query' field cannot be empty when querying Automated Embedding index");
      }

      // Text query path: requires an auto-embedding-capable index and a resolvable model.
      Optional<String> canonicalModel = embedRequestInfo.flatMap(EmbedRequestInfo::modelName);
      if (canonicalModel.isEmpty()) {
        throw new InvalidQueryException(
            "Automated Embedding index query is invalid due to missing model");
      }
      if (this.embeddingServiceManagerSupplier.get() == null) {
        throw new EmbeddingProviderTransientException(
            "Embedding service not initialized.",
            EmbeddingProviderTransientException.Reason.SERVICE_NOT_INITIALIZED);
      }

      // Use the raw (source) auto-embedding index definition for the embedding context.
      // This carries the original user-facing database name needed for multi-tenant credential
      // lookup, as opposed to the derived definition whose database is __mdb_internal_search.
      IndexDefinition sourceDefinition = embedRequestInfo.get().sourceDefinition();
      EmbeddingRequestContext context =
          // TODO(CLOUDP-353553): Handle search index with autoEmbed field.
          prepareEmbeddingRequestContext(vectorSearchQuery, sourceDefinition.asVectorDefinition());
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
          vectorSearchQuery,
          Check.isPresent(result.vector, "vector"),
          findAutoEmbeddingFieldsMapping(vectorSearchQuery),
          findAutoEmbedVectorSourcePaths(embedRequestInfo));
    }
  }

  /** AutoEmbed quantization for the path; empty for non-autoEmbed fields (no reconciliation). */
  private static Optional<VectorAutoEmbedQuantization> findAutoEmbedQuantization(
      VectorIndexDefinition sourceIndexDefinition, FieldPath path) {
    return sourceIndexDefinition
        .getMappings()
        .getFieldDefinition(path)
        .filter(fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .map(VectorIndexFieldDefinition::asVectorAutoEmbedField)
        .map(fieldDef -> fieldDef.specification().autoEmbedQuantization());
  }

  private EmbeddingRequestContext prepareEmbeddingRequestContext(
      VectorSearchQuery vectorSearchQuery, VectorIndexDefinition sourceIndexDefinition)
      throws InvalidQueryException {
    // Get autoEmbed field definition to resolve numDimensions & providerOutputDataType
    FieldPath sourcePath = vectorSearchQuery.criteria().path();
    Optional<VectorAutoEmbedFieldDefinition> autoEmbedField =
        sourceIndexDefinition
            .getMappings()
            .getFieldDefinition(sourcePath)
            .filter(fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .map(VectorIndexFieldDefinition::asVectorAutoEmbedField);
    if (autoEmbedField.isPresent()) {
      // MV-based auto-embedding index: use provider quantization from auto-embed spec.
      VectorAutoEmbedFieldSpecification spec = autoEmbedField.get().specification();
      return new EmbeddingRequestContext(
          sourceIndexDefinition.getDatabase(),
          sourceIndexDefinition.getName(),
          sourceIndexDefinition.getLastObservedCollectionName(),
          spec.numDimensions(),
          spec.autoEmbedQuantization());
    }
    // Type-text index: provider dtype is always float.
    VectorFieldSpecification spec =
        sourceIndexDefinition
            .getMappings()
            .getFieldDefinition(sourcePath)
            .filter(VectorIndexFieldDefinition::isVectorField)
            .map(def -> def.asVectorField().specification())
            .orElseThrow(
                () ->
                    new InvalidQueryException(
                        ("Error when preparing embedding request context: path '%s' is not defined "
                                + "in Automated Embedding index")
                            .formatted(sourcePath)));

    return new EmbeddingRequestContext(
        sourceIndexDefinition.getDatabase(),
        sourceIndexDefinition.getName(),
        sourceIndexDefinition.getLastObservedCollectionName(),
        spec.numDimensions(),
        VectorAutoEmbedQuantization.FLOAT // type:TEXT → always float32 provider output
        );
  }

  // All auto-embed (vector) source paths declared by the index, used to keep filter clauses from
  // being remapped into a vector field's MV path. The schema mapping conflates AUTO_EMBED and
  // FILTER remap entries, so MaterializedVectorSearchQuery needs the embed source paths to tell
  // them apart when an index declares more than one auto-embed field (CLOUDP-402522).
  private Set<FieldPath> findAutoEmbedVectorSourcePaths(
      Optional<EmbedRequestInfo> embedRequestInfo) {
    return embedRequestInfo
        .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
        .map(info -> info.sourceDefinition().asVectorDefinition())
        .map(
            definition ->
                definition.getFields().stream()
                    .filter(VectorIndexFieldDefinition::isVectorField)
                    .map(VectorIndexFieldDefinition::getPath)
                    .collect(Collectors.toUnmodifiableSet()))
        .orElse(Set.of());
  }

  // TODO(CLOUDP-423143): this catalog lookup is independent of the one that picks the reader to
  // serve from, so a generation swap between them can pair a mapping with a reader from different
  // generations. Resolve the catalog entry once per query instead.
  private Map<FieldPath, FieldPath> findAutoEmbeddingFieldsMapping(
      VectorSearchQuery vectorSearchQuery) {
    // Resolved index: the mapping comes from the materialized view.
    Optional<InitializedMaterializedViewIndex> materializedViewIndex =
        resolveMaterializedViewIndex(vectorSearchQuery);
    if (materializedViewIndex.isPresent()) {
      return materializedViewIndex.get().getSchemaMetadata().autoEmbeddingFieldsMapping();
    }
    // Offline (view unresolved): fall back to the mapping persisted with the reconstructed index,
    // without which query paths would not be remapped to the derived Lucene path.
    return this.indexCatalog
        .getIndex(
            this.definition.db(),
            this.definition.collectionUuid(),
            this.definition.viewName(),
            vectorSearchQuery.index())
        .map(IndexGeneration::getIndex)
        .flatMap(
            index ->
                index instanceof UnresolvedAutoEmbeddingIndex unresolved
                    ? Optional.of(unresolved.getAutoEmbeddingFieldsMapping())
                    : Optional.empty())
        .orElse(Map.of());
  }

  private Optional<InitializedMaterializedViewIndex> resolveMaterializedViewIndex(
      VectorSearchQuery vectorSearchQuery) {
    return this.indexCatalog
        .getIndex(
            this.definition.db(),
            this.definition.collectionUuid(),
            this.definition.viewName(),
            vectorSearchQuery.index())
        .filter(indexGeneration -> indexGeneration.getType() == AUTO_EMBEDDING)
        .map(indexGeneration -> (AutoEmbeddingIndexGeneration) indexGeneration)
        .map(it -> it.getMaterializedViewIndexGeneration().getIndex());
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
   * Records the vector search command total latency with index size, quantization, and nested tags.
   *
   * <p>This metric is guarded by the INDEX_SIZE_QUANTIZATION_METRICS feature flag. When disabled,
   * no metrics are recorded.
   *
   * @param timer The timer that was started at the beginning of the command execution
   * @param index The initialized index being queried
   * @param vectorSearchCriteria The vector search criteria being executed
   * @param isNested Whether this is a nested (embedded) vector search query
   */
  private void recordVectorSearchCommandLatency(
      Timer.Sample timer,
      InitializedIndex index,
      VectorSearchCriteria vectorSearchCriteria,
      boolean isNested) {

    if (!this.metadata.featureFlags().isEnabled(Feature.INDEX_SIZE_QUANTIZATION_METRICS)) {
      return;
    }

    long indexSizeBytes = index.getIndexSize();
    String indexSizeCategory = categorizeIndexSize(indexSizeBytes);
    String quantizationType =
        determineQuantizationType(index.getDefinition(), vectorSearchCriteria);

    Tags tags =
        Tags.of(
            Tag.of("indexSizeCategory", indexSizeCategory),
            Tag.of("quantizationType", quantizationType),
            Tag.of("isNested", String.valueOf(isNested)));

    Timer commandTimer =
        this.metricsFactory.timer(
            "vectorSearchCommandTotalLatencyByIndexSize", tags, 0.5, 0.75, 0.9, 0.99);
    timer.stop(commandTimer);
  }

  /**
   * Determines whether a vector search query is a nested (embedded) vector search.
   *
   * <p>A query is considered nested if the query specifies {@code nestedOptions} (embeddedOptions),
   * or if the query's field path falls under the index definition's {@code nestedRoot}.
   *
   * @param vectorSearchQuery The vector search query
   * @param definition The index definition
   * @return true if the query is a nested vector search
   */
  private static boolean isNestedVectorSearch(
      VectorSearchQuery vectorSearchQuery, IndexDefinition definition) {
    if (vectorSearchQuery.criteria().embeddedOptions().isPresent()) {
      return true;
    }
    if (definition.getType() == VECTOR_SEARCH) {
      return definition
          .asVectorDefinition()
          .getNestedRoot()
          .map(nestedRoot -> vectorSearchQuery.criteria().path().isChildOf(nestedRoot))
          .orElse(false);
    }
    return false;
  }

  /**
   * Records tagged counters for nested vector search queries to track filter, parentFilter, and
   * scoreMode usage independently.
   *
   * @param vectorSearchQuery The vector search query
   */
  private void recordNestedVectorSearchTags(VectorSearchQuery vectorSearchQuery) {
    VectorSearchCriteria criteria = vectorSearchQuery.criteria();

    String hasFilter = criteria.filter().isPresent() ? "true" : "false";
    String hasParentFilter = criteria.parentFilter().isPresent() ? "true" : "false";
    String scoreMode =
        criteria
            .embeddedOptions()
            .map(opts -> opts.scoreMode().name().toLowerCase(Locale.ROOT))
            .orElse("max");

    Tags tags =
        Tags.of(
            Tag.of("hasFilter", hasFilter),
            Tag.of("hasParentFilter", hasParentFilter),
            Tag.of("scoreMode", scoreMode));

    this.metrics.metricsFactory.counter("nestedVectorSearchQueryTags", tags).increment();
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
   * <p>This returns how vectors are quantized and stored in the index, not how the query vector is
   * encoded by the client (client-side quantization). Index-side quantization affects search
   * performance and is a key characteristic for latency metrics.
   *
   * @param definition The index definition
   * @param vectorSearchCriteria The vector search criteria (used to identify the field path)
   * @return The quantization type: "unquantized", "scalar_quantized", "binary_quantized", or
   *     "unknown"
   */
  private String determineQuantizationType(
      IndexDefinition definition, VectorSearchCriteria vectorSearchCriteria) {
    if (definition.getType() == VECTOR_SEARCH) {
      VectorIndexDefinition vectorIndexDef = definition.asVectorDefinition();
      FieldPath queryPath = vectorSearchCriteria.path();

      return vectorIndexDef
          .getMappings()
          .getQuantizationForField(queryPath)
          .map(
              quantization ->
                  switch (quantization) {
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
    private final MongotCursorManager cursorManager;
    private final Bytes bsonSizeSoftLimit;

    public Factory(
        MongotCursorManager cursorManager,
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        Bytes bsonSizeSoftLimit,
        Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory) {
      this.cursorManager = cursorManager;
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.bsonSizeSoftLimit = bsonSizeSoftLimit;
      this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
      this.metrics = new Metrics(metricsFactory);
      this.metadata = metadata;
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
            this.metrics,
            this.cursorManager,
            this.bsonSizeSoftLimit);
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
          this.metrics,
          this.cursorManager,
          this.bsonSizeSoftLimit);
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
    private final Counter nestedVectorSearchQueries;
    private final Counter queriesWithDeadline;
    private final Counter vectorQueriesTimedOut;
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
      this.nestedVectorSearchQueries = metricsFactory.counter("nestedVectorSearchQueries");
      this.queriesWithDeadline = metricsFactory.counter("vectorQueriesWithDeadline");
      this.vectorQueriesTimedOut = metricsFactory.counter("vectorQueriesTimedOut");
      this.concurrentExactQueries = metricsFactory.numGauge("concurrentExactQueries");
      this.concurrentApproximateQueries = metricsFactory.numGauge("concurrentApproximateQueries");
      this.concurrentAutoEmbeddingQueries =
          metricsFactory.numGauge("concurrentAutoEmbeddingQueries");
      this.metricsFactory = metricsFactory;
    }
  }
}
