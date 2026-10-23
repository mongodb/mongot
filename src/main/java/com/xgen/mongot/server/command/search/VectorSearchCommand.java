package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Type.VECTOR_SEARCH;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.NamespaceBuilder;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.EmptyExplainInformation;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.QuarantinedIndexException;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexNamespaceDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.IndexNamespaceFilter;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.version.IndexEngine;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.executors.LoadSheddingRejectedException;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
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
  private final QuarantinedIndexLookup quarantinedIndexLookup;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Metrics metrics;

  private final MongotCursorManager cursorManager;
  private final Bytes bsonSizeSoftLimit;
  private final List<Long> createdCursorIds;

  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;
  private final VectorQueryEmbedder embedder;

  VectorSearchCommand(
      VectorSearchCommandDefinition definition,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      QuarantinedIndexLookup quarantinedIndexLookup,
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
    this.quarantinedIndexLookup = quarantinedIndexLookup;
    this.metadata = metadata;
    this.metrics = metrics;
    this.searchEnvoyMetadata = Optional.empty();
    this.embedder =
        new VectorQueryEmbedder(
            indexCatalog,
            embeddingServiceManagerSupplier,
            definition.db(),
            definition.collectionUuid(),
            definition.viewName());
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
      if (criteria instanceof ApproximateVectorSearchCriteria approx
          && approx.isAutoNumCandidates()
          && !this.metadata.featureFlags().isEnabled(Feature.AUTO_NUM_CANDIDATES)) {
        throw new InvalidQueryException("numCandidates is required for approximate vector search");
      }
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
    } catch (LoadSheddingRejectedException e) {
      // Let the server handler return the overload error code and retry labels.
      throw e;
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
      this.metrics.recordInternalFailure(e);

      QuarantinedIndexException.rethrowIfEnvoyCanRetry(e, this.searchEnvoyMetadata.isPresent());

      if (e instanceof IndexUnavailableException
          && this.searchEnvoyMetadata.isPresent()
          && this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.INDEX_UNAVAILABLE_RETRY)) {
        // Preserve the failure for gRPC to choose a peer retry or the final BSON error.
        throw new CompletionException(e);
      }

      if (!isUserFacing) {
        // Mongot internal processing error should be rare, log with a low limit. We still need a
        // limit because the warning log level may cause a flush().
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Failed to process a vectorSearchCommand.");
      }

      return MessageUtils.createErrorBody(e);
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

      validateIndexNamespace(vectorSearchQuery, optionalIndex.get().getDefinition());

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
          this.embedder.materializeQuery(
              vectorSearchQuery,
              this.embedder.findEmbedRequestInfo(
                  optionalIndex.get().getDefinition(), vectorSearchQuery));
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
      try {
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
      } catch (IndexClosedException e) {
        // The index was swapped out and closed after we pulled it from the catalog (auto-embed
        // queries widen this window with the query embedding call). Surface it as
        // ReaderClosedException so run() retries against the new generation.
        throw ReaderClosedException.create("getReader");
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

  @VisibleForTesting
  Optional<VectorQueryEmbedder.EmbedRequestInfo> findEmbedRequestInfo(
      IndexDefinition regularIndexDefinition, VectorSearchQuery vectorSearchQuery)
      throws InvalidQueryException {
    return this.embedder.findEmbedRequestInfo(regularIndexDefinition, vectorSearchQuery);
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
      // An index whose journal entry could not be parsed has no catalog entry, and reporting it as
      // nonexistent would hide why it is unavailable.
      this.quarantinedIndexLookup.throwIfQuarantined(
          this.definition.db(), this.definition.collectionUuid(), query.index());
      this.metrics.nonexistentIndexQueries.increment();
      return Optional.empty();
    }
    var index = indexGeneration.get();

    try {
      index.getIndex().throwIfUnavailableForQuerying();
    } catch (IndexUnavailableException e) {
      this.metrics.recordUnavailableIndex(index.getDefinition().engine());
      throw e;
    }
    InitializedIndex initializedIndex =
        this.initializedIndexCatalog
            .getIndex(index.getGenerationId())
            .orElseThrow(
                () -> {
                  this.metrics.recordUnavailableIndex(index.getDefinition().engine());
                  return new IndexUnavailableException(
                      String.format("Index %s not initialized", index.getDefinition().getName()));
                });
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
      case FLOAT16 -> this.metrics.bsonFloat16VectorQueries.increment();
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
   * An index defined with an indexNamespace requires every query to select a namespace with a
   * matching indexNamespace equality; an index without one must not receive it.
   */
  @VisibleForTesting
  static void validateIndexNamespace(
      VectorSearchQuery vectorSearchQuery, IndexDefinition definition)
      throws InvalidQueryException {
    Optional<IndexNamespaceDefinition> indexNamespace =
        definition.getType() == VECTOR_SEARCH
            ? definition.asVectorDefinition().getIndexNamespace()
            : Optional.empty();
    Optional<IndexNamespaceFilter> queryNamespace = vectorSearchQuery.indexNamespace();
    if (indexNamespace.isPresent()) {
      if (queryNamespace.isEmpty()) {
        throw new InvalidQueryException(
            String.format(
                "index is defined with indexNamespace; the query must supply"
                    + " indexNamespace: { %s: { $eq: <value> } }",
                indexNamespace.get().path()));
      }
      if (!queryNamespace.get().path().equals(indexNamespace.get().path())) {
        throw new InvalidQueryException(
            String.format(
                "indexNamespace field '%s' does not match the index definition's"
                    + " indexNamespace path '%s'",
                queryNamespace.get().path(), indexNamespace.get().path()));
      }
    } else if (queryNamespace.isPresent()) {
      throw new InvalidQueryException("index is not defined with indexNamespace");
    }
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
    private final QuarantinedIndexLookup quarantinedIndexLookup;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
    private final Metrics metrics;
    private final MongotCursorManager cursorManager;
    private final Bytes bsonSizeSoftLimit;

    public Factory(
        MongotCursorManager cursorManager,
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        QuarantinedIndexLookup quarantinedIndexLookup,
        Bytes bsonSizeSoftLimit,
        Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory) {
      this.cursorManager = cursorManager;
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.quarantinedIndexLookup = quarantinedIndexLookup;
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
            this.quarantinedIndexLookup,
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
          this.quarantinedIndexLookup,
          this.metadata,
          this.embeddingServiceManagerSupplier,
          this.metrics,
          this.cursorManager,
          this.bsonSizeSoftLimit);
    }

    /**
     * Counts a query rejected because its index is defined but unavailable, for callers that reject
     * it before a {@link VectorSearchCommand} is created.
     */
    void recordUnavailableIndex(IndexEngine engine) {
      this.metrics.recordUnavailableIndex(engine);
    }

    void recordNonexistentIndex() {
      this.metrics.nonexistentIndexQueries.increment();
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
    private final Counter nonexistentIndexQueries;
    private final Counter queriesAgainstView;
    private final Counter queriesAgainstViewSourceCollection;
    private final Counter exactVectorSearchQueries;
    private final Counter approximateVectorSearchQueries;
    private final Counter autoEmbeddingVectorSearchQueries;
    private final Counter autoEmbeddingTextQueries;
    private final Counter autoEmbeddingMultiModalQueries;
    private final Counter bsonFloatVectorQueries;
    private final Counter bsonFloat16VectorQueries;
    private final Counter bsonByteVectorQueries;
    private final Counter bsonBitVectorQueries;
    private final Counter vectorStoredSourceQueries;
    private final Counter nestedVectorSearchQueries;
    private final Counter queriesWithDeadline;
    private final Counter vectorQueriesTimedOut;
    private final AtomicLong concurrentExactQueries;
    private final AtomicLong concurrentApproximateQueries;
    private final AtomicLong concurrentAutoEmbeddingQueries;
    private final Map<IndexEngine, Counter> unavailableIndexQueries;
    private final MetricsFactory metricsFactory;

    Metrics(MetricsFactory metricsFactory) {
      // The counter for $vectorSearch queries over vector indexes.
      this.totalCount = metricsFactory.counter("vectorSearchCommandTotalCount");
      this.invalidQueries = metricsFactory.counter("vectorSearchCommandInvalidQueries");
      this.wrappedInvalidQueryExceptions =
          metricsFactory.counter("vectorSearchCommandWrappedInvalidQueryException");
      this.nonexistentIndexQueries = metricsFactory.counter("vectorSearchCommandNonexistentIndex");
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
      this.bsonFloat16VectorQueries = metricsFactory.counter("bsonFloat16VectorQueries");
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
      // Registered for every engine up front so each series exists from process start; increase()
      // over a series that first appears mid-range undercounts.
      this.unavailableIndexQueries = new EnumMap<>(IndexEngine.class);
      for (IndexEngine engine : IndexEngine.values()) {
        this.unavailableIndexQueries.put(
            engine,
            metricsFactory.counter(
                "vectorSearchCommandUnavailableIndex", Tags.of("indexEngine", engine.name())));
      }
      this.metricsFactory = metricsFactory;
    }

    /** Counts a query rejected because its index is defined but not available for querying. */
    void recordUnavailableIndex(IndexEngine engine) {
      this.unavailableIndexQueries.get(engine).increment();
    }

    /** Emit failure metrics with the same set of label keys. */
    void recordInternalFailure(Throwable throwable) {
      ErrorType errorType =
          UserFacingException.isUserFacing(throwable)
              ? ErrorType.USER_FACING_ERROR
              : ErrorType.INTERNAL_ERROR;
      this.metricsFactory
          .counter(
              "vectorSearchCommandInternalFailures",
              Tags.of(
                  "exceptionName",
                  throwable.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  errorType.toString()))
          .increment();
    }
  }
}
