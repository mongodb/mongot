package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.cursor.IntermediateSearchCursorInfo;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.cursor.serialization.MongotIntermediateCursorBatch;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryParseOptions;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.OptimizationFlagsDefinition;
import com.xgen.mongot.server.command.search.definition.request.SearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.UserFacingException;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(SearchCommand.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final Metrics metrics;
  private final MongotCursorManager cursorManager;
  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final QuarantinedIndexLookup quarantinedIndexLookup;
  private final SearchCommandDefinition definition;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Bytes bsonSizeSoftLimit;
  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
  private final List<Long> createdCursorIds;
  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;

  SearchCommand(
      Metrics metrics,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      QuarantinedIndexLookup quarantinedIndexLookup,
      SearchCommandDefinition definition,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Bytes bsonSizeSoftLimit,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    this.metrics = metrics;
    this.cursorManager = cursorManager;
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.quarantinedIndexLookup = quarantinedIndexLookup;
    this.definition = definition;
    this.metadata = metadata;
    this.bsonSizeSoftLimit = bsonSizeSoftLimit;
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.createdCursorIds = new ArrayList<>();
    this.searchEnvoyMetadata = Optional.empty();
  }

  @Override
  public String name() {
    return SearchCommandDefinition.NAME;
  }

  @Override
  public void handleSearchEnvoyMetadata(SearchEnvoyMetadata searchEnvoyMetadata) {
    this.searchEnvoyMetadata = Optional.of(searchEnvoyMetadata);
  }

  @VisibleForTesting
  Counter getSearchCommandsTotalCount() {
    return this.metrics.searchCommandsTotalCount;
  }

  @VisibleForTesting
  Counter getSearchCommandInvalidQueries() {
    return this.metrics.searchCommandInvalidQueries;
  }

  @VisibleForTesting
  Counter getSearchCommandUnwrappedExceptions() {
    return this.metrics.searchCommandUnwrappedExceptions;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace().addKeyValue("command", SearchCommandDefinition.NAME).log("Received command");

    try (var guard = Tracing.searchSpanGuard("SearchCommand.run")) {
      this.metrics.searchCommandsTotalCount.increment();
      QueryOptimizationFlags queryOptimizationFlags =
          this.definition
              .optimizationFlags()
              .map(OptimizationFlagsDefinition::toQueryOptimizationFlags)
              .orElse(QueryOptimizationFlags.DEFAULT_OPTIONS);
      boolean allow10k =
          this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT);
      boolean searchAggregationEnabled =
          this.metadata.featureFlags().isEnabled(Feature.SEARCH_AGGREGATION);
      boolean embeddedScopedSortEnabled =
          this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT);
      SearchQueryParseOptions parseOptions =
          new SearchQueryParseOptions(
              allow10k, searchAggregationEnabled, embeddedScopedSortEnabled);
      @Var
      SearchQuery queryDefinition =
          SearchQuery.fromBson(
              this.definition.queryDocument(), queryOptimizationFlags, parseOptions);
      QueryCursorOptions queryCursorOptions =
          this.definition
              .cursorOptions()
              .map(CursorOptionsDefinition::toQueryCursorOptions)
              .orElse(QueryCursorOptions.empty());

      // Validate before touching the catalog or the embedding service. These checks depend only on
      // the parsed query and cursor options, so running them first ensures a malformed request
      // (e.g. an omitted numCandidates with the AUTO_NUM_CANDIDATES flag disabled) is rejected with
      // the expected user error rather than an index-unavailable or embedding error, and without
      // invoking embedding, matching the previous parser-level rejection order.
      validateQueryAndCursorOptions(queryDefinition, queryCursorOptions);

      Optional<InitializedIndex> index = getIndexFromCatalog(queryDefinition);

      if (index.isPresent()) {
        queryDefinition = maybeEmbedSearchQuery(queryDefinition, index.get().getDefinition());
      }

      try (var unusedExplain =
              Explain.setup(
                  this.definition.explain().map(ExplainDefinition::verbosity),
                  index.map(idx -> idx.getDefinition().getNumPartitions()));
          var unusedFeatureFlags = DynamicFeatureFlagsMetricsRecorder.setup()) {

        addMetadataIfExplain(queryDefinition);

        boolean populateCursorResult =
            determinePopulateCursor(
                Explain.getExplainQueryState(),
                this.metadata
                    .mongoDbServerInfoProvider()
                    .getCachedMongoDbServerInfo()
                    .mongoDbVersion());

        try (var cursorGuard = new CursorGuard(this.createdCursorIds, this.cursorManager)) {
          BsonDocument batch;
          if (this.definition.intermediateVersion().isEmpty()) {
            batch =
                getBatch(
                    queryDefinition,
                    queryCursorOptions,
                    queryOptimizationFlags,
                    populateCursorResult);
          } else {
            var protocolVersion = this.definition.intermediateVersion().get();
            batch =
                getIntermediateBatch(
                    queryDefinition,
                    protocolVersion,
                    queryCursorOptions,
                    queryOptimizationFlags,
                    populateCursorResult);
          }
          if (populateCursorResult) {
            cursorGuard.keepCursors();
          }
          return batch;
        }
      }
    } catch (BsonParseException | InvalidQueryException e) {
      return handleInvalidQueryException(e);
    } catch (Exception e) {
      // Unwrap RuntimeExceptions to check if they mask a user-facing InvalidQueryException.
      if (e instanceof RuntimeException && e.getCause() instanceof InvalidQueryException) {
        this.metrics.searchCommandUnwrappedExceptions.increment();
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
            "InvalidQueryException wrapped in RuntimeException, "
                + "throw InvalidQueryException directly");
        return handleInvalidQueryException((Exception) e.getCause());
      }

      boolean isUserFacing = UserFacingException.isUserFacing(e);
      this.metrics.recordInternalFailure(e);

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
        // The query document is deliberately not logged: it holds customer search terms and
        // filter values. See VectorSearchCommand for the equivalent path.
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Failed to process a searchCommand.");
      }
      return MessageUtils.createErrorBody(e);
    }
  }

  // Utility method to handle invalid query exceptions that are caught explicitly or wrapped.
  private BsonDocument handleInvalidQueryException(Exception e) {
    this.metrics.searchCommandInvalidQueries.increment();
    // The query document is deliberately not logged: it holds customer search terms and filter
    // values. The parse exception attached via withCause names the offending field.
    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
        "searchCommand query is invalid");
    return MessageUtils.createErrorBody(e);
  }

  /**
   * Rewrites a vectorSearch operator against an auto-embedding index so it addresses the derived
   * index: text input is embedded into a query vector using the queried field's auto-embedding
   * model, and a user-supplied query vector is retargeted to the materialized-view vector path and
   * reconciled against the field's indexed encoding.
   *
   * <p>For auto-embedding composite indexes the initialized index exposes the derived definition
   * (auto-embed fields replaced with regular vector fields); the raw definition carrying the
   * auto-embed model metadata is resolved from the catalog.
   */
  @VisibleForTesting
  SearchQuery maybeEmbedSearchQuery(SearchQuery queryDefinition, IndexDefinition indexDefinition)
      throws InvalidQueryException {
    if (!(queryDefinition instanceof OperatorQuery operatorQuery)) {
      return queryDefinition;
    }
    if (!(operatorQuery.operator() instanceof VectorSearchOperator vecOp)) {
      return queryDefinition;
    }
    VectorSearchCriteria criteria = vecOp.criteria();
    Optional<String> textToEmbed = criteria.query().flatMap(VectorSearchQueryInput::getText);

    Optional<IndexDefinition> effectiveDef =
        SearchAutoEmbeddingHelper.resolveAutoEmbeddingDefinition(
            this.indexCatalog,
            this.definition.db(),
            this.definition.collectionUuid(),
            this.definition.viewName(),
            queryDefinition.index(),
            indexDefinition);
    if (effectiveDef.isEmpty()
        || !(effectiveDef.get() instanceof SearchIndexDefinition searchDef)) {
      return queryDefinition;
    }

    var autoEmbeddingFieldsMapping =
        SearchAutoEmbeddingHelper.resolveAutoEmbeddingFieldsMapping(
            this.indexCatalog,
            this.definition.db(),
            this.definition.collectionUuid(),
            this.definition.viewName(),
            queryDefinition.index());

    VectorSearchCriteria rewrittenCriteria;
    if (textToEmbed.isPresent()) {
      // Text query on an auto-embedding index: embed the text and retarget the criteria to the
      // materialized-view vector path.
      Optional<String> queryModel = criteria.query().flatMap(VectorSearchQueryInput::getModel);
      rewrittenCriteria =
          SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
              criteria,
              textToEmbed.get(),
              queryModel,
              searchDef,
              autoEmbeddingFieldsMapping,
              this.embeddingServiceManagerSupplier);
    } else if (criteria.queryVector().isPresent()) {
      // User-supplied queryVector. On an auto-embed field the declared path is retargeted to the
      // materialized-view vector path and the dtype reconciled against the indexed encoding; on a
      // regular type:vector field the criteria is returned unchanged.
      rewrittenCriteria =
          SearchAutoEmbeddingHelper.retargetQueryVectorCriteria(
              criteria, criteria.queryVector().get(), searchDef, autoEmbeddingFieldsMapping);
    } else {
      // VectorSearchCriteria guarantees exactly one of query/queryVector, so a text query must be
      // present here; getText() is empty only when that query carries no text.
      Check.checkState(
          criteria.query().isPresent(), "query must be present when queryVector is absent");
      throw new InvalidQueryException(
          "'query' field cannot be empty when querying Automated Embedding index");
    }
    return operatorQuery.withOperator(new VectorSearchOperator(vecOp.score(), rewrittenCriteria));
  }

  private void validateQueryAndCursorOptions(
      SearchQuery queryDefinition, QueryCursorOptions queryCursorOptions)
      throws InvalidQueryException {

    if (!(queryDefinition instanceof OperatorQuery operatorQuery)) {
      return;
    }

    if (operatorQuery.operator() instanceof VectorSearchOperator
        && queryCursorOptions.requireSequenceTokens()) {
      throw new InvalidQueryException(
          "Pagination is not supported with the 'vectorSearch' operator. "
              + "Use $skip and $limit stages instead.");
    }

    // Mirror the $vectorSearch command: an omitted numCandidates (auto mode) is only allowed when
    // the AUTO_NUM_CANDIDATES flag is enabled. Otherwise reject it as before, since nothing
    // resolves the auto value on the flag-off path.
    if (operatorQuery.operator() instanceof VectorSearchOperator vectorSearchOperator
        && vectorSearchOperator.criteria() instanceof ApproximateVectorSearchCriteria approx
        && approx.isAutoNumCandidates()
        && !this.metadata.featureFlags().isEnabled(Feature.AUTO_NUM_CANDIDATES)) {
      throw new InvalidQueryException("numCandidates is required for approximate vector search");
    }
  }

  private BsonDocument getBatch(
      SearchQuery queryDefinition,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags,
      boolean populateCursorResult)
      throws MongotCursorNotFoundException,
          IndexUnavailableException,
          InvalidQueryException,
          IOException,
          InterruptedException,
          ReaderClosedException {
    try (var guard = Tracing.simpleSpanGuard("SearchCommand.getBatch")) {
      Timer.Sample sample = Timer.start();
      SearchCursorInfo cursorInfo =
          this.cursorManager.newCursor(
              this.definition.db(),
              this.definition.collectionName(),
              this.definition.collectionUuid(),
              this.definition.viewName(),
              queryDefinition,
              queryCursorOptions,
              queryOptimizationFlags,
              this.searchEnvoyMetadata);

      long cursorId = cursorInfo.cursorId;
      this.createdCursorIds.add(cursorId);

      // Get the timer consumer first, as the cursor may be killed when the next batch is retrieved.
      QueryBatchTimerRecorder queryBatchTimerRecorder =
          this.cursorManager.getIndexQueryBatchTimerRecorder(cursorId);

      boolean searchAggregationEnabled =
          this.metadata.featureFlags().isEnabled(Feature.SEARCH_AGGREGATION);
      // Explain-only correlation: chains this query's explain output to its log lines, kill errors
      // and traces, including across getMore batches, which record under the same id. No-op unless
      // the query is explained and an aggregation explainer already exists. Same flag gate as
      // applyAggregationLimit so the extra step is clearly aggregation-only.
      if (searchAggregationEnabled) {
        AggregationFeatureExplainer.recordCursorId(cursorId);
      }

      // Apply the aggregation numBuckets top-N here, at the non-sharded / non-intermediate
      // terminal.
      // cursorInfo.metaResults is computed once, when the cursor is created (this path runs only
      // when
      // intermediateVersion is empty); GetMore batches carry no meta results, so this runs exactly
      // once on the single fully-merged result. Top-N must never run on a per-partition / per-shard
      // result. Gate on SEARCH_AGGREGATION so this extra step is clearly aggregation-only (the flag
      // is also enforced upstream, so an AggregationResult can only be present when it is enabled).
      MetaResults metaResults =
          searchAggregationEnabled
              ? cursorInfo.metaResults.applyAggregationLimit()
              : cursorInfo.metaResults;
      Optional<BsonValue> variables = Optional.of(new Variables(metaResults).toRawBson());
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

      return batch.toBson();
    }
  }

  private BsonDocument getIntermediateBatch(
      SearchQuery queryDefinition,
      int intermediateVersion,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags,
      boolean populateCursorResult)
      throws MongotCursorNotFoundException,
          IndexUnavailableException,
          InvalidQueryException,
          IOException,
          InterruptedException {
    checkState(intermediateVersion == 1, "Only protocolVersion 1 is currently implemented.");

    try (var guard = Tracing.simpleSpanGuard("SearchCommand.getIntermediateBatch")) {
      Timer.Sample sample = Timer.start();
      IntermediateSearchCursorInfo cursorInfo =
          this.cursorManager.newIntermediateCursors(
              this.definition.db(),
              this.definition.collectionName(),
              this.definition.collectionUuid(),
              this.definition.viewName(),
              queryDefinition,
              intermediateVersion,
              queryCursorOptions,
              queryOptimizationFlags,
              this.searchEnvoyMetadata);
      this.createdCursorIds.add(cursorInfo.searchCursorId);
      this.createdCursorIds.add(cursorInfo.metaCursorId);

      // Get the timer consumer first, as the cursor may be killed when the next batch is retrieved.
      QueryBatchTimerRecorder queryBatchTimerRecorder =
          this.cursorManager.getIndexQueryBatchTimerRecorder(cursorInfo.searchCursorId);

      Bytes bytesRemaining =
          this.bsonSizeSoftLimit.subtract(MongotIntermediateCursorBatch.calculateEmptyBatchSize());

      // calculateEmptyBatchSize() above only reserves the *combined* worst-case envelope for both
      // cursors up front; it doesn't stop us from handing the entire remaining budget to just one
      // cursor as its own raw-content limit. Reserve room for the meta cursor's own envelope
      // (cursor id, namespace, etc.) before handing off its content budget, so a greedy batch
      // producer that fills all the way to the limit doesn't push the wrapped result past
      // bytesRemaining once cursor/namespace fields are added on top.
      Bytes metaEnvelopeSize =
          MongotCursorBatch.calculateEmptyBatchSize(
              Optional.empty(), Optional.of(MongotCursorResult.Type.META));
      Bytes metaContentLimit = bytesRemaining.subtract(metaEnvelopeSize);
      MongotCursorResultInfo metaCursorResultInfo =
          this.cursorManager.getNextBatch(
              cursorInfo.metaCursorId, metaContentLimit, QueryCursorOptions.empty());

      MongotCursorBatch metaCursor =
          new MongotCursorBatch(
              populateCursorResult
                  ? Optional.of(
                      metaCursorResultInfo.toCursorResult(
                          cursorInfo.metaCursorId, Optional.of(MongotCursorResult.Type.META)))
                  : Optional.empty(),
              metaCursorResultInfo.explainResult,
              Optional.empty());

      Bytes searchCursorBytesRemaining =
          bytesRemaining.subtract(
              Bytes.ofBytes(metaCursor.toRawBson().getByteBuffer().remaining()));
      MongotCursorResultInfo searchCursorResultInfo =
          this.cursorManager.getNextBatch(
              cursorInfo.searchCursorId, searchCursorBytesRemaining, queryCursorOptions);

      MongotCursorBatch searchCursor =
          new MongotCursorBatch(
              populateCursorResult
                  ? Optional.of(
                      searchCursorResultInfo.toCursorResult(
                          cursorInfo.searchCursorId, Optional.of(MongotCursorResult.Type.RESULTS)))
                  : Optional.empty(),
              searchCursorResultInfo.explainResult,
              Optional.empty());

      MongotIntermediateCursorBatch batch =
          new MongotIntermediateCursorBatch(metaCursor, searchCursor);

      queryBatchTimerRecorder.recordSample(sample);

      return batch.toBson();
    }
  }

  private Optional<InitializedIndex> getIndexFromCatalog(Query query)
      throws IndexUnavailableException {
    try (var guard = Tracing.simpleSpanGuard("SearchCommand.getIndexFromCatalog")) {
      Optional<IndexGeneration> indexGeneration =
          this.indexCatalog.getIndex(
              this.definition.db(),
              this.definition.collectionUuid(),
              this.definition.viewName(),
              query.index());

      if (indexGeneration.isEmpty()) {
        // An index whose journal entry could not be parsed has no catalog entry, and reporting it
        // as nonexistent would hide why it is unavailable.
        this.quarantinedIndexLookup.throwIfQuarantined(
            this.definition.db(), this.definition.collectionUuid(), query.index());
        this.metrics.nonexistentIndexQueries.increment();
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
  }

  private void addMetadataIfExplain(SearchQuery query) {
    Explain.getExplainQueryState()
        .map(ExplainQueryState::getQueryInfo)
        .map(
            queryInfo ->
                queryInfo.getFeatureExplainer(
                    MetadataFeatureExplainer.class, MetadataFeatureExplainer::new))
        .ifPresent(
            metadataExplainer -> {
              metadataExplainer.setIndexName(query.index());
              metadataExplainer.setMongotVersion(this.metadata.mongotVersion());
              metadataExplainer.setMongotHostName(this.metadata.mongotHostName());
              metadataExplainer.setCursorOptions(this.definition.cursorOptions());
              metadataExplainer.setOptimizationFlags(this.definition.optimizationFlags());
            });
  }

  static boolean determinePopulateCursor(
      Optional<ExplainQueryState> explainState, Optional<MongoDbVersion> mongoDbVersion) {
    if (explainState.isEmpty()) {
      return true; // Populate cursor by default if non-explain query
    }

    if (explainState
        .get()
        .getQueryInfo()
        .getVerbosity()
        .isLessThanOrEqual(Explain.Verbosity.QUERY_PLANNER)) {
      return false;
    }

    return mongoDbVersion
        .map(dbVersion -> dbVersion.compareTo(Explain.FIRST_VERSION_KILLS_CURSORS_EXPLAIN) >= 0)
        .orElse(true); // populate cursor by default if version information isn't present
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }

  @Override
  public List<Long> getCreatedCursorIds() {
    return List.copyOf(this.createdCursorIds);
  }

  public static class Factory implements CommandFactory {

    private final MongotCursorManager cursorManager;
    private final IndexCatalog indexCatalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final QuarantinedIndexLookup quarantinedIndexLookup;
    private final Bytes bsonSizeSoftLimit;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final SearchCommand.Metrics metrics;
    private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

    public Factory(
        MongotCursorManager cursorManager,
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        QuarantinedIndexLookup quarantinedIndexLookup,
        Bytes bsonSizeSoftLimit,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory,
        Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
      this.cursorManager = cursorManager;
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.quarantinedIndexLookup = quarantinedIndexLookup;
      this.bsonSizeSoftLimit = bsonSizeSoftLimit;
      this.metadata = metadata;
      this.metrics = new SearchCommand.Metrics(metricsFactory);
      this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    }

    @Override
    public Command create(BsonDocument args) {
      try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
        SearchCommandDefinition definition = SearchCommandDefinition.fromBson(parser);
        return new SearchCommand(
            this.metrics,
            this.cursorManager,
            this.indexCatalog,
            this.initializedIndexCatalog,
            this.quarantinedIndexLookup,
            definition,
            this.metadata,
            this.bsonSizeSoftLimit,
            this.embeddingServiceManagerSupplier);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }

  static class Metrics {
    private final MetricsFactory metricsFactory;
    // Counter for total search commands received.
    private final Counter searchCommandsTotalCount;
    // Either the query has BSON parsing error, or doesn't conform to the query syntax (e.g. missing
    // a
    // required field, or having an unrecognized field).
    private final Counter searchCommandInvalidQueries;
    // Tracks instances where a RuntimeException wrapping an InvalidQueryException was caught and
    // unwrapped.
    private final Counter searchCommandUnwrappedExceptions;
    private final Counter nonexistentIndexQueries;
    private final Counter queriesAgainstView;
    private final Counter queriesAgainstViewSourceCollection;

    Metrics(MetricsFactory metricsFactory) {
      this.metricsFactory = metricsFactory;
      this.searchCommandsTotalCount = metricsFactory.counter("searchCommandTotalCount");
      this.searchCommandInvalidQueries = metricsFactory.counter("searchCommandInvalidQueries");
      this.searchCommandUnwrappedExceptions =
          metricsFactory.counter("searchCommandWrappedInvalidQueryException");
      this.nonexistentIndexQueries = metricsFactory.counter("searchCommandNonexistentIndex");
      this.queriesAgainstView = metricsFactory.counter("queriesAgainstView");
      this.queriesAgainstViewSourceCollection =
          metricsFactory.counter("queriesAgainstViewSourceCollection");
    }

    /** Emit failure metrics with the same set of label keys. */
    void recordInternalFailure(Throwable throwable) {
      ErrorType errorType =
          UserFacingException.isUserFacing(throwable)
              ? ErrorType.USER_FACING_ERROR
              : ErrorType.INTERNAL_ERROR;
      this.metricsFactory
          .counter(
              "searchCommandInternalFailures",
              Tags.of(
                  "exceptionName",
                  throwable.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  errorType.toString()))
          .increment();
    }
  }
}
