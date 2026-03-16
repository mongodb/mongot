package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
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
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
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
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(SearchCommand.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final MetricsFactory metricsFactory;
  private final Metrics metrics;
  private final MongotCursorManager cursorManager;
  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final SearchCommandDefinition definition;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Bytes bsonSizeSoftLimit;
  private final List<Long> createdCursorIds;
  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;

  SearchCommand(
      Metrics metrics,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandDefinition definition,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Bytes bsonSizeSoftLimit) {
    this.metricsFactory = metrics.metricsFactory;
    this.metrics = metrics;
    this.cursorManager = cursorManager;
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.definition = definition;
    this.metadata = metadata;
    this.bsonSizeSoftLimit = bsonSizeSoftLimit;
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

    try (var guard = Tracing.simpleSpanGuard("SearchCommand.run", Tracing.TOGGLE_OFF)) {
      this.metrics.searchCommandsTotalCount.increment();
      QueryOptimizationFlags queryOptimizationFlags =
          this.definition
              .optimizationFlags()
              .map(OptimizationFlagsDefinition::toQueryOptimizationFlags)
              .orElse(QueryOptimizationFlags.DEFAULT_OPTIONS);
      SearchQuery queryDefinition =
          SearchQuery.fromBson(this.definition.queryDocument(), queryOptimizationFlags);
      Optional<InitializedIndex> index = getIndexFromCatalog(queryDefinition);
      QueryCursorOptions queryCursorOptions =
          this.definition
              .cursorOptions()
              .map(CursorOptionsDefinition::toQueryCursorOptions)
              .orElse(QueryCursorOptions.empty());

      validateQueryAndCursorOptions(queryDefinition, queryCursorOptions);

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
      this.metricsFactory
          .counter(
              "searchCommandInternalFailures",
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
            "Failed to process a searchCommand. queryDocument: %s",
            this.definition.queryDocument());
      }
      return MessageUtils.createErrorBody(e);
    } catch (Throwable e) {
      this.metricsFactory
          .counter(
              "searchCommandInternalFailures",
              Tags.of("throwableName", e.getClass().getSimpleName()))
          .increment();
      throw e;
    }
  }

  // Utility method to handle invalid query exceptions that are caught explicitly or wrapped.
  private BsonDocument handleInvalidQueryException(Exception e) {
    this.metrics.searchCommandInvalidQueries.increment();
    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
        "searchCommand query is invalid. queryDocument: %s", this.definition.queryDocument());
    return MessageUtils.createErrorBody(e);
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

    Optional<BsonValue> variables = Optional.of(new Variables(cursorInfo.metaResults).toRawBson());
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
      MongotCursorResultInfo metaCursorResultInfo =
          this.cursorManager.getNextBatch(
              cursorInfo.metaCursorId, bytesRemaining, QueryCursorOptions.empty());

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
    private final Bytes bsonSizeSoftLimit;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final SearchCommand.Metrics metrics;

    public Factory(
        MongotCursorManager cursorManager,
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        Bytes bsonSizeSoftLimit,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory) {
      this.cursorManager = cursorManager;
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.bsonSizeSoftLimit = bsonSizeSoftLimit;
      this.metadata = metadata;
      this.metrics = new SearchCommand.Metrics(metricsFactory);
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
            definition,
            this.metadata,
            this.bsonSizeSoftLimit);
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
    // A query that is valid, but mongot internally hit an error when processing/executing this
    // query.
    private final Counter queriesAgainstView;
    private final Counter queriesAgainstViewSourceCollection;

    Metrics(MetricsFactory metricsFactory) {
      this.metricsFactory = metricsFactory;
      this.searchCommandsTotalCount = metricsFactory.counter("searchCommandTotalCount");
      this.searchCommandInvalidQueries = metricsFactory.counter("searchCommandInvalidQueries");
      this.searchCommandUnwrappedExceptions =
          metricsFactory.counter("searchCommandWrappedInvalidQueryException");
      this.queriesAgainstView = metricsFactory.counter("queriesAgainstView");
      this.queriesAgainstViewSourceCollection =
          metricsFactory.counter("queriesAgainstViewSourceCollection");
    }
  }
}
