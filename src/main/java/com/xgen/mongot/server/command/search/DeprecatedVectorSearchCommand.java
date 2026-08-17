package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command allows running vector queries against search indexes. If the looked up index is in
 * fact VectorIndex, it will delegate the query to {@link VectorSearchCommand}. The command should
 * be removed once we disallow running vector queries against search indexes.
 */
public class DeprecatedVectorSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(DeprecatedVectorSearchCommand.class);

  private final VectorSearchCommandDefinition definition;
  private final MongotCursorManager cursorManager;
  private final Bytes bsonSizeSoftLimit;
  private final List<Long> createdCursorIds;
  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final VectorSearchCommand.Factory vectorCommandFactory;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;

  private @Var Optional<SearchEnvoyMetadata> searchEnvoyMetadata;

  DeprecatedVectorSearchCommand(
      MongotCursorManager cursorManager,
      VectorSearchCommandDefinition definition,
      Bytes bsonSizeSoftLimit,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      VectorSearchCommand.Factory vectorCommandFactory,
      SearchCommandsRegister.BootstrapperMetadata metadata) {
    this.cursorManager = cursorManager;
    this.definition = definition;
    this.bsonSizeSoftLimit = bsonSizeSoftLimit;
    this.createdCursorIds = new ArrayList<>();

    this.searchEnvoyMetadata = Optional.empty();
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.vectorCommandFactory = vectorCommandFactory;
    this.metadata = metadata;
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
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", VectorSearchCommandDefinition.NAME)
        .log("Received command");

    try {
      VectorSearchQuery query = this.definition.getQuery();
      Optional<IndexGeneration> index =
          this.indexCatalog.getIndex(
              this.definition.db(),
              this.definition.collectionUuid(),
              this.definition.viewName(),
              query.index());
      Optional<InitializedIndex> initializedIndex =
          index.flatMap(
              indexGeneration ->
                  this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()));

      if (index.isPresent() && initializedIndex.isEmpty()) {
        throw new IndexUnavailableException(
            String.format("Index %s not initialized", index.get().getDefinition().getName()));
      }

      if (initializedIndex.isPresent()) {
        if (initializedIndex.get().getType() == Type.VECTOR) {
          // If query is running against vector index, delegate execution to the new command.
          Command vectorCommand = this.vectorCommandFactory.create(this.definition);
          // Propagate important info between the two Commands.
          // Envoy metadata must be ready before the run.
          this.searchEnvoyMetadata.ifPresent(vectorCommand::handleSearchEnvoyMetadata);
          // Run the VectorSearchCommand.
          BsonDocument response = vectorCommand.run();
          // Cursor ID's are only available after the run.
          Check.checkState(
              this.createdCursorIds.isEmpty(),
              "Expected createdCursorIds to be empty before "
                  + "delegating to VectorSearchCommand, found %s",
              this.createdCursorIds);
          this.createdCursorIds.addAll(vectorCommand.getCreatedCursorIds());
          return response;
        } else {
          var metricsUpdater =
              initializedIndex.get().getMetricsUpdater().getQueryingMetricsUpdater();
          metricsUpdater.getVectorSearchQueriesOverSearchIndexes().increment();
        }
      }

      try (var ignored =
          Explain.setup(
              this.definition.explain().map(ExplainDefinition::verbosity),
              initializedIndex.map(idx -> idx.getDefinition().getNumPartitions()))) {
        addMetadataIfExplain(query);
        boolean populateCursorResult =
            SearchCommand.determinePopulateCursor(
                Explain.getExplainQueryState(),
                this.metadata
                    .mongoDbServerInfoProvider()
                    .getCachedMongoDbServerInfo()
                    .mongoDbVersion());
        try (var cursorGuard = new CursorGuard(this.createdCursorIds, this.cursorManager)) {
          BsonDocument batch = getBatch(query, populateCursorResult);
          if (populateCursorResult) {
            cursorGuard.keepCursors();
          }
          return batch;
        }
      }
    } catch (InvalidQueryException | IndexUnavailableException | BsonParseException e) {
      return MessageUtils.createErrorBody(e);
    } catch (Exception e) {
      // we didn't expect this error, so we will log the entire stack trace for diagnostics
      LOG.error("Unexpected error", e);
      return MessageUtils.createErrorBody(e);
    }
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }

  @Override
  public List<Long> getCreatedCursorIds() {
    return List.copyOf(this.createdCursorIds);
  }

  private BsonDocument getBatch(Query query, boolean populateCursorResult)
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
            query,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            this.searchEnvoyMetadata);
    this.createdCursorIds.add(cursorInfo.cursorId);

    // Get the timer consumer first, as the cursor may be killed when the next batch is retrieved.
    QueryBatchTimerRecorder queryBatchTimerRecorder =
        this.cursorManager.getIndexQueryBatchTimerRecorder(cursorInfo.cursorId);

    Bytes maxBatchSize =
        this.bsonSizeSoftLimit.subtract(
            MongotCursorBatch.calculateEmptyBatchSize(
                Optional.empty(), Optional.of(MongotCursorResult.Type.RESULTS)));

    var cursorResultInfo =
        this.cursorManager.getNextBatch(
            cursorInfo.cursorId, maxBatchSize, QueryCursorOptions.empty());

    Optional<MongotCursorResult> cursorResult =
        populateCursorResult
            ? Optional.of(cursorResultInfo.toCursorResult(cursorInfo.cursorId, Optional.empty()))
            : Optional.empty();

    MongotCursorBatch batch =
        new MongotCursorBatch(cursorResult, cursorResultInfo.explainResult, Optional.empty());

    queryBatchTimerRecorder.recordSample(sample);

    return batch.toBson();
  }

  private void addMetadataIfExplain(VectorSearchQuery query) {
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
            });
  }

  public static class Factory implements CommandFactory {

    private final MongotCursorManager cursorManager;
    private final Bytes bsonSizeSoftLimit;
    private final IndexCatalog indexCatalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final VectorSearchCommand.Factory vectorSearchFactory;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;

    public Factory(
        MongotCursorManager cursorManager,
        Bytes bsonSizeSoftLimit,
        IndexCatalog indexCatalog,
        InitializedIndexCatalog initializedIndexCatalog,
        VectorSearchCommand.Factory vectorSearchFactory,
        SearchCommandsRegister.BootstrapperMetadata metadata) {
      this.cursorManager = cursorManager;
      this.bsonSizeSoftLimit = bsonSizeSoftLimit;
      this.indexCatalog = indexCatalog;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.vectorSearchFactory = vectorSearchFactory;
      this.metadata = metadata;
    }

    @Override
    public Command create(BsonDocument args) {
      try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
        VectorSearchCommandDefinition definition =
            VectorSearchCommandDefinition.fromBson(
                parser, this.metadata.featureFlags().isEnabled(Feature.VECTOR_STORED_SOURCE));
        return new DeprecatedVectorSearchCommand(
            this.cursorManager,
            definition,
            this.bsonSizeSoftLimit,
            this.indexCatalog,
            this.initializedIndexCatalog,
            this.vectorSearchFactory,
            this.metadata);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
