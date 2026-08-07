package com.xgen.mongot.server.command.search;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.lucene.LuceneAggregationPlanResolver;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import com.xgen.mongot.index.query.collectors.AggregationValidator;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanShardedSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(PlanShardedSearchCommand.class);

  private final PlanShardedSearchCommandDefinition definition;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final IndexCatalog indexCatalog;

  PlanShardedSearchCommand(
      PlanShardedSearchCommandDefinition definition,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      IndexCatalog indexCatalog) {
    this.definition = definition;
    this.metadata = metadata;
    this.indexCatalog = indexCatalog;
  }

  @Override
  public String name() {
    return PlanShardedSearchCommandDefinition.NAME;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", PlanShardedSearchCommandDefinition.NAME)
        .log("Received command");

    try {
      boolean allow10k =
          this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT);
      boolean searchAggregationEnabled =
          this.metadata.featureFlags().isEnabled(Feature.SEARCH_AGGREGATION);
      SearchQuery queryDefinition =
          SearchQuery.fromBson(
              this.definition.queryDocument(),
              QueryOptimizationFlags.DEFAULT_OPTIONS,
              allow10k,
              searchAggregationEnabled);
      Optional<AggregationSpecBuilder.BoundaryFieldResolver> boundaryResolver =
          resolveBoundaryFieldResolver(queryDefinition);
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan shardedSearchPlan =
          ShardedSearchPlanner.planSearch(
              queryDefinition,
              this.definition.searchFeatures(),
              this.metadata.featureFlags(),
              boundaryResolver);

      return PlanShardedSearchCommandResponseDefinition.create(shardedSearchPlan).toBson();
    } catch (ShardedSearchPlannerException | BsonParseException e) {
      LOG.info("Invalid ShardedSearchPlanDefinition:", e);
      return MessageUtils.createErrorBody(e);
    }
  }

  /**
   * Resolves index field metadata for boundary grouping when the query needs it. Terms grouping
   * does not require index metadata at plan time.
   */
  private Optional<AggregationSpecBuilder.BoundaryFieldResolver> resolveBoundaryFieldResolver(
      SearchQuery queryDefinition) throws ShardedSearchPlannerException {
    if (queryDefinition instanceof CollectorQuery collectorQuery
        && collectorQuery.collector() instanceof AggregationCollector aggregationCollector
        && aggregationCollector.usesBoundaryGrouping()) {
      Optional<IndexGeneration> indexGeneration = resolveIndexForBoundaryPlanning(queryDefinition);
      if (indexGeneration.isEmpty()) {
        throw new ShardedSearchPlannerException(
            String.format(
                "Search index \"%s\" not found for database \"%s\"",
                queryDefinition.index(), this.definition.db()));
      }
      SearchIndexDefinition indexDefinition =
          indexGeneration.get().getDefinitionGeneration().asSearch().getIndexDefinition();
      IndexFormatVersion indexFormatVersion =
          indexGeneration.get().getGenerationId().generation.indexFormatVersion;
      validateGroupingFields(
          aggregationCollector,
          indexDefinition,
          indexFormatVersion,
          queryDefinition.returnScope().map(ReturnScope::path));
      return Optional.of(
          LuceneAggregationPlanResolver.boundaryFieldResolverForPlanning(
              indexDefinition, indexFormatVersion));
    }
    return Optional.empty();
  }

  /**
   * Runs the same grouping-field schema check the query path runs per reader ({@code
   * LuceneSearchIndexReader#aggregationCollectorQuery}). Without it, a grouping field that carries
   * no v2 doc values -- a legacy {@code numberFacet}/{@code dateFacet} field -- reaches {@code
   * SearchAggregationBootstrap} boundary setup, whose {@code orElseThrow()} raises a bare {@code
   * NoSuchElementException} that escapes {@code planShardedSearch} as an UnknownError instead of a
   * query error.
   */
  private void validateGroupingFields(
      AggregationCollector aggregationCollector,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      Optional<FieldPath> returnScope)
      throws ShardedSearchPlannerException {
    try {
      AggregationValidator.validateAggregationQuery(
          aggregationCollector,
          indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
          returnScope,
          this.metadata.featureFlags().isEnabled(Feature.SEARCH_AGGREGATION));
    } catch (AggregationValidator.AggregationValidationException e) {
      throw new ShardedSearchPlannerException(Objects.requireNonNull(e.getMessage()), e);
    }
  }

  /**
   * Mongod may omit {@code collectionUUID} on {@code planShardedSearch}; when present we resolve by
   * UUID, otherwise fall back to database + {@code planShardedSearch} collection name + index name.
   */
  private Optional<IndexGeneration> resolveIndexForBoundaryPlanning(SearchQuery queryDefinition) {
    Optional<String> viewName = this.definition.viewName();
    String indexName = queryDefinition.index();

    Optional<IndexGeneration> indexByUuid =
        this.definition
            .collectionUuid()
            .flatMap(
                collectionUuid ->
                    this.indexCatalog.getIndex(
                        this.definition.db(), collectionUuid, viewName, indexName));
    if (indexByUuid.isPresent()) {
      return indexByUuid;
    }
    if (this.definition.collectionUuid().isPresent()) {
      return Optional.empty();
    }

    return this.indexCatalog.getIndexes().stream()
        .filter(indexGeneration -> matchesIndexForPlanning(indexGeneration, viewName, indexName))
        .findFirst();
  }

  private boolean matchesIndexForPlanning(
      IndexGeneration indexGeneration, Optional<String> viewName, String indexName) {
    IndexDefinition indexDefinition = indexGeneration.getDefinition();
    if (!indexDefinition.getDatabase().equals(this.definition.db())
        || !indexDefinition.getName().equals(indexName)
        || !indexDefinition
            .getLastObservedCollectionName()
            .equals(this.definition.collectionName())) {
      return false;
    }
    if (viewName.isPresent()) {
      return indexDefinition.getView().map(ViewDefinition::getName).equals(viewName);
    }
    return true;
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }

  public static class Factory implements CommandFactory {

    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final IndexCatalog indexCatalog;

    public Factory(
        SearchCommandsRegister.BootstrapperMetadata metadata, IndexCatalog indexCatalog) {
      this.metadata = metadata;
      this.indexCatalog = indexCatalog;
    }

    @Override
    public Command create(BsonDocument doc) {
      try (var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build()) {
        PlanShardedSearchCommandDefinition definition =
            PlanShardedSearchCommandDefinition.fromBson(parser);
        return new PlanShardedSearchCommand(definition, this.metadata, this.indexCatalog);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
