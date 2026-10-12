package com.xgen.mongot.server.command.search;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryParseOptions;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanShardedSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(PlanShardedSearchCommand.class);

  private final PlanShardedSearchCommandDefinition definition;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;

  PlanShardedSearchCommand(
      PlanShardedSearchCommandDefinition definition,
      SearchCommandsRegister.BootstrapperMetadata metadata) {
    this.definition = definition;
    this.metadata = metadata;
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
      boolean embeddedScopedSortEnabled =
          this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT);
      boolean enforceRequestedBucketLimit =
          this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(
                  DynamicFeatureFlags.ENABLE_SEARCH_AGGREGATION_REQUESTED_BUCKET_LIMIT);
      SearchQuery queryDefinition =
          SearchQuery.fromBson(
              this.definition.queryDocument(),
              QueryOptimizationFlags.DEFAULT_OPTIONS,
              new SearchQueryParseOptions(
                  allow10k, searchAggregationEnabled, embeddedScopedSortEnabled));
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan shardedSearchPlan =
          ShardedSearchPlanner.planSearch(
              queryDefinition,
              this.definition.searchFeatures(),
              this.metadata.featureFlags(),
              enforceRequestedBucketLimit);

      return PlanShardedSearchCommandResponseDefinition.create(shardedSearchPlan).toBson();
    } catch (ShardedSearchPlannerException | BsonParseException e) {
      LOG.info("Invalid ShardedSearchPlanDefinition:", e);
      return MessageUtils.createErrorBody(e);
    }
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }

  public static class Factory implements CommandFactory {

    private final SearchCommandsRegister.BootstrapperMetadata metadata;

    public Factory(SearchCommandsRegister.BootstrapperMetadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public Command create(BsonDocument doc) {
      try (var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build()) {
        PlanShardedSearchCommandDefinition definition =
            PlanShardedSearchCommandDefinition.fromBson(parser);
        return new PlanShardedSearchCommand(definition, this.metadata);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
