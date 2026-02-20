package com.xgen.mongot.server.command.search;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.CommandFactoryMarker;
import com.xgen.mongot.server.command.management.aic.AicManageSearchIndexCommandFactory;
import com.xgen.mongot.server.command.management.definition.ManageSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.command.search.definition.request.GetMoreCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.KillCursorsCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.SearchCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.mongodb.MongoDbServerInfoProvider;
import io.micrometer.core.instrument.MeterRegistry;

public class SearchCommandsRegister {
  @FunctionalInterface
  public interface CommandRegistrationFunction {
    CommandRegistry register(
        String commandName, CommandFactoryMarker factory, boolean detailedStats);
  }

  public record BootstrapperMetadata(
      String mongotVersion,
      String mongotHostName,
      MongoDbServerInfoProvider mongoDbServerInfoProvider,
      FeatureFlags featureFlags) {}

  public enum RegistrationMode {
    SECURE {
      @Override
      public CommandRegistrationFunction getRegistrationCommand(CommandRegistry cr) {
        return cr::registerCommand;
      }
    },
    INSECURE {
      @Override
      public CommandRegistrationFunction getRegistrationCommand(CommandRegistry cr) {
        return cr::registerInsecureCommand;
      }
    };

    public abstract CommandRegistrationFunction getRegistrationCommand(CommandRegistry cr);
  }

  /** Registers all mongot-specific wire-protocol commands. */
  public static void register(
      RegistrationMode registrationMode,
      CommandRegistry commandRegistry,
      MongotCursorManager cursorManager,
      MeterRegistry meterRegistry,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      BootstrapperMetadata metadata,
      Bytes bsonSizeSoftLimit,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    var registrationCommand = registrationMode.getRegistrationCommand(commandRegistry);

    MetricsFactory searchMetrics = new MetricsFactory("search_metrics", meterRegistry);
    var search =
        new SearchCommand.Factory(
            cursorManager,
            indexCatalog,
            initializedIndexCatalog,
            bsonSizeSoftLimit,
            metadata,
            searchMetrics);
    var getMore = new GetMoreCommand.Factory(cursorManager, bsonSizeSoftLimit, searchMetrics);
    var planShardedSearch = new PlanShardedSearchCommand.Factory();
    var killCursors = new KillCursorsCommand.Factory(cursorManager);
    var vectorSearch =
        new VectorSearchCommand.Factory(
            indexCatalog,
            initializedIndexCatalog,
            embeddingServiceManagerSupplier,
            metadata,
            searchMetrics);

    // TODO(CLOUDP-280897): replace deprecated command with VectorSearchCommand once we disallow
    // running vector queries against search indexes
    var deprecatedVectorSearch =
        new DeprecatedVectorSearchCommand.Factory(
            cursorManager,
            bsonSizeSoftLimit,
            indexCatalog,
            initializedIndexCatalog,
            vectorSearch,
            metadata);

    registrationCommand.register(SearchCommandDefinition.NAME, search, true);
    registrationCommand.register(SearchCommandDefinition.SEARCH_BETA_LEGACY_NAME, search, false);
    registrationCommand.register(GetMoreCommandDefinition.NAME, getMore, true);
    registrationCommand.register(PlanShardedSearchCommandDefinition.NAME, planShardedSearch, true);
    registrationCommand.register(KillCursorsCommandDefinition.NAME, killCursors, false);
    registrationCommand.register(VectorSearchCommandDefinition.NAME, deprecatedVectorSearch, true);
  }

  public static void registerIndexManagementCommands(
      RegistrationMode registrationMode,
      CommandRegistry commandRegistry,
      MetadataService metadataService,
      CachedIndexInfoProvider cachedIndexInfoProvider,
      boolean internalListAllIndexesForTesting) {
    var registrationCommand = registrationMode.getRegistrationCommand(commandRegistry);
    var indexManagement =
        new AicManageSearchIndexCommandFactory(
            metadataService, cachedIndexInfoProvider, internalListAllIndexesForTesting);

    registrationCommand.register(ManageSearchIndexCommandDefinition.NAME, indexManagement, false);
  }
}
