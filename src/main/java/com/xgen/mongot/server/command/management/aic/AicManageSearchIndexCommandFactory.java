package com.xgen.mongot.server.command.management.aic;

import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.AbstractManageSearchIndexCommandFactory;
import com.xgen.mongot.server.command.management.IndexManagementCommandContext;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;

public class AicManageSearchIndexCommandFactory extends AbstractManageSearchIndexCommandFactory {

  private final MetadataService metadataService;
  private final CachedIndexInfoProvider cachedIndexInfoProvider;
  private final CatalogAccessGuard catalogAccessGuard;
  private final boolean listAllIndexes;
  private final IndexDefinitionConfig indexDefinitionConfig;

  public AicManageSearchIndexCommandFactory(
      MetadataService metadataService,
      CachedIndexInfoProvider cachedIndexInfoProvider,
      CatalogAccessGuard catalogAccessGuard,
      boolean internalListAllIndexesForTesting,
      IndexDefinitionConfig indexDefinitionConfig) {

    this.metadataService = metadataService;
    this.cachedIndexInfoProvider = cachedIndexInfoProvider;
    this.catalogAccessGuard = catalogAccessGuard;
    this.listAllIndexes = internalListAllIndexesForTesting;
    this.indexDefinitionConfig = indexDefinitionConfig;
  }

  @Override
  public Command createSearchIndexesCommand(
      IndexManagementCommandContext<CreateSearchIndexesCommandDefinition> commandContext) {
    return new AicCreateSearchIndexesCommand(
        this.metadataService.getAuthoritativeIndexCatalog(),
        this.catalogAccessGuard,
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition(),
        this.indexDefinitionConfig);
  }

  @Override
  public Command listSearchIndexesCommand(
      IndexManagementCommandContext<ListSearchIndexesCommandDefinition> commandContext) {
    return new AicListSearchIndexesCommand(
        this.metadataService,
        this.cachedIndexInfoProvider,
        this.catalogAccessGuard,
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition(),
        this.listAllIndexes);
  }

  @Override
  public Command updateSearchIndexCommand(
      IndexManagementCommandContext<UpdateSearchIndexCommandDefinition> commandContext) {
    return new AicUpdateSearchIndexCommand(
        this.metadataService.getAuthoritativeIndexCatalog(),
        this.catalogAccessGuard,
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition(),
        this.indexDefinitionConfig);
  }

  @Override
  public Command dropSearchIndexCommand(
      IndexManagementCommandContext<DropSearchIndexCommandDefinition> commandContext) {
    return new AicDropSearchIndexCommand(
        this.metadataService.getAuthoritativeIndexCatalog(),
        this.catalogAccessGuard,
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition());
  }
}
