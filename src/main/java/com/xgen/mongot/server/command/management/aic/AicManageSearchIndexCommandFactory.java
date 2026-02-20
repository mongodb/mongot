package com.xgen.mongot.server.command.management.aic;

import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
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
  private final boolean listAllIndexes;

  public AicManageSearchIndexCommandFactory(
      MetadataService metadataService,
      CachedIndexInfoProvider cachedIndexInfoProvider,
      boolean internalListAllIndexesForTesting) {

    this.metadataService = metadataService;
    this.cachedIndexInfoProvider = cachedIndexInfoProvider;
    this.listAllIndexes = internalListAllIndexesForTesting;
  }

  @Override
  public Command createSearchIndexesCommand(
      IndexManagementCommandContext<CreateSearchIndexesCommandDefinition> commandContext) {
    return new AicCreateSearchIndexesCommand(
        this.metadataService.getAuthoritativeIndexCatalog(),
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition());
  }

  @Override
  public Command listSearchIndexesCommand(
      IndexManagementCommandContext<ListSearchIndexesCommandDefinition> commandContext) {
    return new AicListSearchIndexesCommand(
        this.metadataService,
        this.cachedIndexInfoProvider,
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
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition());
  }

  @Override
  public Command dropSearchIndexCommand(
      IndexManagementCommandContext<DropSearchIndexCommandDefinition> commandContext) {
    return new AicDropSearchIndexCommand(
        this.metadataService.getAuthoritativeIndexCatalog(),
        commandContext.dbName(),
        commandContext.collectionUuid(),
        commandContext.collectionName(),
        commandContext.view(),
        commandContext.definition());
  }
}
