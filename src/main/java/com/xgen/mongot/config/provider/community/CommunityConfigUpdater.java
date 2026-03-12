package com.xgen.mongot.config.provider.community;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.config.provider.mongod.ConfigUpdaterUtils;
import com.xgen.mongot.config.updater.ConfigUpdater;
import com.xgen.mongot.config.util.ViewValidator;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidViewDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunityConfigUpdater implements ConfigUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(CommunityConfigUpdater.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final AuthoritativeIndexCatalog authoritativeIndexCatalog;
  private final MongoDbMetadataClient mongoDbMetadataClient;
  private final ConfigManager configManager;
  private final FeatureFlags featureFlags;

  @GuardedBy("this")
  private volatile boolean closed;

  public CommunityConfigUpdater(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      MongoDbMetadataClient mongoDbMetadataClient,
      ConfigManager configManager,
      FeatureFlags featureFlags) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.mongoDbMetadataClient = mongoDbMetadataClient;
    this.configManager = configManager;
    this.featureFlags = featureFlags;

    this.closed = false;
  }

  @Override
  public synchronized void update() {
    checkState(!this.closed, "cannot call update() after close()");

    // First try updating all our live indexes with views. This may fail, if the catalog is
    // concurrently modified by another mongot, or for some other transient reason.
    try {
      updateIndexesWithViews();
    } catch (CheckedMongoException | BsonParseException | MetadataServiceException e) {
      FLOGGER.atSevere().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
          "Unexpected error when updating view definitions, skipping");
    }

    // Regardless of whether applying view updates succeeded, query the AIC for the updated list of
    // desired index definitions.
    List<IndexDefinition> desiredDefinitions;
    try {
      desiredDefinitions = this.authoritativeIndexCatalog.listIndexDefinitions();
    } catch (MetadataServiceException e) {
      // CommunityConfigUpdater may have started before mongod is available or we received a
      // transient error because mongod is unavailable.
      // Instead of raising the error, causing the host to crash we simply catch and log the error.
      // On the next run we'll update the config with any potential config changes.
      LOG.warn(
          "Failed calling authoritative index catalog to get latest index definitions, "
              + "will retry on next run.",
          e);
      return;
    }
    IndexesByType indexesByType = IndexesByType.fromIndexDefinitions(desiredDefinitions);

    Set<UUID> directMongodCollectionSet =
        this.featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            ? ConfigUpdaterUtils.resolveNonexistentUuidsOnDirectMongod(
                this.configManager.getLiveIndexGenerations(),
                this.mongoDbMetadataClient,
                LOG,
                Optional.empty())
            : Set.of();

    this.configManager.update(
        indexesByType.vectorIndexes,
        indexesByType.searchIndexes,
        List.of(),
        directMongodCollectionSet);
  }

  @GuardedBy("this")
  private void updateIndexesWithViews()
      throws CheckedMongoException, BsonParseException, MetadataServiceException {
    List<IndexDefinition> indexesWithViews =
        this.configManager.getLiveIndexes().stream()
            .filter(index -> index.getView().isPresent())
            .toList();
    Set<String> usedDatabases =
        indexesWithViews.stream().map(IndexDefinition::getDatabase).collect(Collectors.toSet());

    MongoDbCollectionInfos collectionInfos =
        this.mongoDbMetadataClient.resolveCollectionInfos(usedDatabases);

    for (IndexDefinition existingDefinition : indexesWithViews) {
      ViewDefinition currentView = existingDefinition.getView().orElseThrow();
      ViewDefinition updatedView =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace(existingDefinition.getDatabase(), currentView.getName()),
              existingDefinition.getCollectionUuid(),
              collectionInfos);

      if (currentView.equals(updatedView)) {
        continue;
      }

      AuthoritativeIndexKey existingIndexKey =
          new AuthoritativeIndexKey(
              existingDefinition.getCollectionUuid(), existingDefinition.getName());

      try {
        ViewValidator.validate(updatedView);
      } catch (InvalidViewDefinitionException e) {
        LOG.atError()
            .addKeyValue("indexId", existingDefinition.getIndexId())
            .addKeyValue("exceptionMessage", e.getMessage())
            .log("Dropping index from catalog");
        this.authoritativeIndexCatalog.deleteIndex(existingIndexKey);
        continue;
      }

      IndexDefinition newDefinition =
          switch (existingDefinition) {
            case SearchIndexDefinition search -> search.withUpdatedViewDefinition(updatedView);
            case VectorIndexDefinition vector -> vector.withUpdatedViewDefinition(updatedView);
          };

      this.authoritativeIndexCatalog.updateIndexDefinition(existingIndexKey, newDefinition);
    }
  }

  private record IndexesByType(
      List<SearchIndexDefinition> searchIndexes, List<VectorIndexDefinition> vectorIndexes) {
    public static IndexesByType fromIndexDefinitions(List<IndexDefinition> indexDefinitions) {
      var result = new IndexesByType(new ArrayList<>(), new ArrayList<>());

      indexDefinitions.forEach(
          index -> {
            switch (index) {
              case SearchIndexDefinition search -> result.searchIndexes().add(search);
              case VectorIndexDefinition vector -> result.vectorIndexes().add(vector);
            }
          });

      return result;
    }
  }

  @Override
  public synchronized void close() {
    LOG.info("Shutting down.");

    this.configManager.close();
    this.closed = true;
  }
}
