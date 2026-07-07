package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.catalogservice.schema.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.schema.IndexStatsEntry;
import com.xgen.mongot.catalogservice.schema.ServerStateEntry;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes stale {@link com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog} entries when their
 * collection has been dropped.
 *
 * <p>An index is eligible for deletion when:
 *
 * <ol>
 *   <li>Its status is {@link IndexStatus.StatusCode#DOES_NOT_EXIST}.
 *   <li>All active mongot servers agree: each has no main or staged index for this index ID.
 *   <li>The collection UUID is not present in the current MongoDB collection infos.
 * </ol>
 */
class DroppedCollectionIndexCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(DroppedCollectionIndexCleaner.class);

  private final MetadataService metadataService;
  private final MongoDbMetadataClient mongoDbMetadataClient;
  private final CatalogAccessGuard catalogAccessGuard;

  DroppedCollectionIndexCleaner(
      MetadataService metadataService,
      MongoDbMetadataClient mongoDbMetadataClient,
      CatalogAccessGuard catalogAccessGuard) {
    this.metadataService = metadataService;
    this.mongoDbMetadataClient = mongoDbMetadataClient;
    this.catalogAccessGuard = catalogAccessGuard;
  }

  /**
   * Scans {@code allIndexInfos} for dropped indexes and deletes their AIC entries if all
   * eligibility conditions are met. This method does not refresh the index infos; the caller is
   * responsible for providing an up-to-date list.
   */
  public void deleteIndexesForDroppedCollections(List<IndexInformation> allIndexInfos) {
    try {
      this.catalogAccessGuard.requireTopologyMatch();
    } catch (TopologyMismatchException e) {
      LOG.error("Skipping dropped-collection index cleanup due to cluster topology mismatch.", e);
      return;
    } catch (CheckedMongoException e) {
      LOG.warn(
          "Skipping dropped-collection index cleanup; unable to determine cluster topology.", e);
      return;
    }

    List<IndexInformation> droppedIndexes =
        allIndexInfos.stream()
            .filter(
                info -> info.getStatus().getStatusCode() == IndexStatus.StatusCode.DOES_NOT_EXIST)
            .toList();
    if (droppedIndexes.isEmpty()) {
      return;
    }

    Set<ObjectId> activeServerIds;
    try {
      activeServerIds =
          this.metadataService
              .getServerState()
              .list(ServerStateEntry.activeServersFilter())
              .stream()
              .map(ServerStateEntry::serverId)
              .collect(Collectors.toSet());
    } catch (MetadataServiceException e) {
      LOG.warn("Failed to list active servers while checking for dropped collection indexes", e);
      return;
    }

    if (activeServerIds.isEmpty()) {
      LOG.warn(
          "No active servers found while checking for dropped collection indexes;"
              + " skipping cleanup to avoid decisions based on stale server state");
      return;
    }

    Map<String, List<IndexInformation>> consensusDroppedByDatabase =
        getIndexesDroppedByAllServersGroupedByDb(droppedIndexes, activeServerIds);

    for (Map.Entry<String, List<IndexInformation>> entry : consensusDroppedByDatabase.entrySet()) {
      deleteDroppedIndexesForDatabase(entry.getKey(), entry.getValue());
    }
  }

  /** Returns indexes that don't exist on any of the active servers, grouped by database name. */
  Map<String, List<IndexInformation>> getIndexesDroppedByAllServersGroupedByDb(
      List<IndexInformation> droppedIndexes, Set<ObjectId> activeServerIds) {
    Map<String, List<IndexInformation>> result = new HashMap<>();
    for (IndexInformation info : droppedIndexes) {
      if (indexDeletedByAllServers(info.getDefinition().getIndexId(), activeServerIds)) {
        result
            .computeIfAbsent(info.getDefinition().getDatabase(), db -> new ArrayList<>())
            .add(info);
      }
    }
    return result;
  }

  /**
   * Returns true when every active server has an IndexStats entry for {@code indexId} where both
   * mainIndex and stagedIndex are either absent or carry {@link
   * IndexStatus.StatusCode#DOES_NOT_EXIST} status. This means no server is actively building or
   * holding live index data.
   */
  boolean indexDeletedByAllServers(ObjectId indexId, Set<ObjectId> activeServerIds) {
    List<IndexStatsEntry> allServerStats;
    try {
      allServerStats =
          this.metadataService.getIndexStats().list(IndexStatsEntry.indexIdFilter(indexId));
    } catch (MetadataServiceException e) {
      LOG.warn("Failed to list index stats for index while checking dropped collection indexes", e);
      return false;
    }

    Map<ObjectId, IndexStatsEntry> statsByServer =
        allServerStats.stream()
            .collect(Collectors.toMap(s -> s.key().serverId(), s -> s, (a, b) -> a));

    return activeServerIds.stream()
        .allMatch(
            serverId -> {
              IndexStatsEntry stats = statsByServer.get(serverId);
              // Every server must have an IndexStatsEntry for the index that doesn't exist.
              return stats != null
                  && indexDoesNotExist(stats.mainIndex())
                  && indexDoesNotExist(stats.stagedIndex());
            });
  }

  private static boolean indexDoesNotExist(Optional<IndexStatsEntry.DetailedIndexStats> detail) {
    return detail.isEmpty()
        || detail.get().status().getStatusCode() == IndexStatus.StatusCode.DOES_NOT_EXIST;
  }

  /**
   * Resolves live collection UUIDs for {@code database} and deletes AIC entries whose UUID is no
   * longer present. Skips the entire database if collection infos cannot be fetched.
   */
  void deleteDroppedIndexesForDatabase(String database, List<IndexInformation> indexesForDb) {
    MongoDbCollectionInfos collectionInfos;
    try {
      collectionInfos = this.mongoDbMetadataClient.resolveCollectionInfos(Set.of(database));
    } catch (CheckedMongoException e) {
      LOG.warn(
          "Failed to resolve collection infos for database while checking dropped collection"
              + " indexes, skipping database",
          e);
      return;
    }

    Set<UUID> existingUuids = collectionInfos.getAllCollectionUuids();
    for (IndexInformation info : indexesForDb) {
      UUID collectionUuid = info.getDefinition().getCollectionUuid();
      if (existingUuids.contains(collectionUuid)) {
        continue;
      }

      AuthoritativeIndexKey key =
          new AuthoritativeIndexKey(collectionUuid, info.getDefinition().getName());
      try {
        this.metadataService.getAuthoritativeIndexCatalog().deleteIndex(key);
        LOG.atInfo()
            .addKeyValue("indexId", info.getDefinition().getIndexId())
            .addKeyValue("db", database)
            .addKeyValue("collectionUUID", collectionUuid)
            .addKeyValue(
                "lastObservedCollectionName", info.getDefinition().getLastObservedCollectionName())
            .log("Deleted AIC entry for dropped collection");
      } catch (MetadataServiceException e) {
        LOG.warn("Failed to delete AIC entry for dropped collection, will retry on next run", e);
      }
    }
  }
}
