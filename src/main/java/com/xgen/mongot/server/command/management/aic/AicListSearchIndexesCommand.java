package com.xgen.mongot.server.command.management.aic;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.IndexEntry;
import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.catalogservice.MetadataClient;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerStateEntry;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.Cursor;
import com.xgen.mongot.server.command.management.definition.common.CommonDefinitions;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.command.management.util.IndexEntryMapper;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AicListSearchIndexesCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(AicListSearchIndexesCommand.class);

  private final MetadataService metadataService;
  private final CachedIndexInfoProvider indexInfoProvider;
  private final String db;
  private final String collectionName;
  private final UUID collectionUuid;
  private final Optional<UserViewDefinition> view;
  private final ListSearchIndexesCommandDefinition definition;

  // Indicates if we should:
  //   1. Include the numDocs field in the response document
  //   2. Support listing ALL indexes when this command is called on the internal indexCatalog
  //      collection
  private final boolean internalListIndexesForTesting;

  // Indicates we should return all indexes and not just the ones included for the collection in the
  // request.
  private final boolean commandShouldReturnAllIndexes;

  AicListSearchIndexesCommand(
      MetadataService metadataService,
      CachedIndexInfoProvider indexInfoProvider,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      ListSearchIndexesCommandDefinition definition,
      boolean internalListIndexesForTesting) {
    this.metadataService = metadataService;
    this.indexInfoProvider = indexInfoProvider;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
    this.internalListIndexesForTesting = internalListIndexesForTesting;

    // If the `internalListIndexesForTesting` flag is set and the query targets the catalog
    // collection then return all indexes.
    this.commandShouldReturnAllIndexes =
        this.internalListIndexesForTesting
            && db.equals(MetadataClient.DATABASE_NAME)
            && collectionName.equals(AuthoritativeIndexCatalog.COLLECTION_NAME);
  }

  @Override
  public String name() {
    return ListSearchIndexesCommandDefinition.NAME;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", ListSearchIndexesCommandDefinition.NAME)
        .addKeyValue("db", this.db)
        .addKeyValue("collectionName", this.collectionName)
        .addKeyValue("viewName", this.view.map(UserViewDefinition::name))
        .log("Received command");

    List<IndexAndStats> matchingIndexesAndStats;
    try {
      matchingIndexesAndStats = getMatchingIndexesAndStats();
    } catch (MetadataServiceException e) {
      LOG.atWarn().setCause(e).log("metadata service exception processing list search indexes");
      return MessageUtils.createError(Errors.COMMAND_FAILED, "Error processing request.");
    }

    List<BsonDocument> responseData = populateResponseData(matchingIndexesAndStats);

    String namespace = String.format("%s.%s", this.db, this.collectionName);
    BsonDocument response =
        new ListSearchIndexesResponseDefinition(
                CommonDefinitions.OK_SUCCESS_CODE, new Cursor(namespace, responseData))
            .toBson();

    if (BsonUtils.isOversized(response)) {
      return MessageUtils.createError(
          Errors.INDEX_INFORMATION_TOO_LARGE,
          "The requested indexes are too large. Try narrowing your filter.");
    }

    return response;
  }

  /**
   * For all indexes matching the queries filter expression, returns the latest {@link
   * IndexDefinition} and it's {@link IndexStatsEntry} per server.
   */
  List<IndexAndStats> getMatchingIndexesAndStats() throws MetadataServiceException {
    List<IndexEntry> matchingIndexes = findMatchingIndexes();
    Map<ObjectId, ServerStateEntry> activeServers = getActiveServers();

    // As of today we have no way of getting the numDocs per index across the active servers in the
    // cluster and therefore this map only contains the local host's numDocs per index. Since this
    // is only used for testing when internalListIndexesForTesting is set, this is fine. If we were
    // to add index stats to the official output we will need a way to collect this data across
    // hosts.
    Map<ObjectId, Long> numDocsByIndex =
        // Only populate the map if internalListIndexesForTesting is set.
        this.internalListIndexesForTesting
            ? this.indexInfoProvider.getIndexInfos().stream()
                .collect(
                    CollectionUtils.toMapUniqueKeys(
                        i -> i.getDefinition().getIndexId(),
                        i -> i.getAggregatedMetrics().numDocs()))
            : Collections.emptyMap();

    List<IndexAndStats> resultList = new ArrayList<>();
    for (IndexEntry indexEntry : matchingIndexes) {
      resultList.add(
          new IndexAndStats(
              indexEntry.definition(),
              indexEntry.customerDefinition().orElse(indexEntry.definition().toBson()),
              getIndexStatsPerServer(indexEntry.definition().getIndexId(), activeServers),
              getNumDocs(indexEntry.definition().getIndexId(), numDocsByIndex)));
    }

    return resultList;
  }

  /**
   * Returns the latest IndexEntries from the AIC who match the queries filter.
   *
   * <p>If {@link #commandShouldReturnAllIndexes} is set then ignores the query filter and returns
   * all indexes across all collections.
   */
  List<IndexEntry> findMatchingIndexes() throws MetadataServiceException {
    if (this.commandShouldReturnAllIndexes) {
      // Return all indexes without filtering
      return this.metadataService.getAuthoritativeIndexCatalog().listIndexes();
    }

    Predicate<IndexEntry> indexesMatchingTarget =
        idx ->
            this.definition
                    .target()
                    .indexId()
                    .map(idx.definition().getIndexId()::equals)
                    .orElse(true)
                && this.definition
                    .target()
                    .indexName()
                    .map(idx.definition().getName()::equals)
                    .orElse(true);
    return this.metadataService
        .getAuthoritativeIndexCatalog()
        .listIndexes(this.collectionUuid)
        .stream()
        .filter(indexesMatchingTarget)
        .toList();
  }

  /** Gets the active servers for this cluster. */
  Map<ObjectId, ServerStateEntry> getActiveServers() throws MetadataServiceException {
    return this.metadataService
        .getServerState()
        .list(ServerStateEntry.activeServersFilter())
        .stream()
        .collect(CollectionUtils.toMapUniqueKeys(ServerStateEntry::serverId, server -> server));
  }

  /**
   * For a given indexId returns a map of the {@link IndexStatsEntry} for each server in this
   * cluster.
   */
  Map<String, IndexStatsEntry> getIndexStatsPerServer(
      ObjectId indexId, Map<ObjectId, ServerStateEntry> serverIdMap)
      throws MetadataServiceException {

    List<IndexStatsEntry> indexStatsEntries =
        this.metadataService.getIndexStats().list(IndexStatsEntry.indexIdFilter(indexId));

    return indexStatsEntries.stream()
        .filter(entry -> serverIdMap.containsKey(entry.key().serverId()))
        .collect(
            CollectionUtils.toMapUniqueKeys(
                entry -> serverIdMap.get(entry.key().serverId()).serverName(),
                Function.identity()));
  }

  /**
   * If the {@link #internalListIndexesForTesting} flag is set, returns the doc count for the
   * requested index, otherwise returns an empty optional.
   *
   * <p>The numDocs field is not part of the officially documented listSearchIndexes output and is
   * only included to support internal E2E testing which is why we return an empty optional when the
   * flag is not set.
   */
  Optional<Long> getNumDocs(ObjectId indexId, Map<ObjectId, Long> numDocsByIndex) {
    if (!this.internalListIndexesForTesting) {
      return Optional.empty();
    }

    return Optional.ofNullable(numDocsByIndex.get(indexId));
  }

  List<BsonDocument> populateResponseData(List<IndexAndStats> matchingIndexes) {
    return matchingIndexes.stream()
        .map(
            i ->
                IndexEntryMapper.toIndexEntry(
                    i.indexDefinition, i.customerDef, i.indexStatsPerServer, i.numDocs))
        .map(ListSearchIndexesResponseDefinition.IndexEntry::toBson)
        .toList();
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }

  private record IndexAndStats(
      IndexDefinition indexDefinition,
      BsonDocument customerDef,
      Map<String, IndexStatsEntry> indexStatsPerServer,
      Optional<Long> numDocs) {}
}
