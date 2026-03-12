package com.xgen.mongot.server.command.management.util;

import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition;
import com.xgen.mongot.util.Check;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;

public class IndexEntryMapper {

  private static final Long DEFAULT_INDEX_VERSION = 0L;
  private static final Comparator<IndexStatus.StatusCode> INDEX_STATUS_PRIORITY_COMPARATOR =
      Comparator.comparingInt(IndexMapper::toIndexPriority);
  private static final Comparator<SynonymStatus.External> SYNONYM_STATUS_PRIORITY_COMPARATOR =
      Comparator.comparingInt(SynonymStatus.External::getPriority);

  /**
   * Maps the IndexDefinition returned by the AIC and the IndexStatsEntries for each host into an
   * IndexEntry to return by ListSearchIndexes.
   *
   * @param definition the AIC index definition
   * @param customerDef the definition BSON provided by the customer when creating the index
   * @param indexStatsPerServer the IndexStatsEntry for each server in the cluster
   * @param numDocs the number of docs stored in this index
   * @return an IndexEntry object to be returned by ListSearchIndexes
   */
  public static ListSearchIndexesResponseDefinition.IndexEntry toIndexEntry(
      IndexDefinition definition,
      BsonDocument customerDef,
      Map<String, IndexStatsEntry> indexStatsPerServer,
      Optional<Long> numDocs) {

    return new ListSearchIndexesResponseDefinition.IndexEntry(
        definition.getIndexId(),
        definition.getName(),
        IndexMapper.toExternalStatus(
            computeConsolidatedIndexStatus(
                indexStatsPerServer.values(),
                definition.getDefinitionVersion().orElse(DEFAULT_INDEX_VERSION))),
        areAllIndexesQueryable(indexStatsPerServer.values()),
        toDefinitionVersion(
            definition.getDefinitionVersion(), definition.getDefinitionVersionCreatedAt()),
        customerDef,
        toHostStatusDetails(
            indexStatsPerServer, definition.getDefinitionVersion().orElse(DEFAULT_INDEX_VERSION)),
        computeConsolidatedSynonymStatus(indexStatsPerServer.values()),
        computeConsolidatedSynonymStatusDetails(indexStatsPerServer.values()),
        numDocs);
  }

  /**
   * Converts the IndexStatsEntries to a per-host status details showing the state of main and
   * staged indexes on each host in the cluster.
   */
  private static List<ListSearchIndexesResponseDefinition.HostStatusDetail> toHostStatusDetails(
      Map<String, IndexStatsEntry> indexStatsPerServer, long latestVersion) {
    return indexStatsPerServer.entrySet().stream()
        .map(
            entry ->
                new ListSearchIndexesResponseDefinition.HostStatusDetail(
                    entry.getKey(),
                    IndexMapper.toExternalStatus(
                        computeStatusForIndexStatsEntry(entry.getValue(), latestVersion)),
                    isIndexQueryable(entry.getValue()),
                    entry.getValue().mainIndex().map(IndexEntryMapper::toHostIndexStatusDetail),
                    entry.getValue().stagedIndex().map(IndexEntryMapper::toHostIndexStatusDetail)))
        .toList();
  }

  /**
   * Converts the DetailedIndexStats object which is how we represent indexes in the IndexStatsEntry
   * into an HostIndexStatusDetail which is how it's represented by ListSearchIndexes.
   */
  private static ListSearchIndexesResponseDefinition.HostIndexStatusDetail toHostIndexStatusDetail(
      IndexStatsEntry.DetailedIndexStats detailedIndexStats) {
    return new ListSearchIndexesResponseDefinition.HostIndexStatusDetail(
        IndexMapper.toExternalStatus(detailedIndexStats.status().getStatusCode()),
        detailedIndexStats.status().canServiceQueries(),
        computeSynonymStatus(detailedIndexStats).map(IndexMapper::toExternalSynonymStatus),
        toSynonymStatusDetails(detailedIndexStats),
        toDefinitionVersion(
            detailedIndexStats.definition().getDefinitionVersion(),
            detailedIndexStats.definition().getDefinitionVersionCreatedAt()),
        IndexMapper.toExternal(detailedIndexStats.definition()),
        detailedIndexStats.status().getMessage());
  }

  /** Generates the DefinitionVersion object. */
  private static ListSearchIndexesResponseDefinition.DefinitionVersion toDefinitionVersion(
      Optional<Long> definitionVersion, Optional<Instant> definitionVersionCreatedAt) {
    return new ListSearchIndexesResponseDefinition.DefinitionVersion(
        Math.toIntExact(definitionVersion.orElse(DEFAULT_INDEX_VERSION)),
        definitionVersionCreatedAt.orElse(Instant.now()));
  }

  /**
   * Looks at the status code for each IndexStatsEntry and computes a consolidated status based on
   * the highest priority status across all IndexStatsEntries.
   */
  private static IndexStatus.StatusCode computeConsolidatedIndexStatus(
      Collection<IndexStatsEntry> indexStatsEntries, long latestVersion) {
    // TODO(CLOUDP-381175): handle actively deleting indexes

    // IndexStatsEntries don't yet exist for the given index
    if (indexStatsEntries.isEmpty()) {
      return IndexStatus.StatusCode.UNKNOWN;
    }

    return indexStatsEntries.stream()
        .map(i -> computeStatusForIndexStatsEntry(i, latestVersion))
        .max(INDEX_STATUS_PRIORITY_COMPARATOR)
        .orElseThrow(() -> new IllegalStateException("Expected at least one status"));
  }

  /**
   * Considers the main and staged indexes for an IndexStatusEntry to compute the index's status.
   */
  private static IndexStatus.StatusCode computeStatusForIndexStatsEntry(
      IndexStatsEntry entry, long latestVersion) {
    if (entry.mainIndex().isEmpty()) {
      return IndexStatus.StatusCode.UNKNOWN;
    }

    IndexStatus.StatusCode mainIndexStatus = entry.mainIndex().get().status().getStatusCode();
    IndexStatus.StatusCode stagedIndexStatus =
        entry
            .stagedIndex()
            .map(IndexStatsEntry.DetailedIndexStats::status)
            .map(IndexStatus::getStatusCode)
            .orElse(IndexStatus.StatusCode.DOES_NOT_EXIST);

    boolean isMainIndexLatestVersion =
        Objects.equals(
            entry
                .mainIndex()
                .map(IndexStatsEntry.DetailedIndexStats::definition)
                .flatMap(IndexDefinition::getDefinitionVersion)
                .orElse(DEFAULT_INDEX_VERSION),
            latestVersion);

    return IndexMapper.calculateStatus(
        mainIndexStatus, stagedIndexStatus, isMainIndexLatestVersion);
  }

  /**
   * Checks that all hosts have queryable main indexes (excluding hosts where the index does not
   * exist).
   */
  private static boolean areAllIndexesQueryable(Collection<IndexStatsEntry> indexStatsEntries) {

    List<IndexStatsEntry> existingIndexes =
        indexStatsEntries.stream()
            .filter(
                i ->
                    i.mainIndex().isPresent()
                        && i.mainIndex().get().status().getStatusCode()
                            != IndexStatus.StatusCode.DOES_NOT_EXIST)
            .toList();

    return !existingIndexes.isEmpty()
        && existingIndexes.stream().allMatch(IndexEntryMapper::isIndexQueryable);
  }

  /** Checks that a given index is queryable. */
  private static boolean isIndexQueryable(IndexStatsEntry indexStatsEntry) {
    return indexStatsEntry.mainIndex().isPresent()
        && indexStatsEntry.mainIndex().get().status().canServiceQueries();
  }

  /**
   * Looks at the synonym status for each synonym on each IndexStatsEntry and computes a
   * consolidated status based on the highest priority status across all indexes and synonyms.
   */
  private static Optional<String> computeConsolidatedSynonymStatus(
      Collection<IndexStatsEntry> indexStatsEntries) {
    List<SynonymStatus.External> statuses =
        indexStatsEntries.stream()
            .map(IndexStatsEntry::mainIndex)
            .flatMap(Optional::stream)
            .flatMap(mainIndex -> computeSynonymStatus(mainIndex).stream())
            .toList();

    return findHighestPrioritySynonymStatus(statuses).map(IndexMapper::toExternalSynonymStatus);
  }

  /**
   * Computes consolidated synonym status details across all hosts. For each synonym mapping,
   * selects the status from the host instance with the highest priority status.
   */
  private static Optional<List<ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail>>
      computeConsolidatedSynonymStatusDetails(Collection<IndexStatsEntry> indexStatsEntries) {

    List<IndexStatsEntry.DetailedIndexStats> mainIndexes =
        indexStatsEntries.stream()
            .map(IndexStatsEntry::mainIndex)
            .flatMap(Optional::stream)
            .toList();

    List<Map<String /* synonymName */, SynonymDetailedStatus>> synonymMaps =
        mainIndexes.stream()
            .map(IndexStatsEntry.DetailedIndexStats::synonymDetailedStatusMap)
            .flatMap(Optional::stream)
            .toList();

    Map<String /* synonymName */, List<SynonymDetailedStatus>> detailsPerSynonym =
        synonymMaps.stream()
            .flatMap(synonymMap -> synonymMap.entrySet().stream())
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

    if (detailsPerSynonym.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        detailsPerSynonym.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> computeSynonymStatusDetailAcrossHosts(entry.getValue()))
            .toList());
  }

  /**
   * Given multiple instances of the same synonym across different hosts, returns the detailed
   * status from the instance with the highest priority status
   */
  private static ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail
      computeSynonymStatusDetailAcrossHosts(List<SynonymDetailedStatus> synonymStatuses) {
    Check.argNotEmpty(synonymStatuses, "synonymStatuses");

    List<SynonymStatus.External> externalStatuses =
        synonymStatuses.stream().map(s -> SynonymStatus.toExternal(s.statusCode())).toList();

    SynonymStatus.External highestPriorityStatus =
        findHighestPrioritySynonymStatus(externalStatuses)
            .orElseThrow(() -> new IllegalStateException("Expected at least one synonym status"));

    SynonymDetailedStatus detailedStatus =
        synonymStatuses.stream()
            .filter(s -> SynonymStatus.toExternal(s.statusCode()).equals(highestPriorityStatus))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Expected at least one synonym detailed status with "
                            + "highest priority status"));

    return new ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail(
        IndexMapper.toExternalSynonymStatus(highestPriorityStatus),
        synonymStatuses.stream().allMatch(s -> s.statusCode().isReady()),
        detailedStatus.message());
  }

  /**
   * Computes the overall synonym status for a single index by finding the highest priority status
   * among all synonym mappings in that index.
   */
  private static Optional<SynonymStatus.External> computeSynonymStatus(
      IndexStatsEntry.DetailedIndexStats detailedIndexStats) {
    Optional<Map<String, SynonymDetailedStatus>> synonymMap =
        detailedIndexStats.synonymDetailedStatusMap();
    if (synonymMap.isEmpty() || synonymMap.get().isEmpty()) {
      return Optional.empty();
    }

    List<SynonymStatus.External> externalStatuses =
        synonymMap.get().values().stream()
            .map(s -> SynonymStatus.toExternal(s.statusCode()))
            .toList();
    return findHighestPrioritySynonymStatus(externalStatuses);
  }

  /** Finds the synonym status with the highest priority from a collection of statuses. */
  private static Optional<SynonymStatus.External> findHighestPrioritySynonymStatus(
      Collection<SynonymStatus.External> statuses) {
    if (statuses.isEmpty()) {
      return Optional.empty();
    }
    return statuses.stream().max(SYNONYM_STATUS_PRIORITY_COMPARATOR);
  }

  /**
   * Converts each SynonymMapping for the IndexStatsEntry into a SynonymMappingStatusDetail for the
   * ListSearchIndexes response.
   */
  private static Optional<List<ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail>>
      toSynonymStatusDetails(IndexStatsEntry.DetailedIndexStats indexStats) {
    return indexStats
        .synonymDetailedStatusMap()
        .map(
            i ->
                i.values().stream()
                    .map(
                        s ->
                            new ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail(
                                IndexMapper.toExternalSynonymStatus(
                                    SynonymStatus.toExternal(s.statusCode())),
                                s.statusCode().isReady(),
                                s.message()))
                    .toList())
        // if the list is empty return an empty optional
        .filter(list -> !list.isEmpty());
  }
}
