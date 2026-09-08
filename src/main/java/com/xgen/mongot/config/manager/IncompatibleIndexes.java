package com.xgen.mongot.config.manager;

import com.xgen.mongot.catalog.IndexProperties;
import com.xgen.mongot.index.IndexUnavailableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.types.ObjectId;

/**
 * The indexes mongot cannot use because their config journal entry could not be parsed, held by
 * index id and split by journal state. This class is not thread-safe.
 *
 * <p>Staged and live are held separately because an index can have a quarantined generation in each
 * at once.
 *
 * <p>Deleted entries are not tracked: {@link ConfigInitializer} drops everything in the journal's
 * deleted array at startup, so none are left to report on.
 */
public class IncompatibleIndexes implements QuarantinedIndexLookup {

  private final ConcurrentHashMap<ObjectId, QuarantinedIndex> stagedIndexes =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ObjectId, QuarantinedIndex> liveIndexes =
      new ConcurrentHashMap<>();

  /**
   * The parse error of each live entry, by the properties a query names it with, so the query path
   * can resolve one without a scan. An index of {@link #liveIndexes}, which stays authoritative.
   */
  private final ConcurrentHashMap<IndexProperties, String> liveParseErrorsByProperties =
      new ConcurrentHashMap<>();

  /** Adds a quarantined staged index, replacing any already tracked for the same index. */
  public void addStagedIndex(QuarantinedIndex quarantinedIndex) {
    this.stagedIndexes.put(quarantinedIndex.indexId(), quarantinedIndex);
  }

  /** Adds a quarantined live index, replacing any already tracked for the same index. */
  public void addLiveIndex(QuarantinedIndex quarantinedIndex) {
    QuarantinedIndex replaced = this.liveIndexes.put(quarantinedIndex.indexId(), quarantinedIndex);
    IndexProperties properties = propertiesOf(quarantinedIndex);

    if (replaced != null && !propertiesOf(replaced).equals(properties)) {
      // The replaced generation was named differently, so it is under a key we are not overwriting.
      this.liveParseErrorsByProperties.remove(propertiesOf(replaced));
    }
    this.liveParseErrorsByProperties.put(properties, quarantinedIndex.parseError());
  }

  /**
   * Removes the quarantined live index with the given id, returning it if there was one.
   *
   * <p>Callers that go on to delete the index's data on disk need the returned entry for its
   * generation id: there is no definition to derive one from.
   */
  public Optional<QuarantinedIndex> removeLiveIndex(ObjectId indexId) {
    QuarantinedIndex removed = this.liveIndexes.remove(indexId);
    if (removed == null) {
      return Optional.empty();
    }
    this.liveParseErrorsByProperties.remove(propertiesOf(removed));

    return Optional.of(removed);
  }

  /**
   * {@inheritDoc}
   *
   * <p>A quarantined staged index is deliberately not matched: its live counterpart parsed, so it
   * is in the catalog and queries against it behave as they always have.
   *
   * <p>An index on a view is keyed by its source collection, as {@link
   * com.xgen.mongot.catalog.IndexCatalog} keys it, but without the view-name comparison that
   * follows a catalog hit - the view name is not thin-parsed, so it is not known here. A query
   * naming no view can therefore be told the definition is unparseable when the quarantined index
   * of that name is one on a view over the queried collection. Such a query had no index to resolve
   * either way; only the error it is given differs.
   */
  @Override
  public void throwIfQuarantined(String database, UUID collectionUuid, String indexName)
      throws IndexUnavailableException {
    String parseError =
        this.liveParseErrorsByProperties.get(
            new IndexProperties(indexName, database, collectionUuid));
    if (parseError == null) {
      return;
    }

    throw new IndexUnavailableException(
        String.format(
            "Index '%s' is unavailable: its definition cannot be parsed (%s).",
            indexName, parseError));
  }

  public Collection<QuarantinedIndex> getStagedIndexes() {
    return new ArrayList<>(this.stagedIndexes.values());
  }

  public Collection<QuarantinedIndex> getLiveIndexes() {
    return new ArrayList<>(this.liveIndexes.values());
  }

  public boolean isEmpty() {
    return this.stagedIndexes.isEmpty() && this.liveIndexes.isEmpty();
  }

  /**
   * Stops tracking every quarantined index whose id is not in {@code keepIds}, staged and live
   * alike, and returns them. An entry removed here is no longer written back out by {@link
   * ConfigState#currentJournal()}.
   */
  public List<QuarantinedIndex> removeAllExcept(Set<ObjectId> keepIds) {
    List<QuarantinedIndex> removed = new ArrayList<>();

    this.stagedIndexes
        .values()
        .removeIf(
            quarantinedIndex -> {
              if (keepIds.contains(quarantinedIndex.indexId())) {
                return false;
              }
              removed.add(quarantinedIndex);
              return true;
            });

    // Live entries go through removeLiveIndex so the query path's parse error index is kept in
    // step with them.
    getLiveIndexes().stream()
        .map(QuarantinedIndex::indexId)
        .filter(indexId -> !keepIds.contains(indexId))
        .forEach(indexId -> removeLiveIndex(indexId).ifPresent(removed::add));

    return removed;
  }

  private static IndexProperties propertiesOf(QuarantinedIndex quarantinedIndex) {
    return new IndexProperties(
        quarantinedIndex.name(), quarantinedIndex.database(), quarantinedIndex.collectionUuid());
  }
}
