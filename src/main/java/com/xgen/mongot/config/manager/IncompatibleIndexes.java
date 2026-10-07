package com.xgen.mongot.config.manager;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.catalog.IndexProperties;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.QuarantinedIndexException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
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

  /**
   * The metric name, under {@link IndexMetricsUpdater#NAMESPACE}, of the per-index quarantine
   * gauge. Deliberately distinct from the per-index status metric: a quarantine has no {@code
   * generationId}, so it cannot carry the tag-key set Prometheus requires of every series sharing a
   * metric name with the per-index status gauges.
   */
  @VisibleForTesting static final String QUARANTINED_INDEX_METRIC_NAME = "quarantinedIndex";

  /**
   * Writes one {@code index.stats.quarantinedIndex{status=QUARANTINED,indexId=...}} gauge per
   * quarantined live index, constant value 1 while tracked. The only tags are the index id and the
   * constant status, so a re-added index overwrites the existing series and no orphaned gauge can
   * be left behind. No index type is carried: a quarantined entry has no parsed definition, so
   * whether it is auto-embedding is unknowable here. Registered on the process registry, which
   * production forwards to the FTDC child, so it reaches Prometheus and FTDC alike.
   */
  private final MetricsFactory metricsFactory;

  public IncompatibleIndexes(MeterRegistry meterRegistry) {
    this.metricsFactory = new MetricsFactory(IndexMetricsUpdater.NAMESPACE, meterRegistry);
  }

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

    this.metricsFactory.objectValueGauge(
        QUARANTINED_INDEX_METRIC_NAME,
        quarantinedIndex,
        index -> 1.0,
        quarantinedTags(quarantinedIndex.indexId()));
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
    this.metricsFactory.removeMeters(QUARANTINED_INDEX_METRIC_NAME, quarantinedTags(indexId));

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
      throws QuarantinedIndexException {
    String parseError =
        this.liveParseErrorsByProperties.get(
            new IndexProperties(indexName, database, collectionUuid));
    if (parseError == null) {
      return;
    }

    throw new QuarantinedIndexException(
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

  /**
   * The tags of the live-quarantine gauge: {@code status=QUARANTINED} and the index id, and nothing
   * else, so the meter's identity is exactly the live map's key and a re-added index overwrites its
   * series. {@code QUARANTINED} is a metric tag value only and must never be added to {@code
   * IndexStatus.StatusCode}, which is persisted in Lucene commit metadata.
   */
  private Tags quarantinedTags(ObjectId indexId) {
    return Tags.of("status", "QUARANTINED")
        .and(PerIndexMetricsFactory.indexIdTag(indexId.toHexString()));
  }

  private static IndexProperties propertiesOf(QuarantinedIndex quarantinedIndex) {
    return new IndexProperties(
        quarantinedIndex.name(), quarantinedIndex.database(), quarantinedIndex.collectionUuid());
  }
}
