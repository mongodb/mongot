package com.xgen.mongot.config.manager;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.bson.types.ObjectId;

public interface ConfigManager extends CachedIndexInfoProvider {

  /**
   * Updates Indexes and Analyzers based off the desired state supplied.
   *
   * <p>The desired state arrives in two parts because an authoritative definition this binary
   * cannot parse still says its index is wanted, and the definition lists cannot say so on their
   * own: the id arrives separately. Diffing against the definition lists alone would delete indexes
   * that were only unreadable.
   *
   * @param vectorDefinitions List of vector index definitions.
   * @param searchDefinitions List of search index definitions.
   * @param analyzerDefinitions List of analyzer definitions.
   * @param directMongodCollectionSet A set of collection UUIDs resolved directly on mongod by the
   *     metadata client. Note: This set does not include all collections referenced by the indexes.
   *     It only includes collections that previously could not be resolved.
   * @param unreadableDesiredIds ids the authoritative source still holds but whose definition this
   *     binary could not use, whatever the caller ended up putting in the definition lists for
   *     them. They are kept out of every destructive diff. Pass an empty set when every definition
   *     was readable. Only pass ids from a listing that <em>succeeded</em>: a failed read of the
   *     authoritative source must never reach this method, or every index would look unreadable and
   *     nothing would ever be deleted.
   */
  void update(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      Set<UUID> directMongodCollectionSet,
      Set<ObjectId> unreadableDesiredIds);

  /**
   * Updates the sync source config, shutting down and restarting replication if it has changed. If
   * the config has not changed, delegates to disk-based replication restart instead.
   */
  void handleReplicationAndSyncSourceUpdate(SyncSourceConfig syncSourceConfig);

  List<IndexDefinition> getLiveIndexes();

  List<IndexGeneration> getLiveIndexGenerations();

  /** Determine if the replication manager has been initialized after startup. */
  boolean isReplicationInitialized();

  /**
   * Enqueues index generations that were deferred until their initialization prerequisites became
   * available.
   *
   * <p>Defaults to a no-op so callers can invoke through the {@link ConfigManager} type. {@link
   * DefaultConfigManager} also runs it each primary update cycle as a backstop.
   */
  default void promoteReadyIndexes() {}

  void close();
}
