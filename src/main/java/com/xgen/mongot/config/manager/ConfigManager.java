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

public interface ConfigManager extends CachedIndexInfoProvider {

  /**
   * Updates Indexes and Analyzers based off the desired state supplied.
   *
   * @param vectorDefinitions List of vector index definitions.
   * @param searchDefinitions List of search index definitions.
   * @param analyzerDefinitions List of analyzer definitions.
   * @param directMongodCollectionSet A set of collection UUIDs resolved directly on mongod by the
   *     metadata client. Note: This set does not include all collections referenced by the indexes.
   *     It only includes collections that previously could not be resolved.
   */
  void update(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      Set<UUID> directMongodCollectionSet);

  /**
   * Updates the sync source config, shutting down and restarting replication if it has changed.
   * If the config has not changed, delegates to disk-based replication restart instead.
   */
  void handleReplicationAndSyncSourceUpdate(SyncSourceConfig syncSourceConfig);

  List<IndexDefinition> getLiveIndexes();

  List<IndexGeneration> getLiveIndexGenerations();

  /** Determine if the replication manager has been initialized after startup. */
  boolean isReplicationInitialized();

  void close();
}
