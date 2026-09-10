package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.manager.metrics.GroupedIndexGenerationMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.replication.ReplicationStatus;
import java.util.List;

public interface CachedIndexInfoProvider {
  /** Forces a refresh of cached index infos for all subsequent consumers. May be expensive. */
  void refreshIndexInfos();

  /** Returns IndexInformation for each of the Indexes currently configured. */
  List<IndexInformation> getIndexInfos();

  /**
   * Returns the indexes this node cannot serve because their config journal entry would not parse,
   * so that the authoritative source can be told about them.
   */
  QuarantinedIndexInfos getQuarantinedIndexInfos();

  /** Returns cached GroupedIndexGenerationMetrics for all of the Indexes currently configured. */
  List<GroupedIndexGenerationMetrics> getGroupedIndexGenerationMetrics();

  /** Returns the current index format version in use. */
  int getCurrentIndexFormatVersion();

  ReplicationStatus getReplicationStatus();
}
