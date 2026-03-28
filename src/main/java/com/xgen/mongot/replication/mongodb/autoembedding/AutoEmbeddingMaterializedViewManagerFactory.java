package com.xgen.mongot.replication.mongodb.autoembedding;

import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;

@FunctionalInterface
public interface AutoEmbeddingMaterializedViewManagerFactory {
  // TODO(CLOUDP-360913): Implement customized disk monitor for mat view.
  /** Creates a singleton AutoEmbeddingMaterializedViewManager. */
  Optional<MaterializedViewManager> create(Optional<SyncSourceConfig> syncSourceConfig);
}
