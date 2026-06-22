package com.xgen.mongot.catalogservice;

import com.xgen.mongot.util.mongodb.CheckedMongoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blocks {@code __mdb_internal_search} catalog access when the {@code syncSource.router} config
 * does not match the cluster topology (sharded vs. replica set).
 *
 * <p>Callers should invoke {@link #requireTopologyMatch()} before catalog read or write. Acting on
 * a mismatched topology can cause index loss, duplicated catalog collections, or writes to the
 * wrong shard.
 */
public class CatalogAccessGuard {

  public static final Logger LOG = LoggerFactory.getLogger(CatalogAccessGuard.class);

  private final MongodTopologyMonitor topologyMonitor;
  private final boolean routerConfigured;

  public CatalogAccessGuard(MongodTopologyMonitor topologyMonitor, boolean routerConfigured) {
    this.topologyMonitor = topologyMonitor;
    this.routerConfigured = routerConfigured;
  }

  /**
   * Verifies that the current cluster topology matches the {@code syncSource.router} config.
   *
   * @throws CheckedMongoException if the cluster topology cannot be determined
   * @throws TopologyMismatchException if the cluster topology does not match {@code
   *     syncSource.router}
   */
  public void requireTopologyMatch() throws TopologyMismatchException, CheckedMongoException {
    boolean sharded = this.topologyMonitor.isShardedCluster();
    if (sharded != this.routerConfigured) {
      throw mismatchException(sharded);
    }
  }

  private TopologyMismatchException mismatchException(boolean sharded) {
    return new TopologyMismatchException(
        String.format(
            "cluster topology mismatch: cluster is %s but syncSource.router is %s in the mongot",
            sharded ? "sharded" : "a replica set",
            this.routerConfigured ? "configured" : "not configured"));
  }
}
