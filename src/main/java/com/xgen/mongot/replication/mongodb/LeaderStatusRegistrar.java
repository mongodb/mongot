package com.xgen.mongot.replication.mongodb;

import com.xgen.mongot.index.IndexGeneration;
import java.util.function.Consumer;

/**
 * Connects a per-index leadership sink to whatever native source backs the index.
 *
 * <p>Declared in core but implemented in the atlas vectorlite layer, so that core replication can
 * invoke engine-specific leadership registration without taking a (forbidden) dependency on atlas
 * types. Invoked once per index, when its {@link RecyclableReplicationIndexManager} is created.
 */
public interface LeaderStatusRegistrar {

  /**
   * Wires {@code sink} to receive leadership transitions for the index described by {@code
   * indexGeneration}. Implementations are a no-op when the index has no native leadership source.
   *
   * <p>The sink may be invoked from a native upcall thread.
   */
  void register(
      IndexGeneration indexGeneration,
      Consumer<RecyclableReplicationIndexManager.LeaderStatus> sink);
}
