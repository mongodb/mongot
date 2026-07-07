package com.xgen.mongot.replication.mongodb;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public interface ReplicationIndexManagerFactory {

  IndexManager create(
      Executor executor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      Optional<SynonymManager> synonymManager,
      IndexGeneration indexGeneration,
      InitializedIndex index,
      DocumentIndexer documentIndexer,
      ScheduledExecutorService commitExecutor,
      Duration commitInterval,
      Duration requestRateLimitBackoff,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan);

  /**
   * Returns the appropriate factory for the given index generation: a {@link
   * RecyclableReplicationIndexManager} factory for VectorLite indexes, or {@code defaultFactory}
   * for all other indexes.
   *
   * <p>For VectorLite indexes the returned factory also registers the freshly created manager's
   * {@link RecyclableReplicationIndexManager#onLeaderStatusChange} as the leadership sink via
   * {@code leaderStatusRegistrar}. Such an index is leadership-driven, so the registrar is
   * required: the factory throws {@link IllegalStateException} when {@code leaderStatusRegistrar}
   * is empty, rather than silently leaving the manager without leadership updates. Because the
   * factory creates the concrete manager type directly, no downcast or {@code instanceof} is needed
   * at the call site.
   */
  static ReplicationIndexManagerFactory forIndexGeneration(
      IndexGeneration indexGeneration,
      ReplicationIndexManagerFactory defaultFactory,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
    if (indexGeneration.getDefinition() instanceof VectorIndexDefinition vectorDef
        && vectorDef.isCustomVectorEngineIndex()) {
      return (executor,
          cursorManager,
          initialSyncQueue,
          steadyStateManager,
          synonymManager,
          generation,
          index,
          documentIndexer,
          commitExecutor,
          commitInterval,
          requestRateLimitBackoff,
          meterRegistry,
          featureFlags,
          enableNaturalOrderScan) -> {
        // A custom vector engine index is leadership-driven, so a registrar must be wired or the
        // manager would silently never receive leadership transitions. Fail fast (before creating
        // the manager) rather than degrading to a no-op.
        LeaderStatusRegistrar registrar =
            leaderStatusRegistrar.orElseThrow(
                () ->
                    new IllegalStateException(
                        "Custom vector engine index "
                            + generation.getGenerationId()
                            + " requires a LeaderStatusRegistrar, but none was configured"));
        RecyclableReplicationIndexManager manager =
            RecyclableReplicationIndexManager.create(
                executor,
                cursorManager,
                initialSyncQueue,
                steadyStateManager,
                synonymManager,
                generation,
                index,
                documentIndexer,
                commitExecutor,
                commitInterval,
                requestRateLimitBackoff,
                meterRegistry,
                featureFlags,
                enableNaturalOrderScan);
        registrar.register(generation, manager::onLeaderStatusChange);
        return manager;
      };
    }
    return defaultFactory;
  }
}
