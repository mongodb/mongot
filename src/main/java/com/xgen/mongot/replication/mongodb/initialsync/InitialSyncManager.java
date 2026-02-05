package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * InitialSyncManager coordinates the different components of an initial sync, calling them at the
 * proper times and supports being shut down by interrupting the Thread it is running on.
 */
interface InitialSyncManager {

  // Needs to be greater than our MongoD client timeout, which is 30s.
  Duration SHUTDOWN_TIMEOUT = Duration.ofMinutes(1);

  // TODO(CLOUDP-370676): Make this configurable in the future
  // For auto-embedding indexes, in-flight embedding API calls (e.g., to Voyage) may take longer
  // than SHUTDOWN_TIMEOUT. We want a relaxed timeout value in this case prior to forced shutdown
  Duration AUTO_EMBEDDING_SHUTDOWN_TIMEOUT = Duration.ofMinutes(5);

  String NO_CRASH_LOG_REASON =
      "this timeout is unlikely to corrupt state and is very common when "
          + "indexing, data fetching, driver server selection "
          + "take longer than the timeout";

  /**
   * Runs the initial sync, returning after the index has been committed. Returns a
   * ChangeStreamResumeInfo indicating the position in a change stream where the sync has applied
   * to.
   */
  ChangeStreamResumeInfo sync() throws InitialSyncException;

  static InitialSyncManagerFactory getFactory(
      InitialSyncConfig bufferlessConfig,
      CommonReplicationConfig replicationConfig,
      MetricsFactory metricsFactory) {
    return BufferlessInitialSyncManager.factory(
        bufferlessConfig.collectionScanTime(),
        bufferlessConfig.changeStreamCatchupTimeout(),
        bufferlessConfig.changeStreamLagTime(),
        bufferlessConfig.avoidNaturalOrderScanSyncSourceChangeResync(),
        replicationConfig.getExcludedChangestreamFields(),
        replicationConfig.getMatchCollectionUuidForUpdateLookup(),
        metricsFactory);
  }

  /** Awaits shutdown using the default SHUTDOWN_TIMEOUT. */
  @Crash.SafeWithoutCrashLog
  static void awaitShutdown(CompletableFuture<?>... futures) {
    awaitShutdown(SHUTDOWN_TIMEOUT, futures);
  }

  /**
   * Awaits shutdown using the specified timeout. Crashes the JVM if timeout is exceeded.
   *
   * @param timeout the maximum time to wait for futures to complete
   * @param futures the futures to await
   */
  @Crash.SafeWithoutCrashLog
  static void awaitShutdown(Duration timeout, CompletableFuture<?>... futures) {
    try {
      FutureUtils.awaitAllComplete(timeout, futures);
    } catch (TimeoutException e) {
      Crash.because("timed out waiting to shut down")
          .withoutCrashLog(NO_CRASH_LOG_REASON)
          .withThrowable(e)
          .withThreadDump()
          .now();
    }
  }

  /**
   * Waits for the provided future to complete and returns the resulting value.
   *
   * <p>If an exception is thrown in the future, shutDownProcedure is called, then the exception is
   * re-thrown.
   */
  static <V> V getResultOrThrow(
      CompletableFuture<V> future,
      String description,
      Runnable shutDownProcedure,
      DefaultKeyValueLogger logger)
      throws InitialSyncException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      logger.info("Interrupted {}. Shutting down.", description);
      shutDownProcedure.run();

      throw InitialSyncException.createShutDown();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      logger.error("Caught exception {}. Shutting down.", description, cause);
      shutDownProcedure.run();

      if (cause instanceof InitialSyncException) {
        throw (InitialSyncException) cause;
      }

      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }

      if (cause instanceof Error) {
        throw (Error) cause;
      }

      throw new AssertionError(
          "CompletableFuture threw unexpected checked exception: " + cause, cause);
    }
  }

  /**
   * Returns a CompletableFuture that represents the work of supplier being run on a separate
   * thread.
   *
   * <p>The result of the returned CompletableFuture must be resolved using getResultOrThrow().
   */
  static <V> CompletableFuture<V> supplyAsync(
      CheckedSupplier<V, InitialSyncException> supplier, String name) {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor(name);
    return FutureUtils.checkedSupplyAsync(supplier, executor, InitialSyncException.class);
  }

  /**
   * Returns a CompletableFuture that represents the work of runnable being run on a separate
   * thread.
   *
   * <p>The result of the returned CompletableFuture must be resolved using getResultOrThrow().
   */
  static CompletableFuture<Void> runAsync(
      CheckedRunnable<InitialSyncException> runnable, String name) {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor(name);
    return FutureUtils.checkedRunAsync(runnable, executor, InitialSyncException.class);
  }
}
