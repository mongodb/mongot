package com.xgen.mongot.replication.mongodb;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide, bounded executor for native writer prepares on promotion (leadership-transition
 * correctness, CLOUDP-446295).
 *
 * <p>Prepare is I/O-bound (SLS reads plus a materialization wait) and may take up to its native
 * deadline, so it must not occupy the shared {@code indexing-lifecycle} threads that every index's
 * init/build needs. A node taking over many logs therefore prepares at most pool-size indexes
 * concurrently; the rest queue. The pool is created lazily on first use (so processes that never
 * open a leadership-driven index pay nothing) and its {@code min(4, cores)} threads are daemons, so
 * it never blocks JVM shutdown.
 */
final class VectorlitePrepareExecutor {

  private static final int MAX_THREADS = 4;

  private VectorlitePrepareExecutor() {}

  /** Lazily-created process-wide pool. */
  private static final class Holder {
    private static final ExecutorService INSTANCE = create();

    private static ExecutorService create() {
      int threads = Math.max(1, Math.min(MAX_THREADS, Runtime.getRuntime().availableProcessors()));
      AtomicInteger counter = new AtomicInteger();
      ThreadFactory threadFactory =
          runnable -> {
            Thread thread = new Thread(runnable, "vectorlite-prepare-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
          };
      return Executors.newFixedThreadPool(threads, threadFactory);
    }
  }

  /** The shared prepare executor, created on first call. */
  static Executor get() {
    return Holder.INSTANCE;
  }
}
