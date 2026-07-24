package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@link ChangeStreamTaskExecutor} that submits tasks straight to the delegate executor's single
 * FIFO queue with no priority ordering or in-flight limiting. This is the pre-priority-scheduling
 * behavior, used for the sync change-stream path when neither priority scheduling nor a
 * max-in-flight-embedding-getMores cap applies.
 */
final class FifoExecutor implements ChangeStreamTaskExecutor {

  private final NamedScheduledExecutorService delegate;

  FifoExecutor(NamedScheduledExecutorService delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean enqueue(Runnable task, EnqueueContext context) {
    try {
      this.delegate.submit(task);
      return true;
    } catch (RejectedExecutionException ex) {
      // The delegate uses an unbounded queue, so rejection only happens after shutdown. Hand the
      // task back to the dispatcher via onDropped so it can release resources.
      context.onDropped().run();
      return false;
    }
  }

  @Override
  public void shutdown() {
    Executors.shutdownOrFail(this.delegate);
  }
}
