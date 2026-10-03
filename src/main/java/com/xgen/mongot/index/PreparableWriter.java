package com.xgen.mongot.index;

import java.io.IOException;

/**
 * A writer whose native write path can be (re)prepared for a leadership session before it publishes
 * commits (leadership-transition correctness, CLOUDP-446295).
 *
 * <p>On promotion the replication layer calls {@link #prepareWriter()} before it starts
 * replicating: the native side joins the stale writer, refreshes its recovery baseline to the
 * latest durable checkpoint, rebuilds from it, and returns the resume token to resume from. Until
 * then the write path is unprepared and its commits are counted no-ops, so a promoted follower can
 * never publish a checkpoint forked off its stale open-time base.
 */
public interface PreparableWriter {

  /**
   * Prepare the write path for the current leadership session.
   *
   * @return {@link PrepareResult.Ready} with the resume token, or {@link PrepareResult.Superseded}
   *     if leadership moved away during prepare (nothing was installed).
   * @throws IOException if prepare failed for a storage/timeout reason; the caller retries later.
   */
  PrepareResult prepareWriter() throws IOException;
}
