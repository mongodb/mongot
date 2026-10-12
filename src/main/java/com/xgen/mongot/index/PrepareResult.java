package com.xgen.mongot.index;

/**
 * Outcome of {@link PreparableWriter#prepareWriter()}: rebuilding a native writer for the current
 * leadership session on promotion (leadership-transition correctness).
 *
 * <p>A native error (writer prepare failed for a storage/timeout reason) is surfaced as an {@link
 * java.io.IOException} from {@code prepareWriter()}, not as a variant here.
 */
public sealed interface PrepareResult
    permits PrepareResult.Ready, PrepareResult.Superseded, PrepareResult.Failed {

  /**
   * The write path is prepared. {@link #userData()} is the committed user data (resume token) of
   * the checkpoint the new session was rebuilt from; the replication manager resumes from it.
   */
  record Ready(EncodedUserData userData) implements PrepareResult {}

  /**
   * Leadership moved away during prepare; nothing was installed and the index is left unprepared.
   * The caller should wait for the next leadership signal rather than retry immediately.
   */
  record Superseded() implements PrepareResult {}

  /** The native writer is terminal and will not recover without a process restart. */
  record Failed(String message) implements PrepareResult {}
}
