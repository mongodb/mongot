package com.xgen.mongot.index;

import java.io.IOException;

/**
 * An {@link IndexWriter} whose engine schedules its own commits: the writer's {@link
 * IndexWriter#commit} is a no-op, and the commit user data instead reaches the engine by being
 * staged here, at each completed replication batch, to ride the engine's next scheduled commit.
 */
public interface CommitDataStagingWriter {

  /**
   * Records {@code userData} as the resume position without forcing a durable commit. Safe because
   * a persisted position is a minimum and replay past it is idempotent.
   */
  void stageUserData(EncodedUserData userData) throws IOException;
}
