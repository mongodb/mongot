package com.xgen.mongot.index;

import java.io.IOException;

/**
 * A {@link MeteredIndexWriter} for writers whose engine schedules its own commits: keeps the
 * wrapped writer's {@link CommitDataStagingWriter} contract visible through the wrapper, so
 * replication's {@code instanceof} check still routes commit user data to {@link #stageUserData}
 * rather than silently dropping the resume position.
 */
public class MeteredStagingIndexWriter extends MeteredIndexWriter
    implements CommitDataStagingWriter {

  private final CommitDataStagingWriter stagingWriter;

  public <W extends IndexWriter & CommitDataStagingWriter> MeteredStagingIndexWriter(
      W indexWriter, IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    super(indexWriter, indexingMetricsUpdater);
    this.stagingWriter = indexWriter;
  }

  @Override
  public void stageUserData(EncodedUserData userData) throws IOException {
    this.stagingWriter.stageUserData(userData);
  }
}
