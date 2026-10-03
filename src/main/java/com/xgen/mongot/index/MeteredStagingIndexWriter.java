package com.xgen.mongot.index;

import java.io.IOException;

/**
 * A {@link MeteredIndexWriter} for writers whose engine schedules its own commits: keeps the
 * wrapped writer's {@link CommitDataStagingWriter} and {@link PreparableWriter} contracts visible
 * through the wrapper, so replication's {@code instanceof} check still routes commit user data to
 * {@link #stageUserData} rather than silently dropping the resume position, and so leadership
 * promotion can still prepare the native writer (CLOUDP-446295) through the metered handle that
 * {@code getWriter()} returns.
 */
public class MeteredStagingIndexWriter extends MeteredIndexWriter
    implements CommitDataStagingWriter, PreparableWriter {

  private final CommitDataStagingWriter stagingWriter;
  private final PreparableWriter preparableWriter;

  public <W extends IndexWriter & CommitDataStagingWriter & PreparableWriter>
      MeteredStagingIndexWriter(
          W indexWriter, IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    super(indexWriter, indexingMetricsUpdater);
    this.stagingWriter = indexWriter;
    this.preparableWriter = indexWriter;
  }

  @Override
  public void stageUserData(EncodedUserData userData) throws IOException {
    this.stagingWriter.stageUserData(userData);
  }

  @Override
  public PrepareResult prepareWriter() throws IOException {
    return this.preparableWriter.prepareWriter();
  }
}
