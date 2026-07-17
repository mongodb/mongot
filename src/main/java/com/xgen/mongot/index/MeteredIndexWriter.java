package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.metrics.Timed;
import java.io.IOException;
import java.util.Optional;

public class MeteredIndexWriter implements IndexWriter {
  private final IndexWriter indexWriter;
  private final IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater;

  public MeteredIndexWriter(
      IndexWriter indexWriter, IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    this.indexWriter = indexWriter;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  @Override
  public void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException {
    this.indexWriter.updateIndex(event);
    this.indexingMetricsUpdater.getDocumentEventTypeCounter(event.getEventType()).increment();
    if (!event.getEventType().equals(DocumentEvent.EventType.DELETE)) {
      this.indexingMetricsUpdater
          .getTotalBytesProcessedCounter()
          .increment(event.getDocument().orElseThrow().getByteBuffer().remaining());
    }
  }

  @Override
  public void commit(EncodedUserData userData) throws IOException {
    Timed.runnable(
        this.indexingMetricsUpdater.getCommitTimer(), () -> this.indexWriter.commit(userData));
  }

  @Override
  public EncodedUserData getCommitUserData() {
    return this.indexWriter.getCommitUserData();
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return this.indexWriter.exceededLimits();
  }

  @Override
  public int getNumFields() throws WriterClosedException {
    return this.indexWriter.getNumFields();
  }

  @Override
  public void deleteAll(EncodedUserData userData) throws IOException {
    this.indexWriter.deleteAll(userData);
  }

  @Override
  public void close() throws IOException {
    this.indexWriter.close();
  }

  @Override
  public long getNumDocs() throws WriterClosedException {
    return this.indexWriter.getNumDocs();
  }

  @Override
  public void cancelMerges() throws IOException {
    this.indexWriter.cancelMerges();
  }

  @Override
  public void forceMerge(int maxNumSegments) throws IOException {
    this.indexWriter.forceMerge(maxNumSegments);
  }

  @VisibleForTesting
  public IndexWriter getWrapped() {
    return this.indexWriter;
  }
}
