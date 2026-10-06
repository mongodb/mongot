package com.xgen.mongot.replication.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DocumentIndexer} scoped to a single VectorLite leadership session.
 *
 * <p>The shared per-generation {@link DefaultDocumentIndexer} is reused across leadership sessions,
 * so a late token update from an old session can overwrite the in-memory commit user data the next
 * session relies on, and an initial-sync batch whose native stage slips past {@code RIM.shutdown()}
 * can advance the resume position ahead of the documents the next session has actually committed --
 * leaving a silent gap on the next resume. This indexer closes that: every leadership session gets
 * its own indexer with its own commit user data, seeded from the token the writer was prepared
 * with, fenced to the session's tenure.
 *
 * <p>{@link RecyclableReplicationIndexManager} begins the tenure before it creates the session's
 * {@link ReplicationIndexManager} (whose init may write immediately) and ends it when that
 * manager's shutdown future completes, success or failure. After the tenure ends every mutating
 * call -- batch or status -- is dropped and counted on {@link #STALE_SESSION_WRITE_COUNTER_NAME}.
 * It is dropped, not rejected, and dropping stale work from a shut-down session is safe because the
 * next session re-derives its resume position from the prepared token.
 *
 * <p>The document-level operations are the standard {@link DefaultDocumentIndexer} behaviour behind
 * the tenure check, so they delegate rather than reimplement it (mirroring {@code
 * TenureFencedMaterializedViewDocumentIndexer}). The commit-user-data operations are kept here so
 * this session owns its own resume position instead of sharing the delegate's.
 */
final class SessionFencedDocumentIndexer implements DocumentIndexer {

  private static final Logger LOG = LoggerFactory.getLogger(SessionFencedDocumentIndexer.class);

  /** Counts mutating calls dropped because they arrived after this session's tenure ended. */
  @VisibleForTesting
  static final String STALE_SESSION_WRITE_COUNTER_NAME = "recyclableStaleSessionWrites";

  private final DefaultDocumentIndexer delegate;
  private final InitializedIndex index;
  private final Counter staleSessionWrites;

  /**
   * This session's own commit user data, seeded from the token the writer was prepared with and
   * never shared with another session (fixes N4). Either a constant holding the last staged blob
   * or, for writers that assemble their position from live state, a supplier read at commit time;
   * last write wins -- matching {@link DefaultDocumentIndexer}.
   */
  private final AtomicReference<Supplier<IndexCommitUserData>> commitUserDataSupplier;

  @GuardedBy("this")
  private boolean activeTenure;

  /** The leadership epoch this tenure was begun under; correlates dropped stale work in logs. */
  @GuardedBy("this")
  private long tenureEpoch;

  SessionFencedDocumentIndexer(
      InitializedIndex index, IndexCommitUserData baseline, MeterRegistry meterRegistry) {
    this.delegate = DefaultDocumentIndexer.create(index);
    this.index = index;
    this.staleSessionWrites = meterRegistry.counter(STALE_SESSION_WRITE_COUNTER_NAME);
    this.commitUserDataSupplier = new AtomicReference<>(() -> baseline);
    this.activeTenure = false;
    this.tenureEpoch = 0;
  }

  /**
   * Begins this session's tenure. Called by {@link RecyclableReplicationIndexManager} before it
   * creates the session's {@link ReplicationIndexManager}, whose init may write immediately.
   */
  synchronized void beginTenure(long epoch) {
    this.tenureEpoch = epoch;
    this.activeTenure = true;
  }

  /**
   * Ends this session's tenure. Idempotent: it is called when the session's manager shutdown future
   * completes, which the terminal/drop paths may reach more than once.
   */
  synchronized void endTenure() {
    this.activeTenure = false;
  }

  @Override
  public synchronized void indexDocumentEvent(DocumentEvent event)
      throws FieldExceededLimitsException {
    if (dropIfEnded("indexDocumentEvent")) {
      return;
    }
    this.delegate.indexDocumentEvent(event);
  }

  @Override
  public synchronized List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
    if (dropIfEnded("prepareBatch")) {
      return events;
    }
    return this.delegate.prepareBatch(events);
  }

  @Override
  public synchronized void updateCommitUserData(IndexCommitUserData commitUserData) {
    if (dropIfEnded("updateCommitUserData")) {
      return;
    }
    this.commitUserDataSupplier.set(() -> commitUserData);
  }

  @Override
  public synchronized void updateCommitUserDataSupplier(
      Supplier<IndexCommitUserData> commitUserDataSupplier) {
    if (dropIfEnded("updateCommitUserDataSupplier")) {
      return;
    }
    this.commitUserDataSupplier.set(commitUserDataSupplier);
  }

  @Override
  public synchronized void stageCommitUserData() throws IOException {
    if (dropIfEnded("stageCommitUserData")) {
      return;
    }
    stageCurrentUserData();
  }

  @Override
  public synchronized void commit() throws IOException {
    if (dropIfEnded("commit")) {
      return;
    }
    // Stage first so a writer whose commit is a no-op (its engine schedules its own commits) still
    // receives the envelope, then commit -- mirrors DefaultDocumentIndexer.commit().
    stageCurrentUserData();
    this.index.getWriter().commit(currentUserData().toEncodedData());
  }

  @Override
  public synchronized void clearIndex(IndexCommitUserData commitUserData) throws IOException {
    if (dropIfEnded("clearIndex")) {
      return;
    }
    this.index.clear(commitUserData.toEncodedData());
    this.commitUserDataSupplier.set(() -> commitUserData);
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return this.delegate.exceededLimits();
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.delegate.getIndexDefinition();
  }

  @GuardedBy("this")
  private void stageCurrentUserData() throws IOException {
    if (this.index.getWriter() instanceof CommitDataStagingWriter stagingWriter) {
      stagingWriter.stageUserData(currentUserData().toEncodedData());
    }
  }

  private IndexCommitUserData currentUserData() {
    return this.commitUserDataSupplier.get().get();
  }

  /**
   * Returns true (counting and logging the drop) when this session's tenure has ended, so the
   * caller drops the stale call. CLOUDP-446295 PR 3 adds typed native rejections.
   */
  @GuardedBy("this")
  private boolean dropIfEnded(String operation) {
    if (this.activeTenure) {
      return false;
    }
    this.staleSessionWrites.increment();
    LOG.atDebug().log(
        "dropping stale {} on ended VectorLite leadership session (epoch {})",
        operation,
        this.tenureEpoch);
    return true;
  }
}
