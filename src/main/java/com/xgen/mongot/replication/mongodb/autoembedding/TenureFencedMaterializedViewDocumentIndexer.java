package com.xgen.mongot.replication.mongodb.autoembedding;

import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Restricts a materialized-view indexer to the lease tenure that activated its generator.
 *
 * <p>The document-level operations are the standard {@link DefaultDocumentIndexer} behaviour behind
 * a tenure check, so they delegate rather than reimplement it. The commit-user-data operations are
 * kept here instead: {@code commit()} has to stamp the write with the lease version pinned at
 * {@link #beginTenure()} rather than read it live, and it needs the commit user data to do so,
 * which the delegate keeps private. The delegate's own copy of that data is therefore never read or
 * written -- {@code DefaultDocumentIndexer#commit()} is never called on it.
 */
final class TenureFencedMaterializedViewDocumentIndexer implements DocumentIndexer {
  private final DefaultDocumentIndexer delegate;
  private final InitializedMaterializedViewIndex index;
  private final LeaseManager leaseManager;
  private final MaterializedViewGenerationId generationId;
  private final MaterializedViewWriter materializedViewWriter;
  private final AtomicReference<Supplier<IndexCommitUserData>> commitUserDataSupplier;
  private boolean activeTenure;
  private long tenureLeaseVersion;

  TenureFencedMaterializedViewDocumentIndexer(
      InitializedMaterializedViewIndex index,
      LeaseManager leaseManager,
      MaterializedViewGenerationId generationId) {
    this.delegate = DefaultDocumentIndexer.create(index);
    this.index = index;
    this.leaseManager = leaseManager;
    this.generationId = generationId;
    this.materializedViewWriter = index.getWriter();
    IndexCommitUserData initialCommitUserData =
        IndexCommitUserData.fromEncodedData(
            this.materializedViewWriter.getCommitUserData(), Optional.of(generationId));
    this.commitUserDataSupplier = new AtomicReference<>(() -> initialCommitUserData);
    this.activeTenure = false;
    this.tenureLeaseVersion = 0;
  }

  // Reads leaseVersion independently rather than being handed the value confirmed at
  // acquisition (e.g. tryAcquireLeadership()'s success). That's benign for leaseVersion today:
  // any currently self-owned version is a safe fencing floor (monotonic, and the ownership
  // check below throws if some other host has since taken over), so racing a concurrent renewal
  // just means pinning a slightly later, still-valid version.
  //
  // Design note for CLOUDP-439815 (or a future epoch-style token): this pattern does not
  // generalize. An identity token whose purpose is "which leadership instance is this" (unlike
  // leaseVersion's "safe fencing floor") must not be independently re-derived here — two racing
  // acquisitions for the same host both pass the ownership check, so this method has no way to
  // tell that it observed a later acquisition's token than the one its caller actually confirmed.
  // Source such a token by threading it explicitly from the point of acquisition
  // (tryAcquireLeadership() returning the value it just wrote, passed through
  // becomeLeader(token) into beginTenure(token)) instead of re-reading shared state here.
  synchronized void beginTenure() {
    long leaseVersion = this.leaseManager.getOwnedLeaseVersion(this.generationId);
    if (leaseVersion <= 0) {
      throw invalidTenure();
    }
    this.tenureLeaseVersion = leaseVersion;
    this.activeTenure = true;
  }

  synchronized void endTenure() {
    if (!this.activeTenure) {
      // Already ended (e.g. a duplicate/late call racing a stale generator reference through
      // MaterializedViewManager#transitionToFollower). Must be a no-op: discarding again here
      // would wipe out whatever a successor tenure has since buffered on the shared writer.
      return;
    }
    this.activeTenure = false;
    // This tenure's own indexDocumentEvent() calls are already serialized against this endTenure()
    // by synchronized. Any writes still buffered here belong to this ended tenure and must not
    // survive to be fenced with a future tenure's lease version by a later commit() on the shared
    // writer.
    this.materializedViewWriter.discardBufferedWrites();
  }

  @Override
  public synchronized void indexDocumentEvent(DocumentEvent event)
      throws FieldExceededLimitsException {
    verifyActiveTenure();
    this.delegate.indexDocumentEvent(event);
  }

  @Override
  public synchronized List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
    verifyActiveTenure();
    return this.delegate.prepareBatch(events);
  }

  @Override
  public synchronized void updateCommitUserData(IndexCommitUserData commitUserData) {
    verifyActiveTenure();
    this.commitUserDataSupplier.set(() -> commitUserData);
  }

  @Override
  public synchronized void updateCommitUserDataSupplier(
      Supplier<IndexCommitUserData> commitUserDataSupplier) {
    verifyActiveTenure();
    this.commitUserDataSupplier.set(commitUserDataSupplier);
  }

  @Override
  public void stageCommitUserData() {
    // The materialized-view writer has no staging path; durability comes from the per-batch commit.
  }

  /**
   * Commits with the lease version pinned by {@link #beginTenure()} rather than the version the
   * lease holds now, so writes from an ended tenure cannot be stamped with a successor's token.
   */
  @Override
  public synchronized void commit() throws IOException {
    verifyActiveTenure();
    this.materializedViewWriter.commit(
        this.commitUserDataSupplier.get().get().toEncodedData(), this.tenureLeaseVersion);
  }

  @Override
  public synchronized void clearIndex(IndexCommitUserData commitUserData) throws IOException {
    verifyActiveTenure();
    this.index.clear(commitUserData.toEncodedData());
    updateCommitUserData(commitUserData);
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return this.delegate.exceededLimits();
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.delegate.getIndexDefinition();
  }

  // TODO(CLOUDP-434414): commit() and clearIndex() throw INVALID_LEASE once the tenure has ended,
  // and every call site for them in ReplicationIndexManager is wrapped in
  // Crash.because(...).ifThrows(...), which halts the JVM instead of propagating. No current path
  // reaches them with an ended tenure: the tenure ends only inside MaterializedViewGenerator's
  // shutdown(), atomically with the SHUT_DOWN transition, and each of those call sites either
  // re-checks isShutDown() under the generator monitor or runs as part of that same transition.
  // That argument spans several call sites and nothing enforces it, so route INVALID_LEASE to a
  // graceful replication shutdown at those call sites instead, and an ordinary demotion can then
  // never become a process kill.
  private void verifyActiveTenure() {
    if (!this.activeTenure) {
      throw invalidTenure();
    }
  }

  private MaterializedViewNonTransientException invalidTenure() {
    return new MaterializedViewNonTransientException(
        "Materialized-view work cannot run without an active lease tenure",
        MaterializedViewNonTransientException.Reason.INVALID_LEASE);
  }
}
