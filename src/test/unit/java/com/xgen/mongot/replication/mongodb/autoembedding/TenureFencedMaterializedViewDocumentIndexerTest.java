package com.xgen.mongot.replication.mongodb.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class TenureFencedMaterializedViewDocumentIndexerTest {
  private final InitializedMaterializedViewIndex index =
      mock(InitializedMaterializedViewIndex.class);
  private final LeaseManager leaseManager = mock(LeaseManager.class);
  private final MaterializedViewGenerationId generationId =
      mock(MaterializedViewGenerationId.class);
  private final MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);

  private TenureFencedMaterializedViewDocumentIndexer createIndexer() {
    when(this.index.getWriter()).thenReturn(this.materializedViewWriter);
    when(this.materializedViewWriter.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
    // Needed by the DefaultDocumentIndexer this class delegates to.
    when(this.index.getDefinition()).thenReturn(SearchIndexDefinitionBuilder.VALID_INDEX);
    when(this.index.getGenerationId()).thenReturn(this.generationId);
    return new TenureFencedMaterializedViewDocumentIndexer(
        this.index, this.leaseManager, this.generationId);
  }

  @Test
  public void commit_currentTenure_delegates() throws IOException {
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L);

    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();
    indexer.beginTenure();
    indexer.commit();

    verify(this.materializedViewWriter).commit(EncodedUserData.EMPTY, 7L);
  }

  @Test
  public void commit_checkpointAdvancesLeaseVersion_usesOriginalTenure() throws Exception {
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L, 8L);
    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();

    indexer.beginTenure();
    indexer.indexDocumentEvent(mock(DocumentEvent.class));
    indexer.commit();

    verify(this.materializedViewWriter).updateIndex(org.mockito.ArgumentMatchers.any());
    verify(this.materializedViewWriter).commit(EncodedUserData.EMPTY, 7L);
    verify(this.leaseManager, times(1)).getOwnedLeaseVersion(this.generationId);
  }

  @Test
  public void commit_endedTenure_rejectsWork() throws Exception {
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L);
    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();
    indexer.beginTenure();
    indexer.endTenure();

    assertThrows(MaterializedViewNonTransientException.class, () -> indexer.commit());

    verify(this.materializedViewWriter, never()).commit(EncodedUserData.EMPTY, 7L);
  }

  @Test
  public void endTenure_discardsWritesBufferedDuringThisTenure() throws Exception {
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L);
    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();

    indexer.beginTenure();
    indexer.indexDocumentEvent(mock(DocumentEvent.class));
    indexer.endTenure();

    // Writes queued but never committed by an ended tenure must not survive to be fenced with a
    // later tenure's lease version by a subsequent commit() on the shared writer.
    verify(this.materializedViewWriter).discardBufferedWrites();
  }

  @Test
  public void commit_installedSupplier_isReadAtCommitTimeAndStampedWithTheTenureVersion()
      throws Exception {
    // updateCommitUserDataSupplier() exists so a writer whose resume position is assembled from
    // live state (the parallel collection scan, whose partitions advance independently) is read at
    // commit time rather than staged per batch. Verify the supplier really is resolved at commit
    // time, not captured when installed, and that the resulting write still carries this tenure's
    // pinned lease version rather than a live read.
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L);
    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();
    indexer.beginTenure();

    AtomicReference<IndexCommitUserData> live =
        new AtomicReference<>(IndexCommitUserData.createExceeded("stale-at-install"));
    indexer.updateCommitUserDataSupplier(live::get);

    // Move the live value after installing the supplier: a supplier captured at install time would
    // commit the old value.
    IndexCommitUserData advanced = IndexCommitUserData.EMPTY;
    live.set(advanced);
    indexer.commit();

    verify(this.materializedViewWriter).commit(advanced.toEncodedData(), 7L);

    // The tenure gate covers the supplier setter too.
    indexer.endTenure();
    assertEquals(
        MaterializedViewNonTransientException.Reason.INVALID_LEASE,
        assertThrows(
                MaterializedViewNonTransientException.class,
                () -> indexer.updateCommitUserDataSupplier(live::get))
            .getReason());
  }

  @Test
  public void allWork_afterLeaseLossEndsTenure_isRejectedAsInvalidLeaseAndNeverReachesWriter()
      throws Exception {
    // Losing the lease mid-stream demotes the generator, and shutdown() ends its tenure while
    // indexing work for the old tenure may still be queued. Every entry point must then reject
    // that work as a typed INVALID_LEASE MaterializedViewNonTransientException rather than let it
    // through: that is the reason EmbeddingIndexingWorkScheduler#handleBatchException maps to a
    // graceful SteadyStateException.createShutDown()/InitialSyncException.createShutDown(), so an
    // ordinary demotion drains the batch instead of failing the index. Nothing may reach the
    // shared writer after the tenure ends.
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L);
    TenureFencedMaterializedViewDocumentIndexer indexer = createIndexer();
    indexer.beginTenure();

    // Work accepted while the tenure is live.
    indexer.indexDocumentEvent(mock(DocumentEvent.class));
    verify(this.materializedViewWriter, times(1)).updateIndex(org.mockito.ArgumentMatchers.any());

    // Lease lost: shutdown() ends the tenure.
    indexer.endTenure();

    assertEquals(
        MaterializedViewNonTransientException.Reason.INVALID_LEASE,
        assertThrows(
                MaterializedViewNonTransientException.class,
                () -> indexer.indexDocumentEvent(mock(DocumentEvent.class)))
            .getReason());
    assertEquals(
        MaterializedViewNonTransientException.Reason.INVALID_LEASE,
        assertThrows(MaterializedViewNonTransientException.class, indexer::commit).getReason());
    assertEquals(
        MaterializedViewNonTransientException.Reason.INVALID_LEASE,
        assertThrows(
                MaterializedViewNonTransientException.class,
                () -> indexer.prepareBatch(java.util.List.of()))
            .getReason());
    assertEquals(
        MaterializedViewNonTransientException.Reason.INVALID_LEASE,
        assertThrows(
                MaterializedViewNonTransientException.class,
                () -> indexer.clearIndex(IndexCommitUserData.EMPTY))
            .getReason());

    // No further writes reached the shared writer: still just the one pre-loss updateIndex, and
    // no commit at all.
    verify(this.materializedViewWriter, times(1)).updateIndex(org.mockito.ArgumentMatchers.any());
    verify(this.materializedViewWriter, never())
        .commit(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong());
  }

  @Test
  public void endTenure_lateDuplicateCall_doesNotDiscardSuccessorsBufferedWrites()
      throws Exception {
    // Simulates a stale/late second demotion of the same (now-dead) generator reaching
    // endTenure() again after a successor tenure, sharing the same underlying writer, has
    // already begun buffering its own writes (e.g. a duplicate MaterializedViewManager
    // #transitionToFollower call racing a stale generator reference). The predecessor's second
    // endTenure() must be a no-op: it must not discard the successor's legitimate buffer.
    when(this.leaseManager.getOwnedLeaseVersion(this.generationId)).thenReturn(7L, 8L);
    TenureFencedMaterializedViewDocumentIndexer predecessor = createIndexer();
    TenureFencedMaterializedViewDocumentIndexer successor = createIndexer();

    predecessor.beginTenure();
    predecessor.endTenure();
    verify(this.materializedViewWriter, times(1)).discardBufferedWrites();

    successor.beginTenure();
    successor.indexDocumentEvent(mock(DocumentEvent.class));

    // The late, duplicate call on the already-ended predecessor must not fire a second discard.
    predecessor.endTenure();
    verify(this.materializedViewWriter, times(1)).discardBufferedWrites();

    // The successor's buffered write survives to be committed under its own tenure.
    successor.commit();
    verify(this.materializedViewWriter).commit(EncodedUserData.EMPTY, 8L);
  }
}
