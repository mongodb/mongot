package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

public class UnresolvedAutoEmbeddingIndexGenerationTest {

  // The transient-resolution failure message carries the failure prefix matched by the MMS alert.
  private static final String EXPECTED_MESSAGE =
      AutoEmbeddingFailureMessages.withFailurePrefix(
          "Unable to create Automated Embedding index at this time");

  @Test
  public void create_mongoClientNotAvailable_failsNonRecoverableWithFailurePrefix() {
    IndexStatus status =
        createStatus(MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    assertEquals(IndexStatus.StatusCode.FAILED, status.getStatusCode());
    assertEquals(Optional.of(EXPECTED_MESSAGE), status.getMessage());
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED), status.getReason());
  }

  @Test
  public void create_retryableReason_failsRecoverableWithFailurePrefix() {
    IndexStatus status = createStatus(MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
    assertEquals(IndexStatus.StatusCode.FAILED, status.getStatusCode());
    assertEquals(Optional.of(EXPECTED_MESSAGE), status.getMessage());
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY), status.getReason());
  }

  @Test
  public void createCollectionNotFound_reportsDoesNotExist() {
    MaterializedViewIndexDefinitionGeneration definitionGeneration =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId());
    IndexStatus status =
        UnresolvedAutoEmbeddingIndexGeneration.createCollectionNotFound(definitionGeneration)
            .getIndex()
            .getStatus();
    assertEquals(IndexStatus.StatusCode.DOES_NOT_EXIST, status.getStatusCode());
    assertEquals(Optional.of(IndexStatus.Reason.COLLECTION_NOT_FOUND), status.getReason());
  }

  @Test
  public void createStale_delegatesStatusToReconstructedIndex() {
    BsonTimestamp optime = new BsonTimestamp(7, 1);
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus()).thenReturn(IndexStatus.stale("serving offline", optime));
    IndexStatus status =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
                MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId()),
                reconstructed,
                Map.of())
            .getIndex()
            .getStatus();
    assertEquals(IndexStatus.StatusCode.STALE, status.getStatusCode());
    assertEquals(Optional.of(optime), status.getOptime());
  }

  @Test
  public void createStale_dropDeletesReconstructedIndexData() throws Exception {
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", new BsonTimestamp(7, 1)));
    UnresolvedAutoEmbeddingIndexGeneration.createStale(
            MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId()),
            reconstructed,
            Map.of())
        .getIndex()
        .drop();
    // Dropping the phased-out generation must delete the reconstructed derived Lucene's on-disk
    // data, or the old directory leaks after recovery swaps in a new attempt.
    verify(reconstructed).drop();
  }

  @Test
  public void createStale_throwIfUnavailableForQuerying_delegatesToReconstructedIndex()
      throws Exception {
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", new BsonTimestamp(7, 1)));
    doThrow(new IndexUnavailableException("unavailable"))
        .when(reconstructed)
        .throwIfUnavailableForQuerying();
    var index =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
                MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId()),
                reconstructed,
                Map.of())
            .getIndex();
    // The offline serving gate must reject when the reconstructed index is unavailable rather than
    // serve from a bad reader; a status-only stub (no reconstructed index) stays a no-op.
    assertThrows(IndexUnavailableException.class, index::throwIfUnavailableForQuerying);
  }

  private IndexStatus createStatus(MaterializedViewTransientException.Reason reason) {
    return UnresolvedAutoEmbeddingIndexGeneration.createFailed(
            MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId()), reason)
        .getIndex()
        .getStatus();
  }
}
