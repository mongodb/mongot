package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;

public class UnresolvedAutoEmbeddingIndexGenerationTest {

  private static final String EXPECTED_MESSAGE =
      "Automated Embedding Index Failed: Unable to create Automated Embedding index at this time";

  @Test
  public void create_mongoClientNotAvailable_failsNonRecoverableWithPrefix() {
    IndexStatus status =
        createStatus(MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    assertEquals(IndexStatus.StatusCode.FAILED, status.getStatusCode());
    assertEquals(Optional.of(EXPECTED_MESSAGE), status.getMessage());
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED), status.getReason());
  }

  @Test
  public void create_retryableReason_failsRecoverableWithPrefix() {
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

  private IndexStatus createStatus(MaterializedViewTransientException.Reason reason) {
    MaterializedViewIndexDefinitionGeneration definitionGeneration =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId());
    return UnresolvedAutoEmbeddingIndexGeneration.createFailed(definitionGeneration, reason)
        .getIndex()
        .getStatus();
  }
}
