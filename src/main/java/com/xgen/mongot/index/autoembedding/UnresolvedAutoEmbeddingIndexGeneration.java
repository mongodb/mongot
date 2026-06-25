package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED;
import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY;
import static com.xgen.mongot.index.status.IndexStatus.Reason.COLLECTION_NOT_FOUND;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;

public class UnresolvedAutoEmbeddingIndexGeneration extends IndexGeneration {
  private UnresolvedAutoEmbeddingIndexGeneration(
      UnresolvedAutoEmbeddingIndex index, IndexDefinitionGeneration definitionGeneration) {
    super(index, definitionGeneration);
  }

  /**
   * Creates an unresolved generation that reports a FAILED status. Used when materialized view
   * resolution throws a {@link MaterializedViewTransientException}: the index parks in {@code
   * AUTO_EMBEDDING_RESOLUTION_FAILED} (client unavailable) or {@code
   * AUTO_EMBEDDING_RESOLUTION_RETRY} (retried when the sync source is updated), as opposed to the
   * quiet, terminal {@code COLLECTION_NOT_FOUND} produced by {@link #createCollectionNotFound}.
   */
  public static UnresolvedAutoEmbeddingIndexGeneration createFailed(
      IndexDefinitionGeneration definitionGeneration,
      MaterializedViewTransientException.Reason reason) {
    IndexStatus failedStatus =
        switch (reason) {
          case MONGO_CLIENT_NOT_AVAILABLE ->
              IndexStatus.failed(
                  AutoEmbeddingFailureMessages.withFailurePrefix(
                      "Unable to create Automated Embedding index at this time"),
                  AUTO_EMBEDDING_RESOLUTION_FAILED);
          default ->
              IndexStatus.failed(
                  AutoEmbeddingFailureMessages.withFailurePrefix(
                      "Unable to create Automated Embedding index at this time"),
                  AUTO_EMBEDDING_RESOLUTION_RETRY);
        };
    var failedIndex =
        new UnresolvedAutoEmbeddingIndex(definitionGeneration.getIndexDefinition(), failedStatus);
    return new UnresolvedAutoEmbeddingIndexGeneration(failedIndex, definitionGeneration);
  }

  /**
   * Creates an unresolved generation that reports {@code COLLECTION_NOT_FOUND}. Used when the
   * source collection is not present on this node's mongod (e.g. an unsharded collection on a
   * non-owner shard), so that no materialized view collection, lease, or {@code leaderStatus} gauge
   * is created. The {@code COLLECTION_NOT_FOUND} status drives the config manager's recovery loop,
   * which re-stages the index when the collection later appears on this shard.
   */
  public static UnresolvedAutoEmbeddingIndexGeneration createCollectionNotFound(
      IndexDefinitionGeneration definitionGeneration) {
    var unresolvedIndex =
        new UnresolvedAutoEmbeddingIndex(
            definitionGeneration.getIndexDefinition(),
            IndexStatus.doesNotExist(COLLECTION_NOT_FOUND));
    return new UnresolvedAutoEmbeddingIndexGeneration(unresolvedIndex, definitionGeneration);
  }
}
