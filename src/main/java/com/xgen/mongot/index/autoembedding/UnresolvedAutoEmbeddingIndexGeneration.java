package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED;
import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY;
import static com.xgen.mongot.index.status.IndexStatus.Reason.COLLECTION_NOT_FOUND;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;

public class UnresolvedAutoEmbeddingIndexGeneration extends IndexGeneration {
  private static final String FAILURE_MESSAGE =
      AutoEmbeddingFailureMessages.withFailurePrefix(
          "Unable to create Automated Embedding index at this time");

  private UnresolvedAutoEmbeddingIndexGeneration(
      UnresolvedAutoEmbeddingIndex index, IndexDefinitionGeneration definitionGeneration) {
    super(index, definitionGeneration);
  }

  /**
   * Creates a STALE (not FAILED) generation: the materialized view could not be resolved but the
   * derived Lucene was reconstructed offline. Status delegates to {@code reconstructedDerivedIndex}
   * and {@code VectorSearchCommand} serves queries from it, remapping paths via {@code
   * autoEmbeddingFieldsMapping}. The recovery stager keeps retrying full resolution.
   */
  public static UnresolvedAutoEmbeddingIndexGeneration createStale(
      IndexDefinitionGeneration definitionGeneration,
      InitializedIndex reconstructedDerivedIndex,
      Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {
    var staleIndex =
        new UnresolvedAutoEmbeddingIndex(
            definitionGeneration.getIndexDefinition(),
            reconstructedDerivedIndex.getStatus(),
            Optional.of(reconstructedDerivedIndex),
            autoEmbeddingFieldsMapping);
    return new UnresolvedAutoEmbeddingIndexGeneration(staleIndex, definitionGeneration);
  }

  /**
   * Creates a FAILED generation after a {@link MaterializedViewTransientException} when there is no
   * queryable derived Lucene to serve: {@code AUTO_EMBEDDING_RESOLUTION_FAILED} (client
   * unavailable) or {@code AUTO_EMBEDDING_RESOLUTION_RETRY} (otherwise). The stager keeps retrying.
   */
  public static UnresolvedAutoEmbeddingIndexGeneration createFailed(
      IndexDefinitionGeneration definitionGeneration,
      MaterializedViewTransientException.Reason reason) {
    var failedIndex =
        new UnresolvedAutoEmbeddingIndex(
            definitionGeneration.getIndexDefinition(),
            IndexStatus.failed(FAILURE_MESSAGE, failedStatusReason(reason)),
            Optional.empty(),
            Map.of());
    return new UnresolvedAutoEmbeddingIndexGeneration(failedIndex, definitionGeneration);
  }

  private static IndexStatus.Reason failedStatusReason(
      MaterializedViewTransientException.Reason reason) {
    return switch (reason) {
      case MONGO_CLIENT_NOT_AVAILABLE -> AUTO_EMBEDDING_RESOLUTION_FAILED;
      default -> AUTO_EMBEDDING_RESOLUTION_RETRY;
    };
  }

  /**
   * Creates a {@code COLLECTION_NOT_FOUND} generation, used when the source collection is absent on
   * this node's mongod (e.g. an unsharded collection on a non-owner shard) so no materialized view,
   * lease, or {@code leaderStatus} gauge is created. The status drives the config manager's
   * recovery loop, which re-stages the index once the collection appears on this shard.
   */
  public static UnresolvedAutoEmbeddingIndexGeneration createCollectionNotFound(
      IndexDefinitionGeneration definitionGeneration) {
    var unresolvedIndex =
        new UnresolvedAutoEmbeddingIndex(
            definitionGeneration.getIndexDefinition(),
            IndexStatus.doesNotExist(COLLECTION_NOT_FOUND),
            Optional.empty(),
            Map.of());
    return new UnresolvedAutoEmbeddingIndexGeneration(unresolvedIndex, definitionGeneration);
  }
}
