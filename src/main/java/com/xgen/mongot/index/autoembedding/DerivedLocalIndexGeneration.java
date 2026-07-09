package com.xgen.mongot.index.autoembedding;

import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import java.util.function.Supplier;

/**
 * The derived, locally-stored {@link IndexGeneration} of an auto-embedding composite index. Carries
 * a supplier of the auto-embedding commit metadata (e.g. the lease's per-definition-version
 * queryable ratchet) so replication can snapshot it into this index's commit user data. On a
 * restart that cannot reach mongod, that persisted metadata tells whether this index held complete
 * data and can be reported STALE instead of FAILED.
 */
public class DerivedLocalIndexGeneration extends IndexGeneration {
  private final Supplier<AutoEmbeddingCommitMetadata> commitMetadataSupplier;

  public DerivedLocalIndexGeneration(
      Index derivedIndex,
      IndexDefinitionGeneration derivedDefinitionGeneration,
      Supplier<AutoEmbeddingCommitMetadata> commitMetadataSupplier) {
    super(derivedIndex, derivedDefinitionGeneration);
    this.commitMetadataSupplier = commitMetadataSupplier;
  }

  public Supplier<AutoEmbeddingCommitMetadata> getCommitMetadataSupplier() {
    return this.commitMetadataSupplier;
  }
}
