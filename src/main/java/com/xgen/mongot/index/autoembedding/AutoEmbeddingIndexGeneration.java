package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory.createMaterializedViewIndexDefinitionGeneration;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import java.util.Optional;

/** Composite IndexGeneration for Materialized View based AutoEmbedding index */
public class AutoEmbeddingIndexGeneration extends IndexGeneration {
  /** Mat View Index Generation which only includes mat view index and auto embedding definition. */
  private final MaterializedViewIndexGeneration matViewIndexGeneration;

  /** Derived Index Generation which only includes regular vector index information. */
  private final IndexGeneration derivedIndexGeneration;

  /**
   * AutoEmbeddingIndexGeneration takes a composite AutoEmbeddingCompositeIndex, a raw
   * VectorIndexDefinitionGeneration from control plane and a derived
   * VectorIndexDefinitionGeneration generated from Mongot.
   *
   * <p>For example: rawDefinitionGeneration: {indexID:123, database: "sample_mflix",
   * lastObservedCollectionName: "movies", fields:[{type: "autoEmbed", "path": "title", "model":
   * "voyage-3-large"}]}
   *
   * <p>derivedIndexDefinitionGeneration: {indexID:123, database: "__mdb_internal_search",
   * lastObservedCollectionName: "123", fields:[{type: "vector", "path": "title", "dimension":
   * 1024}]}
   */
  public AutoEmbeddingIndexGeneration(
      AutoEmbeddingCompositeIndex compositeIndex,
      IndexDefinitionGeneration rawDefinitionGeneration,
      IndexDefinitionGeneration derivedIndexDefinitionGeneration) {
    // Keep rawDefinitionGeneration in AutoEmbeddingIndexGeneration for ConfigState to use. this is
    // also used by ConfigManager, so AutoEmbeddingIndexGeneration needs to return
    // raw definition and raw generation id.
    super(compositeIndex, rawDefinitionGeneration);
    this.matViewIndexGeneration =
        new MaterializedViewIndexGeneration(
            compositeIndex.matViewIndex,
            createMaterializedViewIndexDefinitionGeneration(rawDefinitionGeneration));
    this.derivedIndexGeneration =
        new DerivedLocalIndexGeneration(
            compositeIndex.derivedIndex,
            derivedIndexDefinitionGeneration,
            () ->
                new AutoEmbeddingCommitMetadata(
                    compositeIndex.matViewIndex.isCurrentVersionQueryablePerLease(),
                    // Both getters are non-null, so these are always present when
                    // writing. The fields are Optional only so a commit from an older
                    // binary that predates them still parses as absent (see fromBson).
                    Optional.of(compositeIndex.matViewIndex.getMaterializedViewDatabaseName()),
                    Optional.of(
                        compositeIndex.matViewIndex.getMaterializedViewCollectionMetadata())));
  }

  /** Returns derived indexGeneration that contains Lucene index and Lucene index definition */
  public IndexGeneration getDerivedIndexGeneration() {
    return this.derivedIndexGeneration;
  }

  /**
   * Returns MaterializedViewIndexGeneration that contains MaterializedViewIndex and auto-embedding
   * MaterializedViewIndexDefinitionGeneration
   */
  public MaterializedViewIndexGeneration getMaterializedViewIndexGeneration() {
    return this.matViewIndexGeneration;
  }

  @Override
  public IndexDefinitionGeneration.Type getType() {
    return IndexDefinitionGeneration.Type.AUTO_EMBEDDING;
  }
}
