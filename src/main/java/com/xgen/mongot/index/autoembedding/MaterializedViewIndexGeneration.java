package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.mongodb.leasing.LeaseManager.DEFAULT_INDEX_DEFINITION_VERSION;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.util.Check;
import java.util.Optional;

/**
 * MaterializedViewIndexGeneration is index generation subtype for auto-embedding Materialized View
 * usage only, this is only used in MaterializedViewGenerator for replication workload, ConfigState
 * uses AutoEmbeddingIndexGeneration instead for status reporting and query workload.
 */
public class MaterializedViewIndexGeneration extends IndexGeneration {
  private Optional<InitializedMaterializedViewIndex> activeIndex;

  public MaterializedViewIndexGeneration(
      InitializedMaterializedViewIndex index,
      MaterializedViewIndexDefinitionGeneration definitionGeneration) {
    super(index, definitionGeneration);
    this.activeIndex = Optional.empty();
  }

  @Override
  public MaterializedViewIndexDefinitionGeneration getDefinitionGeneration() {
    return Check.instanceOf(
        super.getDefinitionGeneration(), MaterializedViewIndexDefinitionGeneration.class);
  }

  @Override
  public MaterializedViewGenerationId getGenerationId() {
    return getDefinitionGeneration().getGenerationId();
  }

  @Override
  public InitializedMaterializedViewIndex getIndex() {
    return this.activeIndex.orElse(
        Check.instanceOf(super.getIndex(), InitializedMaterializedViewIndex.class));
  }

  @Override
  public VectorIndexDefinition getDefinition() {
    // TODO(CLOUDP-353553): Handle search index version - getIndexDefinition() now returns
    //  IndexDefinition which may be a SearchIndexDefinition.
    return getDefinitionGeneration().getIndexDefinition().asVectorDefinition();
  }

  public void swapIndex(InitializedMaterializedViewIndex activeIndex) {
    this.activeIndex = Optional.of(activeIndex);
  }

  /**
   * Compares a new index generation to this index generation to determine if we need to spin up a
   * new MaterializedViewGenerator or if we can re-use the one associated with this index
   * generation.
   *
   * <p>An example of when we need to spin up a new MaterializedViewGenerator is when the index
   * definition gets updated by the user.
   *
   * <p>An example of when we can re-use the current MaterializedViewGenerator is when a new index
   * generation was created because we fell off the oplog.
   *
   * @param newIndexGeneration the new index generation to compare against.
   * @return whether we need to spin up a new MaterializedViewGenerator.
   */
  public boolean needsNewMatViewGenerator(MaterializedViewIndexGeneration newIndexGeneration) {
    return this.getDefinitionGeneration()
            .definition()
            .getDefinitionVersion()
            .orElse(DEFAULT_INDEX_DEFINITION_VERSION)
        < newIndexGeneration
            .getDefinitionGeneration()
            .definition()
            .getDefinitionVersion()
            .orElse(DEFAULT_INDEX_DEFINITION_VERSION);
  }
}
