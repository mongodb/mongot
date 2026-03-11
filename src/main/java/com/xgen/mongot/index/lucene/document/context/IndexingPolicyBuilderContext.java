package com.xgen.mongot.index.lucene.document.context;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import java.util.Set;

/**
 * Context passed to {@link
 * com.xgen.mongot.index.lucene.document.LuceneIndexingPolicy#createBuilder(byte[],
 * IndexingPolicyBuilderContext)} bundling per-document parameters needed for builder creation.
 */
public final class IndexingPolicyBuilderContext {

  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;
  private final Optional<Long> customVectorEngineId;
  private final Set<FieldPath> fieldPathsToFilterOut;

  private IndexingPolicyBuilderContext(Builder builder) {
    this.autoEmbeddings = builder.autoEmbeddings;
    this.customVectorEngineId = builder.customVectorEngineId;
    this.fieldPathsToFilterOut = builder.fieldPathsToFilterOut;
  }

  public ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings() {
    return this.autoEmbeddings;
  }

  public Optional<Long> customVectorEngineId() {
    return this.customVectorEngineId;
  }

  public Set<FieldPath> fieldPathsToFilterOut() {
    return this.fieldPathsToFilterOut;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings =
        ImmutableMap.of();
    private Optional<Long> customVectorEngineId = Optional.empty();
    private Set<FieldPath> fieldPathsToFilterOut = Set.of();

    private Builder() {}

    public Builder autoEmbeddings(
        ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
      this.autoEmbeddings = autoEmbeddings;
      return this;
    }

    public Builder customVectorEngineId(Optional<Long> customVectorEngineId) {
      this.customVectorEngineId = customVectorEngineId;
      return this;
    }

    public Builder fieldPathsToFilterOut(Set<FieldPath> fieldPathsToFilterOut) {
      this.fieldPathsToFilterOut = fieldPathsToFilterOut;
      return this;
    }

    public IndexingPolicyBuilderContext build() {
      return new IndexingPolicyBuilderContext(this);
    }
  }
}
