package com.xgen.testing.mongot.index.query;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorEmbeddedOptions;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;

public class ApproximateVectorQueryCriteriaBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<Vector> queryVector = Optional.empty();
  private Optional<VectorSearchQueryInput> query = Optional.empty();
  private Optional<VectorSearchFilter> filter = Optional.empty();
  private Optional<VectorSearchFilter> parentFilter = Optional.empty();
  private Optional<Integer> numCandidates = Optional.empty();
  private Optional<Integer> limit = Optional.empty();
  private final Optional<VectorSearchCriteria.ExplainOptions> explainOptions = Optional.empty();
  private Optional<Boolean> returnStoredSource = Optional.empty();
  private Optional<VectorEmbeddedOptions> embeddedOptions = Optional.empty();

  public static ApproximateVectorQueryCriteriaBuilder builder() {
    return new ApproximateVectorQueryCriteriaBuilder();
  }

  public static ApproximateVectorQueryCriteriaBuilder builder(
      ApproximateVectorSearchCriteria criteria) {

    ApproximateVectorQueryCriteriaBuilder builder =
        new ApproximateVectorQueryCriteriaBuilder()
            .path(criteria.path())
            .queryVector(criteria.queryVector().orElse(null))
            .numCandidates(criteria.getNumCandidates())
            .limit(criteria.limit())
            .returnStoredSource(criteria.returnStoredSource());

    criteria.filter().ifPresent(builder::filter);
    criteria.query().ifPresent(builder::query);
    criteria.parentFilter().ifPresent(builder::parentFilter);
    criteria.embeddedOptions().ifPresent(builder::embeddedOptions);
    return builder;
  }

  public ApproximateVectorQueryCriteriaBuilder path(String path) {
    return this.path(FieldPath.parse(path));
  }

  public ApproximateVectorQueryCriteriaBuilder path(FieldPath path) {
    this.path = Optional.of(path);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder queryVector(Vector vector) {
    this.queryVector = Optional.ofNullable(vector);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder query(VectorSearchQueryInput query) {
    this.query = Optional.ofNullable(query);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder filter(VectorSearchFilter filter) {
    this.filter = Optional.of(filter);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder parentFilter(VectorSearchFilter parentFilter) {
    this.parentFilter = Optional.of(parentFilter);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder numCandidates(int numCandidates) {
    this.numCandidates = Optional.of(numCandidates);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder limit(int limit) {
    this.limit = Optional.of(limit);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder returnStoredSource(Boolean returnStoredSource) {
    this.returnStoredSource = Optional.of(returnStoredSource);
    return this;
  }

  public ApproximateVectorQueryCriteriaBuilder embeddedOptions(
      VectorEmbeddedOptions embeddedOptions) {
    this.embeddedOptions = Optional.of(embeddedOptions);
    return this;
  }

  public ApproximateVectorSearchCriteria build() {
    Check.isPresent(this.path, "path");
    Check.checkArg(
        this.queryVector.isPresent() != this.query.isPresent(),
        "one of queryVector and query must be present");
    Check.isPresent(this.limit, "limit");
    Check.isPresent(this.path, "path");

    return new ApproximateVectorSearchCriteria(
        this.path.get(),
        this.queryVector,
        this.query,
        this.filter,
        this.parentFilter,
        this.limit.get(),
        this.numCandidates,
        this.explainOptions,
        this.returnStoredSource.orElse(false),
        this.embeddedOptions);
  }
}
