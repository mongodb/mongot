package com.xgen.testing.mongot.index.query;

import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class VectorQueryBuilder {
  private Optional<String> index = Optional.empty();
  private Optional<VectorSearchCriteria> criteria = Optional.empty();
  private Optional<Boolean> returnStoredSource = Optional.empty();
  private Optional<Boolean> concurrent = Optional.empty();

  public static VectorQueryBuilder builder() {
    return new VectorQueryBuilder();
  }

  public static VectorQueryBuilder builder(VectorSearchQuery query) {
    return new VectorQueryBuilder().index(query.index()).criteria(query.criteria());
  }

  public VectorQueryBuilder index(String index) {
    this.index = Optional.of(index);
    return this;
  }

  public VectorQueryBuilder criteria(VectorSearchCriteria criteria) {
    this.criteria = Optional.of(criteria);
    return this;
  }

  public VectorQueryBuilder returnStoredSource(boolean returnStoredSource) {
    this.returnStoredSource = Optional.of(returnStoredSource);
    return this;
  }

  public VectorQueryBuilder concurrent(boolean concurrent) {
    this.concurrent = Optional.of(concurrent);
    return this;
  }

  public VectorSearchQuery build() {
    Check.isPresent(this.index, "index");
    Check.isPresent(this.criteria, "criteria");
    return new VectorSearchQuery(
        this.index.get(),
        this.criteria.get(),
        this.returnStoredSource.orElse(false),
        true,
        this.concurrent);
  }
}
