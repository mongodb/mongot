package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

// Multi-field aggregations. There should at least one intermediate aggregation in aggs
public record AggDefinition(List<FacetDefinition> aggs, LeafOperator leafOperator) {

  // In Search Aggregation, leaf operators sit at leaf nodes of the Accumulator tree,
  // representing metric operators avg / sum / count etc...
  public record LeafOperator(String name, Optional<FieldPath> path) {}

  @VisibleForTesting
  static AggDefinition create(List<FacetDefinition> aggs, LeafOperator leafOperator) {
    return new AggDefinition(aggs, leafOperator);
  }
}
