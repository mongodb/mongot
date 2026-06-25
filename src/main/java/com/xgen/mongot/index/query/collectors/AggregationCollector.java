package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.operators.Operator;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonValue;

/**
 * Collector for search aggregation. Holds the query predicate and top-level aggregation definition.
 */
public record AggregationCollector(Operator operator, Map<String, AggDefinition> aggDefinitions)
    implements Collector {

  @VisibleForTesting
  public static AggregationCollector create(
      Operator operator, Map<String, AggDefinition> aggDefinitions) {
    return new AggregationCollector(operator, aggDefinitions);
  }

  @Override
  public BsonValue collectorToBson() {
    // TODO(CLOUDP-415375)
    throw new UnsupportedOperationException(
        "AggregationCollector BSON serialization is not implemented");
  }

  @Override
  public Type getType() {
    return Type.AGGREGATION;
  }

  @Override
  public Optional<Operator> getOperator() {
    return Optional.of(this.operator);
  }
}
