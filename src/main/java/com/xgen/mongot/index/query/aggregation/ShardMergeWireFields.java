package com.xgen.mongot.index.query.aggregation;

import java.util.ArrayList;
import java.util.List;

/** Wire partial field names shared by shard emit and merge-pipeline planning (CLOUDP-432762). */
final class ShardMergeWireFields {

  private ShardMergeWireFields() {}

  static List<String> wirePartials(OperatorAggregationSpec spec, List<Integer> sortDirections) {
    return switch (spec.getOperator()) {
      case SUM -> List.of(SumAggregationResult.wirePartialName(spec.name()));
      case COUNT -> List.of(CountAggregationResult.partialFieldName(spec.name()));
      case AVG ->
          List.of(
              AvgAggregationResult.wireSumPartialName(spec.name()),
              AvgAggregationResult.wireCountPartialName(spec.name()));
      case MIN -> List.of(MinAggregationResult.partialFieldName(spec.name()));
      case MAX -> List.of(MaxAggregationResult.partialFieldName(spec.name()));
      case FIRST -> firstLastWirePartials(Operator.FIRST, spec.name(), sortDirections);
      case LAST -> firstLastWirePartials(Operator.LAST, spec.name(), sortDirections);
    };
  }

  /** Merged accumulator field for {@code first}/{@code last} in the stage-1 {@code $group}. */
  static String firstLastMergedField(Operator operator, String metricName) {
    return operator.registryKey() + "_" + metricName;
  }

  private static List<String> firstLastWirePartials(
      Operator operator, String metricName, List<Integer> sortDirections) {
    List<String> fields = new ArrayList<>();
    fields.add(FirstLastShardMerge.valueField(operator, metricName));
    fields.add(FirstLastShardMerge.hasValueField(operator, metricName));
    if (!sortDirections.isEmpty()) {
      fields.add(FirstLastShardMerge.keyField(operator, metricName));
    }
    fields.add(firstLastMergedField(operator, metricName));
    return fields;
  }
}
