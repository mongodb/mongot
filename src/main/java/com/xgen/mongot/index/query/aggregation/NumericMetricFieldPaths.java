package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.LinkedHashSet;
import java.util.Set;

/** Collects numeric metric field paths referenced by sum/avg specs in an aggregation tree. */
public final class NumericMetricFieldPaths {
  private NumericMetricFieldPaths() {}

  public static Set<FieldPath> collect(TopAggregationSpec root) {
    Set<FieldPath> paths = new LinkedHashSet<>();
    for (AggregationSpec child : root.children()) {
      collect(child, paths);
    }
    return paths;
  }

  private static void collect(AggregationSpec spec, Set<FieldPath> paths) {
    switch (spec) {
      case SumAggregationSpec sum -> paths.add(sum.getPath());
      case AvgAggregationSpec avg -> paths.add(avg.getPath());
      case TopAggregationSpec top -> {}
      case GroupAggregationSpec group -> {}
      case OperatorAggregationSpec operator -> {}
    }
    for (AggregationSpec child : spec.children()) {
      collect(child, paths);
    }
  }
}
