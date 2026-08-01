package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Collects the metric field paths referenced by an aggregation spec tree, so the field resolver can
 * pre-resolve them before Lucene execution.
 */
public final class MetricFieldPaths {
  private MetricFieldPaths() {}

  /** Paths referenced by numeric metric specs (sum/avg/min/max). */
  public static Set<FieldPath> collectNumeric(TopAggregationSpec root) {
    return collect(
        root,
        spec ->
            switch (spec) {
              case SumAggregationSpec sum -> Optional.of(sum.getPath());
              case AvgAggregationSpec avg -> Optional.of(avg.getPath());
              case MinAggregationSpec min -> Optional.of(min.getPath());
              case MaxAggregationSpec max -> Optional.of(max.getPath());
              case TopAggregationSpec top -> Optional.empty();
              case GroupAggregationSpec group -> Optional.empty();
              case OperatorAggregationSpec operator -> Optional.empty();
            });
  }

  /** Paths referenced by token metric specs (first/last). */
  public static Set<FieldPath> collectToken(TopAggregationSpec root) {
    return collect(
        root,
        spec ->
            switch (spec) {
              case FirstAggregationSpec first -> Optional.of(first.getPath());
              case LastAggregationSpec last -> Optional.of(last.getPath());
              case TopAggregationSpec top -> Optional.empty();
              case GroupAggregationSpec group -> Optional.empty();
              case OperatorAggregationSpec operator -> Optional.empty();
            });
  }

  private static Set<FieldPath> collect(
      TopAggregationSpec root, Function<AggregationSpec, Optional<FieldPath>> pathExtractor) {
    Set<FieldPath> paths = new LinkedHashSet<>();
    for (AggregationSpec child : root.children()) {
      collect(child, pathExtractor, paths);
    }
    return paths;
  }

  private static void collect(
      AggregationSpec spec,
      Function<AggregationSpec, Optional<FieldPath>> pathExtractor,
      Set<FieldPath> paths) {
    pathExtractor.apply(spec).ifPresent(paths::add);
    for (AggregationSpec child : spec.children()) {
      collect(child, pathExtractor, paths);
    }
  }
}
