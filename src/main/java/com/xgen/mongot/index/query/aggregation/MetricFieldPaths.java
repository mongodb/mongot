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

  /** Paths referenced by numeric metric specs (sum/avg). */
  public static Set<FieldPath> collectNumeric(TopAggregationSpec root) {
    return collect(
        root,
        spec ->
            switch (spec) {
              case SumAggregationSpec sum -> Optional.of(sum.getPath());
              case AvgAggregationSpec avg -> Optional.of(avg.getPath());
              // AggregationSpec is sealed (Top/Group/Operator). Sum/Avg are the only Operator
              // subtypes that contribute a numeric path; a default covers the rest without
              // listing every remaining subtype.
              default -> Optional.empty();
            });
  }

  /**
   * Paths referenced by multi-typed metric specs (min/max).
   *
   * <p>These are not resolved to a single doc-values column the way numeric paths are -- min/max
   * probes every column its path is indexed under at collection time. Schema eligibility is checked
   * in {@code AggregationValidator}, not here.
   */
  public static Set<FieldPath> collectMixed(TopAggregationSpec root) {
    return collect(
        root,
        spec ->
            switch (spec) {
              case MinAggregationSpec min -> Optional.of(min.getPath());
              case MaxAggregationSpec max -> Optional.of(max.getPath());
              // Same sealed-hierarchy default as collectNumeric.
              default -> Optional.empty();
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
              // Same sealed-hierarchy default as collectNumeric.
              default -> Optional.empty();
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
