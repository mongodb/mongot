package com.xgen.mongot.index.query.aggregation;

/** Positional aliases for facet branch and metric partial field names on the sharded merge wire. */
public final class FacetMetricAliases {

  private FacetMetricAliases() {}

  /** Alias for a top-level or nested facet branch at {@code index} in spec-tree order. */
  public static String facet(int index) {
    return "f" + index;
  }

  /** Alias for a metric wire partial field at {@code index} in spec-tree order. */
  public static String metric(int index) {
    return "m" + index;
  }
}
