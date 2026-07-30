package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;

/**
 * A leaf operator spec whose value is read from a token-valued field (a doc-values scalar column or
 * stored source) at a path, rather than from a numeric doc-values column.
 *
 * <p>Implemented by operators like {@code first}/{@code last} and {@code min}/{@code max}. It lets
 * {@link MetricFieldPaths} collect every such path from a spec tree, and the field resolver
 * pre-resolve them to {@link ResolvedTokenField}s, without the shared collector/resolver depending
 * on any specific operator spec type.
 */
public interface TokenMetricSpec {

  /** The field path this metric reads its value from. */
  FieldPath tokenMetricPath();
}
