package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;

/**
 * Runtime resolver for aggregation metric field paths.
 *
 * <p>Index-definition resolution is performed by {@link
 * com.xgen.mongot.index.lucene.LuceneAggregationContextFieldResolver} before execution; this
 * interface is the pre-resolved view wired through {@link AggregationContext}.
 */
public interface AggregationFieldResolver {

  ResolvedNumericMetricField resolveNumeric(FieldPath path);

  ResolvedTokenField resolveToken(FieldPath path);
}
