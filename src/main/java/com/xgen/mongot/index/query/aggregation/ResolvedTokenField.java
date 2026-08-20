package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.util.FieldPath;
import java.util.Set;

/**
 * The resolved source a token-valued metric (e.g. {@code first}/{@code last}, {@code min}/{@code
 * max}) reads its value from.
 *
 * <p>Which source a path resolves to is decided once, at planning time, from the query's {@code
 * returnStoredSource} and the index definition; see {@code
 * LuceneAggregationContextFieldResolver#resolveTokenMetricField}. A path that neither source can
 * serve is rejected there rather than reaching a reader.
 */
public sealed interface ResolvedTokenField
    permits ResolvedTokenField.FromStoredSource, ResolvedTokenField.FromDocValues {

  /** The path the metric names, as the user wrote it. */
  FieldPath path();

  /**
   * Read from the document's stored source, for full BSON fidelity -- the value is exactly what was
   * indexed, including array order, duplicates, and nested documents.
   */
  record FromStoredSource(FieldPath path) implements ResolvedTokenField {}

  /**
   * Read from the per-type doc-values columns backing the path, avoiding a stored-fields fetch at
   * the cost of the fidelity limits documented on {@link DocValuesFieldReader}.
   *
   * @param types the BSON types the index definition allows the path to hold; columns absent from a
   *     given segment are skipped when reading it
   */
  record FromDocValues(FieldPath path, Set<TypeField> types) implements ResolvedTokenField {}
}
