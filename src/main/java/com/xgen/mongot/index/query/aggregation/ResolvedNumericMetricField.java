package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.definition.NumericFieldOptions;

/** Lucene doc-values field and representation for a resolved numeric aggregation metric path. */
public record ResolvedNumericMetricField(
    String luceneFieldName, NumericFieldOptions.Representation representation) {}
