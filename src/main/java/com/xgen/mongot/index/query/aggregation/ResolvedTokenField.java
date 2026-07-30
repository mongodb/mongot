package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;

/**
 * The resolved source for a token-valued metric (e.g. {@code first}/{@code last}, {@code
 * min}/{@code max}).
 *
 * <p>For private preview this is just the stored-source path: values are read from stored source
 * for full BSON fidelity. Resolution validates that the path is covered by the index's stored
 * source. A scalar doc-values fast path is deferred (see the scalar-optimization follow-up ticket);
 * when it lands, this record regains an optional scalar-column source alongside the stored path.
 */
public record ResolvedTokenField(FieldPath storedSourcePath) {}
