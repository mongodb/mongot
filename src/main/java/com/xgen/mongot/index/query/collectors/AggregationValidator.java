package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

/**
 * Central home for semantic validation of aggregation-capable {@code facet} requests. Keeping these
 * checks in one place makes it easy to scope future aggregation-syntax validation here rather than
 * scattering it across query parsing.
 */
public class AggregationValidator {
  private final BsonParseContext bsonParseContext;

  public AggregationValidator(BsonParseContext bsonParseContext) {
    this.bsonParseContext = bsonParseContext;
  }

  /**
   * Rejects a global {@code sort}/{@code sortBetaV1} on an aggregation collector when no facet tree
   * carries a {@code first}/{@code last}-dependent metric. The borrowed global sort is only
   * meaningful for ordering such metrics, so a sort with nothing to order is a caller error rather
   * than being silently ignored.
   *
   * <p>The converse is deliberately allowed: a {@code first}/{@code last} metric without a global
   * field sort picks the first/last value in encountered (natural) order, mirroring MQL {@code
   * $first}/{@code $last} semantics.
   *
   * <p>Only user-field sorts are accepted: a meta (score) sort component is rejected, because
   * {@code last} under a score sort would need the globally lowest-scoring matching documents --
   * exactly the ones score pruning and early termination skip. Score-sorted {@code first} could be
   * admitted later without a breaking change; starting field-sort-only keeps the rule loosenable.
   */
  public void validateSort(AggregationCollector collector, Optional<SortSpec> sortSpec)
      throws BsonParseException {
    if (sortSpec.isEmpty()) {
      return;
    }
    if (!collector.usesFirstOrLast()) {
      this.bsonParseContext.handleSemanticError(
          "sort (or sortBetaV1) is only supported when at least one 'first' or 'last' aggregation"
              + " metric is specified");
    }
    boolean hasMetaSort =
        sortSpec.get().getSortFields().stream()
            .anyMatch(sortField -> sortField.options() instanceof MetaSortOptions);
    if (hasMetaSort) {
      this.bsonParseContext.handleSemanticError(
          "'first' and 'last' aggregation metrics only support sorting by document fields; a $meta"
              + " sort component is not supported");
    }
  }
}
