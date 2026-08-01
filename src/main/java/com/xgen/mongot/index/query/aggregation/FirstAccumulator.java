package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Picks each bucket's first document -- the one that sorts first under the query's global sort, or
 * the earliest encountered when the query has none -- and reads {@code path} from it.
 *
 * <p>Neither case needs the sort turned around. A single-hit {@link
 * org.apache.lucene.search.TopFieldCollector} over the query's own sort already keeps exactly the
 * document {@code first} wants, ties included, since Lucene's queue resolves an equal sort key
 * toward the lower doc id.
 */
final class FirstAccumulator extends FirstLastAccumulatorBase {

  /** Doc order is the whole ordering when the query has no sort. */
  private static final Sort ENCOUNTER_ORDER = new Sort(SortField.FIELD_DOC);

  FirstAccumulator(String name, FieldPath path, AggregationContext context) {
    super(name, path, context, context.getSort(), context.getSort().orElse(ENCOUNTER_ORDER));
  }

  @Override
  protected AggregationResult createResult(
      Optional<FirstLastAggregationResultBase.Candidate> candidate) {
    return new FirstAggregationResult(this.name, mergeSort(), candidate);
  }
}
