package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Picks each bucket's last document -- the one that sorts last under the query's global sort, or
 * the latest encountered when the query has none -- and reads {@code path} from it.
 *
 * <p>Collection runs over {@link AggregationContext#getInvertedSort}, because a {@link
 * org.apache.lucene.search.TopFieldCollector} only keeps a top document and {@code last} wants the
 * bottom one. Everything downstream then treats this like any other top.
 */
final class LastAccumulator extends FirstLastAccumulatorBase {

  /** Reverse doc order is the whole ordering when the query has no sort. */
  private static final Sort REVERSE_ENCOUNTER_ORDER =
      new Sort(new SortField(null, SortField.Type.DOC, true));

  LastAccumulator(String name, FieldPath path, AggregationContext context) {
    super(name, path, context, context.getInvertedSort(), collectorSort(context));
  }

  @Override
  protected AggregationResult createResult(
      Optional<FirstLastAggregationResultBase.Candidate> candidate) {
    return new LastAggregationResult(this.name, mergeSort(), candidate);
  }

  /**
   * The inverted sort with a descending doc-id tiebreak appended.
   *
   * <p>The tiebreak is applied here rather than by the query layer for two reasons. The sort spec
   * has no way to name a doc id, so it cannot be part of what {@link
   * com.xgen.mongot.index.query.sort.SortSpec#invert()} produces; and it has to stay out of the
   * merge sort, so the collected and merged orientations cannot be the same object. Doc ids are
   * comparable only within a partition, so a merge that consulted this component would order two
   * partitions' winners by raw doc id; across partitions that tiebreak belongs to the partition id,
   * which the merge applies separately.
   *
   * <p>It has to be spelled out at all because Lucene's queue resolves an equal sort key toward the
   * lower doc id, and that rule belongs to the queue rather than to any sort component -- so
   * inverting the components alone would leave a bucket keeping the earliest of the tied documents
   * instead of the latest.
   */
  private static Sort collectorSort(AggregationContext context) {
    Optional<Sort> inverted = context.getInvertedSort();
    if (inverted.isEmpty()) {
      return REVERSE_ENCOUNTER_ORDER;
    }
    SortField[] fields = inverted.get().getSort();
    SortField[] withTiebreak = new SortField[fields.length + 1];
    System.arraycopy(fields, 0, withTiebreak, 0, fields.length);
    withTiebreak[fields.length] = new SortField(null, SortField.Type.DOC, true);
    return new Sort(withTiebreak);
  }
}
