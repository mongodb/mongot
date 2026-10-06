package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import com.xgen.mongot.index.query.aggregation.DocValuesFieldReader.Selection;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

/**
 * Shared Lucene leaf accumulator for the {@code min} / {@code max} aggregation operators.
 *
 * <p>Both operators read every doc-values column {@link DocValuesFieldReader} supports for the
 * metric path, so a path indexed under several types at once -- a number in one document, a string
 * in another -- contributes all of them. Values are ordered by the BSON total ordering, matching
 * how a mixed sort on the same path would rank them; see the <a
 * href="https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/">BSON comparison
 * order</a>.
 *
 * <p>Unlike sum/avg, a multi-valued metric field does contribute: the extremum of the array is
 * taken, so {@code [-1, 2, 4]} contributes {@code -1} to a min and {@code 4} to a max.
 *
 * <p>Also unlike sum/avg, a doc that a multi-valued ancestor group field fanned into several
 * buckets contributes to every one of them rather than being skipped: selecting an extremum is
 * idempotent, so the doc counting in each bucket it belongs to is the correct answer rather than
 * double-counting.
 */
final class MinMaxAccumulator extends Accumulator {

  /** Which extremum to select. */
  enum Extremum {
    MIN {
      @Override
      MetricAggregationResult newResult(String name, Optional<BsonValue> value) {
        return new MinAggregationResult(name, value);
      }
    },
    MAX {
      @Override
      MetricAggregationResult newResult(String name, Optional<BsonValue> value) {
        return new MaxAggregationResult(name, value);
      }
    };

    /**
     * Whether {@code candidate} is more extreme than the running extremum, in BSON order.
     *
     * <p>Uses {@link SortUtil} so collection matches mixed sort. Partition/shard merge uses {@link
     * MinMaxSelection} ({@code MqlComparator}) instead; the two disagree only on large-magnitude
     * {@code int64} vs {@code double} (see {@link SortUtil} javadoc). That residual is pinned by
     * test, not closed here. Nulls sort below numbers, matching BSON total order rather than a
     * user-facing sort option.
     */
    final boolean replaces(BsonValue candidate, BsonValue current) {
      int comparison = SortUtil.mqlMixedCompare(candidate, current, NullEmptySortPosition.LOWEST);
      return this == MIN ? comparison < 0 : comparison > 0;
    }

    abstract MetricAggregationResult newResult(String name, Optional<BsonValue> value);

    final Selection selection() {
      return this == MIN ? Selection.MIN : Selection.MAX;
    }
  }

  private final FieldPath path;
  private final Extremum extremum;
  // Running extremum per bucket ord. An unset slot reads as empty, which is what distinguishes a
  // bucket that has seen no value from one whose extremum happens to be a falsy value.
  private final ObjectArray<BsonValue> extrema;

  MinMaxAccumulator(String name, FieldPath path, Extremum extremum, AggregationContext context) {
    super(name, List.of(), context);
    this.path = path;
    this.extremum = extremum;
    this.extrema = new ObjectArray<>(8);
    declareMetric(Optional.of(path.toString()));
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    DocValuesFieldReader values =
        DocValuesFieldReader.create(
            this.path,
            DocValuesFieldReader.SUPPORTED_TYPES,
            context.getLeafReaderContext(),
            this.extremum.selection());
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        maybeRecordCollectInvocation();
        // Deliberately no context.isMultiValued(doc) guard. sum/avg skip a doc that an ancestor
        // group field fanned into several buckets, because adding its value to each of them
        // double-counts. min/max are selective rather than additive, so contributing the same
        // value to every bucket the doc belongs to is correct, not double-counting -- and
        // idempotence is also what keeps the cross-shard $min/$max merge in agreement.
        //
        // Missing metric values do not contribute. BSON null does: it is a type, not an absence.
        extremumOf(values, doc)
            .ifPresent(
                candidate ->
                    MinMaxAccumulator.this.update(Math.toIntExact(owningBucketOrd), candidate));
      }
    };
  }

  /**
   * The extremum of every value {@code doc} has across all columns, or empty when the document has
   * no value for the path at all.
   *
   * <p>A multi-valued (array) field contributes only its own extremum: {@link Selection#MIN} /
   * {@link Selection#MAX} collapse each column to a single value before it is decoded. Columns are
   * opened in BSON bracket order, so the first populated column is this document's min and the last
   * populated column is its max. {@link DocValuesFieldReader#read} is not used here because several
   * columns would come back as a {@code BsonArray} rather than as that pick.
   */
  private Optional<BsonValue> extremumOf(DocValuesFieldReader values, int doc) throws IOException {
    List<DocValuesFieldReader.Column> columns = values.columns();
    if (this.extremum == Extremum.MIN) {
      for (DocValuesFieldReader.Column column : columns) {
        List<BsonValue> columnValues = column.values(doc);
        if (!columnValues.isEmpty()) {
          return Optional.of(columnValues.get(0));
        }
      }
    } else {
      for (int i = columns.size() - 1; i >= 0; i--) {
        List<BsonValue> columnValues = columns.get(i).values(doc);
        if (!columnValues.isEmpty()) {
          return Optional.of(columnValues.get(0));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean supportsStateMerge() {
    return true;
  }

  @Override
  public void mergeStateFrom(Accumulator other) {
    MinMaxAccumulator that = Check.instanceOf(other, MinMaxAccumulator.class);
    // Unset slots are empty, not a stored extremum. Skip them so a slice that never saw the
    // bucket cannot overwrite the other side with a missing value.
    for (int ord = 0; ord < that.extrema.size(); ord++) {
      Optional<BsonValue> value = that.extrema.get(ord);
      if (value.isPresent()) {
        update(ord, value.get());
      }
    }
  }

  private void update(int bucketOrd, BsonValue candidate) {
    Optional<BsonValue> current = this.extrema.get(bucketOrd);
    if (current.isEmpty() || this.extremum.replaces(candidate, current.get())) {
      this.extrema.set(bucketOrd, candidate);
    }
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      int bucketOrd = (int) ordsToCollect.get(i);
      results.add(this.extremum.newResult(this.name, this.extrema.get(bucketOrd)));
    }
    return results;
  }
}
