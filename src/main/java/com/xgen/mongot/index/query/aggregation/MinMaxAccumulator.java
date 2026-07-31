package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.bson.BsonValue;

/**
 * Shared Lucene leaf accumulator for the {@code min} / {@code max} aggregation operators.
 *
 * <p>Both operators read the {@code NUMBER_*_V2} doc values of a numeric metric field (see {@link
 * ResolvedNumericMetricField}) and keep a running extremum per bucket ordinal, exactly like {@link
 * SumAccumulator} / {@link AvgAccumulator}. Non-numeric paths are rejected during field resolution,
 * so this accumulator only ever sees numbers.
 *
 * <p>Unlike sum/avg, a multi-valued metric field does contribute: the extremum of the array is
 * taken, so {@code [-1, 2, 4]} contributes {@code -1} to a min and {@code 4} to a max. {@link
 * SortedNumericDocValues} returns a doc's values in ascending encoded order, and both {@code INT64}
 * and {@code DOUBLE} encodings are order-preserving (see {@link NumericMetricValues}), so the array
 * extremum is just the first or last value -- no comparisons needed.
 *
 * <p>Also unlike sum/avg, a doc that a multi-valued ancestor group field fanned into several
 * buckets contributes to every one of them rather than being skipped: selecting an extremum is
 * idempotent, so the doc counting in each bucket it belongs to is the correct answer rather than
 * double-counting.
 *
 * <p>Comparison and per-bucket state stay in encoded {@code long} space; values are decoded to BSON
 * once, when results are built.
 */
final class MinMaxAccumulator extends Accumulator {

  /** Which extremum to select. */
  enum Extremum {
    MIN {
      @Override
      MetricAggregationResult newResult(String name, Optional<BsonValue> value) {
        return new MinAggregationResult(name, value);
      }

      @Override
      boolean replaces(long candidate, long current) {
        return candidate < current;
      }
    },
    MAX {
      @Override
      MetricAggregationResult newResult(String name, Optional<BsonValue> value) {
        return new MaxAggregationResult(name, value);
      }

      @Override
      boolean replaces(long candidate, long current) {
        return candidate > current;
      }
    };

    /** Whether the encoded {@code candidate} is more extreme than the running extremum. */
    abstract boolean replaces(long candidate, long current);

    abstract MetricAggregationResult newResult(String name, Optional<BsonValue> value);
  }

  private final ResolvedNumericMetricField metricField;
  private final Extremum extremum;
  // Running extremum per bucket ord, in encoded doc-value space.
  private final LongArray extrema;
  // Bucket ords that have seen at least one value; entries in `extrema` are meaningless without it,
  // since LongArray zero-fills and 0 is a legitimate encoded value.
  private final BitSet seen;

  MinMaxAccumulator(
      String name,
      ResolvedNumericMetricField metricField,
      Extremum extremum,
      AggregationContext context) {
    super(name, List.of(), context);
    this.metricField = metricField;
    this.extremum = extremum;
    this.extrema = new LongArray(8);
    this.seen = new BitSet();
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    SortedNumericDocValues values =
        DocValues.getSortedNumeric(
            context.getLeafReaderContext().reader(), this.metricField.luceneFieldName());
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        // Deliberately no context.isMultiValued(doc) guard. sum/avg skip a doc that an ancestor
        // group field fanned into several buckets, because adding its value to each of them
        // double-counts. min/max are selective rather than additive, so contributing the same
        // value to every bucket the doc belongs to is correct, not double-counting -- and
        // idempotence is also what keeps the cross-shard $min/$max merge in agreement.
        if (!values.advanceExact(doc)) {
          // Missing metric values do not contribute, matching $min/$max.
          return;
        }
        // Values arrive in ascending encoded order, so the array's min is the first value and its
        // max is the last one.
        int count = values.docValueCount();
        @Var long candidate = values.nextValue();
        if (MinMaxAccumulator.this.extremum == Extremum.MAX) {
          for (int i = 1; i < count; i++) {
            candidate = values.nextValue();
          }
        }
        MinMaxAccumulator.this.update(Math.toIntExact(owningBucketOrd), candidate);
      }
    };
  }

  private void update(int bucketOrd, long candidate) {
    if (!this.seen.get(bucketOrd)
        || this.extremum.replaces(candidate, this.extrema.get(bucketOrd))) {
      this.extrema.set(bucketOrd, candidate);
      this.seen.set(bucketOrd);
    }
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      int bucketOrd = (int) ordsToCollect.get(i);
      Optional<BsonValue> value =
          this.seen.get(bucketOrd)
              ? Optional.of(
                  NumericMetricValues.decodeToBson(
                      this.extrema.get(bucketOrd), this.metricField.representation()))
              : Optional.empty();
      results.add(this.extremum.newResult(this.name, value));
    }
    return results;
  }
}
