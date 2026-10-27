package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/** Lucene leaf accumulator for the {@code avg} aggregation operator. */
final class AvgAccumulator extends Accumulator {
  private final ResolvedNumericMetricField metricField;
  private final BucketDoubleStore sums;
  private final BucketLongStore counts;

  AvgAccumulator(String name, ResolvedNumericMetricField metricField, AggregationContext context) {
    super(name, List.of(), context);
    this.metricField = metricField;
    this.sums =
        new BucketDoubleStore(context.getHeapAccountant()::add, this.context::recordDenseSpill);
    this.counts =
        new BucketLongStore(context.getHeapAccountant()::add, this.context::recordDenseSpill);
    declareMetric(Optional.of(metricField.luceneFieldName()));
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    SortedNumericDocValues values =
        DocValues.getSortedNumeric(
            context.getLeafReaderContext().reader(), this.metricField.luceneFieldName());
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        maybeRecordCollectInvocation();
        if (context.isMultiValued(doc)) {
          // Ancestor group field had multiple values for this doc.
          return;
        }
        // Metric field is multi-valued or missing: only single-valued docs contribute.
        if (!values.advanceExact(doc) || values.docValueCount() != 1) {
          return;
        }
        var representation = AvgAccumulator.this.metricField.representation();
        double value = NumericMetricValues.decode(values.nextValue(), representation);
        AvgAccumulator.this.sums.add(owningBucketOrd, value);
        AvgAccumulator.this.counts.add(owningBucketOrd, 1L);
      }
    };
  }

  @Override
  protected void releaseState() {
    AutoCloseables.closeAll(List.of(this.sums, this.counts));
  }

  @Override
  public boolean supportsStateMerge() {
    // Sums and counts add by the parent's ordinal. Same bucket, no term lookup.
    return true;
  }

  @Override
  public void mergeStateFrom(Accumulator other) {
    AvgAccumulator that = Check.instanceOf(other, AvgAccumulator.class);
    this.sums.addAll(that.sums);
    this.counts.addAll(that.counts);
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      long bucketOrd = ordsToCollect.get(i);
      double sum = this.sums.get(bucketOrd, 0.0);
      long count = this.counts.get(bucketOrd, 0L);
      results.add(new AvgAggregationResult(this.name, sum, count));
    }
    return results;
  }
}
