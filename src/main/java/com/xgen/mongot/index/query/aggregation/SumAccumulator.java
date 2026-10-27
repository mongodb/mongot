package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/** Lucene leaf accumulator for the {@code sum} aggregation operator. */
final class SumAccumulator extends Accumulator {
  private final ResolvedNumericMetricField metricField;
  private final BucketDoubleStore sums;

  SumAccumulator(String name, ResolvedNumericMetricField metricField, AggregationContext context) {
    super(name, List.of(), context);
    this.metricField = metricField;
    this.sums =
        new BucketDoubleStore(context.getHeapAccountant()::add, this.context::recordDenseSpill);
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
        var representation = SumAccumulator.this.metricField.representation();
        SumAccumulator.this.sums.add(
            owningBucketOrd, NumericMetricValues.decode(values.nextValue(), representation));
      }
    };
  }

  @Override
  protected void releaseState() {
    this.sums.close();
  }

  @Override
  public boolean supportsStateMerge() {
    return true;
  }

  @Override
  public void mergeStateFrom(Accumulator other) {
    this.sums.addAll(Check.instanceOf(other, SumAccumulator.class).sums);
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      long bucketOrd = ordsToCollect.get(i);
      double sum = this.sums.get(bucketOrd, 0.0);
      results.add(new SumAggregationResult(this.name, sum));
    }
    return results;
  }
}
