package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/** Lucene leaf accumulator for the {@code sum} aggregation operator. */
final class SumAccumulator extends Accumulator {
  private final ResolvedNumericMetricField metricField;
  private final DoubleArray sums;

  SumAccumulator(String name, ResolvedNumericMetricField metricField, AggregationContext context) {
    super(name, List.of(), context);
    this.metricField = metricField;
    this.sums = new DoubleArray(8);
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    SortedNumericDocValues values =
        DocValues.getSortedNumeric(
            context.getLeafReaderContext().reader(), this.metricField.luceneFieldName());
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        if (context.isMultiValued(doc)) {
          // Ancestor group field had multiple values for this doc.
          return;
        }
        // Metric field is multi-valued or missing: only single-valued docs contribute.
        if (!values.advanceExact(doc) || values.docValueCount() != 1) {
          return;
        }
        int bucketOrd = Math.toIntExact(owningBucketOrd);
        var representation = SumAccumulator.this.metricField.representation();
        SumAccumulator.this.sums.increment(
            bucketOrd, NumericMetricValues.decode(values.nextValue(), representation));
      }
    };
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      int bucketOrd = (int) ordsToCollect.get(i);
      double sum = bucketOrd < this.sums.size() ? this.sums.get(bucketOrd) : 0.0;
      results.add(new SumAggregationResult(this.name, sum));
    }
    return results;
  }
}
