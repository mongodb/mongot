package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/** Lucene leaf accumulator for the {@code avg} aggregation operator. */
final class AvgAccumulator extends Accumulator {
  private final ResolvedNumericMetricField metricField;
  private final DoubleArray sums;
  private final LongArray counts;

  AvgAccumulator(String name, ResolvedNumericMetricField metricField, AggregationContext context) {
    super(name, List.of(), context);
    this.metricField = metricField;
    this.sums = new DoubleArray(8);
    this.counts = new LongArray(8);
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
        int bucketOrd = Math.toIntExact(owningBucketOrd);
        var representation = AvgAccumulator.this.metricField.representation();
        double value = NumericMetricValues.decode(values.nextValue(), representation);
        AvgAccumulator.this.sums.increment(bucketOrd, value);
        AvgAccumulator.this.counts.increment(bucketOrd, 1L);
      }
    };
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      int bucketOrd = (int) ordsToCollect.get(i);
      double sum = bucketOrd < this.sums.size() ? this.sums.get(bucketOrd) : 0.0;
      long count = bucketOrd < this.counts.size() ? this.counts.get(bucketOrd) : 0L;
      results.add(new AvgAggregationResult(this.name, sum, count));
    }
    return results;
  }
}
