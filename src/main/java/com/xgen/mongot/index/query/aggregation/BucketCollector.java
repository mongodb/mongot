package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

/** A bucket oriented collector. It bridges the Lucene Collector and the Accumulator. */
public abstract class BucketCollector {
  /** Get the leaf collector for a segment. */
  public abstract LeafBucketCollector getLeafCollector(AggregationContext context)
      throws IOException;

  /** Return the Lucene Collector used by Lucene query machinery. */
  public final BucketCollectorWrapper asCollector(AggregationContext context) throws IOException {
    return new BucketCollectorWrapper(this, context);
  }

  // Lucene compatible Collector implementation.
  public record BucketCollectorWrapper(BucketCollector bucketCollector, AggregationContext context)
      implements Collector {
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return this.bucketCollector.getLeafCollector(this.context.withLeafReaderContext(context));
    }

    @Override
    public ScoreMode scoreMode() {
      // Aggregation doesn't care about scores. We should revisit if we explore within-bucket
      // ordering based on score.
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }
}
