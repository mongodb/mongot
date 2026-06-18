package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Scorable;

/**
 * A BucketCollector which allows running a bucket collection with several BucketCollectors. It is
 * used by BucketAccumulator to form a tree of accumulators.
 */
public final class MultiBucketCollector extends BucketCollector {
  private final List<? extends BucketCollector> collectors;

  private MultiBucketCollector(List<? extends BucketCollector> collectors) {
    this.collectors = collectors;
  }

  public static MultiBucketCollector of(List<? extends BucketCollector> collectors) {
    return new MultiBucketCollector(collectors);
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    List<LeafBucketCollector> leafCollectors = new ArrayList<>();
    for (BucketCollector collector : this.collectors) {
      leafCollectors.add(collector.getLeafCollector(context));
    }
    return new MultiLeafBucketCollector(leafCollectors);
  }

  private static final class MultiLeafBucketCollector extends LeafBucketCollector {
    private final List<LeafBucketCollector> leafCollectors;

    private MultiLeafBucketCollector(List<LeafBucketCollector> leafCollectors) {
      this.leafCollectors = leafCollectors;
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
      for (LeafBucketCollector leafCollector : this.leafCollectors) {
        leafCollector.collect(doc, owningBucketOrd);
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      for (LeafBucketCollector leafCollector : this.leafCollectors) {
        leafCollector.setScorer(scorer);
      }
    }
  }
}
