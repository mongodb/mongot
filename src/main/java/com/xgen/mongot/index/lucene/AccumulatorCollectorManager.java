package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.aggregation.Accumulator;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.BucketCollector.BucketCollectorWrapper;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import org.apache.lucene.search.CollectorManager;

/**
 * Bridges the Accumulator tree to Lucene. {@code newCollector()} builds a new accumulator tree per
 * search slice; {@code reduce()} folds slice results with {@link
 * AggregationResult#mergePartitionResult}.
 */
public class AccumulatorCollectorManager
    implements CollectorManager<BucketCollectorWrapper, AggregationResult> {

  private final AggregationSpec rootSpec;
  private final AggregationContext context;

  public AccumulatorCollectorManager(AggregationSpec rootSpec, AggregationContext context) {
    this.rootSpec = rootSpec;
    this.context = context;
  }

  @Override
  public BucketCollectorWrapper newCollector() throws IOException {
    Accumulator accumulator = this.rootSpec.buildAccumulator(this.context);
    return accumulator.asCollector(this.context);
  }

  @Override
  public AggregationResult reduce(Collection<BucketCollectorWrapper> collectors)
      throws IOException {
    @Var Optional<AggregationResult> merged = Optional.empty();

    for (BucketCollectorWrapper collector : collectors) {
      Accumulator accumulator = Check.instanceOf(collector.bucketCollector(), Accumulator.class);
      AggregationResult result = accumulator.buildTopLevelResult();
      merged = Optional.of(merged.map(r -> r.mergePartitionResult(result)).orElse(result));
    }

    if (merged.isEmpty()) {
      throw new IllegalStateException("No collectors to reduce");
    }
    return merged.get();
  }
}
