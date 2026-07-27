package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.query.aggregation.Accumulator;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationResultBuilder;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.BucketCollector.BucketCollectorWrapper;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.CollectorManager;

/**
 * Bridges the Accumulator tree to Lucene. {@code newCollector()} builds a new accumulator tree per
 * search slice; {@code reduce()} folds slice results with an {@link AggregationResultBuilder}.
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
    // buildTopLevelResult() throws IOException, so CheckedStream stands in for Stream.map.
    List<AggregationResult> results =
        CheckedStream.from(collectors)
            .mapAndCollectChecked(
                collector ->
                    Check.instanceOf(collector.bucketCollector(), Accumulator.class)
                        .buildTopLevelResult());
    Check.checkState(!results.isEmpty(), "No collectors to reduce");
    // get(0) only supplies the shape: the builder starts empty and every result folds in
    // uniformly. A future spec.newResultBuilder() would remove the need for a representative.
    AggregationResultBuilder builder = results.get(0).toBuilder();
    results.forEach(builder::add);
    return builder.build();
  }
}
