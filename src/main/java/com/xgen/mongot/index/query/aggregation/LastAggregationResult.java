package com.xgen.mongot.index.query.aggregation;

import java.util.Comparator;
import java.util.Optional;
import org.apache.lucene.search.Sort;

/**
 * Production leaf result for the {@code last} aggregation operator.
 *
 * <p>Merging takes the top document under the inverted sort the accumulator collected with -- the
 * bottom document under the query's sort -- and prefers the latest document when that leaves
 * candidates tied.
 */
public final class LastAggregationResult extends FirstLastAggregationResultBase {

  LastAggregationResult(String name, Optional<Sort> collectorSort, Optional<Candidate> candidate) {
    super(name, collectorSort, candidate);
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new LastBuilder(getName(), collectorSort());
  }

  private static final class LastBuilder extends FirstLastAggregationResultBase.Builder {
    private final String name;

    LastBuilder(String name, Optional<Sort> collectorSort) {
      super(collectorSort);
      this.name = name;
    }

    @Override
    protected Comparator<Candidate> encounterOrder() {
      return ascendingEncounterOrder().reversed();
    }

    @Override
    protected AggregationResult create(Optional<Sort> collectorSort, Optional<Candidate> winner) {
      return new LastAggregationResult(this.name, collectorSort, winner);
    }
  }
}
