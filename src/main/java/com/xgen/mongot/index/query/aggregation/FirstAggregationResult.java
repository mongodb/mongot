package com.xgen.mongot.index.query.aggregation;

import java.util.Comparator;
import java.util.Optional;
import org.apache.lucene.search.Sort;

/**
 * Production leaf result for the {@code first} aggregation operator.
 *
 * <p>Merging takes the top document under the query's own sort, and prefers the earliest document
 * when that sort leaves candidates tied.
 */
public final class FirstAggregationResult extends FirstLastAggregationResultBase {

  FirstAggregationResult(String name, Optional<Sort> collectorSort, Optional<Candidate> candidate) {
    super(name, collectorSort, candidate);
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new FirstBuilder(getName(), collectorSort());
  }

  private static final class FirstBuilder extends FirstLastAggregationResultBase.Builder {
    private final String name;

    FirstBuilder(String name, Optional<Sort> collectorSort) {
      super(collectorSort);
      this.name = name;
    }

    @Override
    protected Comparator<Candidate> encounterOrder() {
      return ascendingEncounterOrder();
    }

    @Override
    protected AggregationResult create(Optional<Sort> collectorSort, Optional<Candidate> winner) {
      return new FirstAggregationResult(this.name, collectorSort, winner);
    }
  }
}
