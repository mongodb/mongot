package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.util.Check;
import java.util.Map;
import java.util.Optional;

public class MetaResultsBuilder {
  private Optional<CountResult> count = Optional.empty();
  private Optional<Map<String, FacetInfo>> facet = Optional.empty();

  public static MetaResultsBuilder builder() {
    return new MetaResultsBuilder();
  }

  public MetaResultsBuilder count(CountResult count) {
    this.count = Optional.of(count);
    return this;
  }

  public MetaResultsBuilder facet(Map<String, FacetInfo> facet) {
    this.facet = Optional.of(facet);
    return this;
  }

  /** Builds MetaResults from an MetaResultsBuilder. */
  public MetaResults build() {
    Check.isPresent(this.count, "count");

    return new MetaResults(this.count.get(), this.facet, Optional.empty());
  }

  /** Create a MetaResults with the given lowerBound count. */
  public static MetaResults mockLowerBoundMetaResults(long lowerBound) {
    return MetaResultsBuilder.builder().count(CountResult.lowerBoundCount(lowerBound)).build();
  }
}
