package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;

public class FacetFeatureExplainer implements FeatureExplainer {
  private final Map<String, Integer> queriedStringFacetCardinalities;
  private final Map<String, Integer> totalStringFacetCardinalities;
  private final ExplainTimings createCountTimings;

  public FacetFeatureExplainer() {
    this.queriedStringFacetCardinalities = new HashMap<>();
    this.totalStringFacetCardinalities = new HashMap<>();
    this.createCountTimings = ExplainTimings.builder().build();
  }

  /**
   * Records string facet cardinalities across the queried set of documents. This method is only to
   * be used in the intermediate facet creation code path.
   */
  public void addIntermediateQueriedStringFacetCardinalities(
      String facetName, List<IntermediateFacetBucket> bucket) {
    this.queriedStringFacetCardinalities.put(facetName, bucket.size());
  }

  /**
   * Records string facet cardinalities across the queried set of documents. This method is only to
   * be used in the non-intermediate facet creation code path.
   */
  public void addNonIntermediateQueriedStringFacetCardinalities(Map<String, FacetInfo> facets) {
    for (Map.Entry<String, FacetInfo> facet : facets.entrySet()) {
      this.queriedStringFacetCardinalities.put(facet.getKey(), facet.getValue().buckets().size());
    }
  }

  /** Records string facet cardinalities across the entire index. */
  public void addTotalStringFacetCardinalities(
      String facetName, Optional<SortedSetDocValuesReaderState.OrdRange> maybeRange) {
    if (maybeRange.isEmpty()) {
      this.totalStringFacetCardinalities.put(facetName, 0);
      return;
    }

    SortedSetDocValuesReaderState.OrdRange range = maybeRange.get();
    // Range is inclusive on both ends
    this.totalStringFacetCardinalities.put(facetName, range.end - range.start + 1);
  }

  /** Records string facet cardinality across the queried set of documents, by count. */
  public void addQueriedStringFacetCardinality(String facetName, int cardinality) {
    this.queriedStringFacetCardinalities.put(facetName, cardinality);
  }

  public ExplainTimings getCreateCountTimings() {
    return this.createCountTimings;
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {

    if (!this.queriedStringFacetCardinalities.isEmpty()) {
      builder.queriedStringFacetCardinalities(this.queriedStringFacetCardinalities);
    }

    if (!this.totalStringFacetCardinalities.isEmpty()) {
      builder.totalStringFacetCardinalities(this.totalStringFacetCardinalities);
    }

    if (verbosity.compareTo(Explain.Verbosity.QUERY_PLANNER) > 0) {
      if (!this.createCountTimings.allTimingDataIsEmpty()) {
        builder.createFacetCountsStats(
            QueryExecutionArea.facetCreateCountAreaFor(
                this.createCountTimings.extractTimingData()));
      }
    }
  }
}
