package com.xgen.mongot.index.query;

/**
 * Feature gates applied while parsing a {@link SearchQuery}.
 *
 * @param allow10k when true, facet bucket-count bounds are widened to the 10k limit
 * @param searchAggregationEnabled when true (the {@link
 *     com.xgen.mongot.featureflag.Feature#SEARCH_AGGREGATION} flag is on), an aggregation-capable
 *     {@code facet} request (per {@link
 *     com.xgen.mongot.index.query.collectors.AggregationCollector#isAggregationCapable}) is parsed
 *     into an {@link com.xgen.mongot.index.query.collectors.AggregationCollector}; otherwise the
 *     request is always parsed on the legacy {@link
 *     com.xgen.mongot.index.query.collectors.FacetCollector} path
 * @param embeddedScopedSortEnabled whether embedded-document-scoped sort syntax is enabled
 */
public record SearchQueryParseOptions(
    boolean allow10k, boolean searchAggregationEnabled, boolean embeddedScopedSortEnabled) {

  public SearchQueryParseOptions(boolean allow10k, boolean searchAggregationEnabled) {
    this(allow10k, searchAggregationEnabled, false);
  }

  /**
   * Defaults for callers that have no feature-flag registry (tests, benches): 1k facet bounds,
   * aggregation and scoped sorting off. Production {@code search} / {@code planShardedSearch} paths
   * pass explicit options from the registry. The requested-bucket cap is a plan-time DFF, not a
   * parse option.
   */
  public static final SearchQueryParseOptions DEFAULT =
      new SearchQueryParseOptions(/* allow10k= */ false, /* searchAggregationEnabled= */ false);
}
