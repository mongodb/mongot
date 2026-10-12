package com.xgen.mongot.index.lucene.facet;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Decides when a token facet should count with query-time ordinals instead of the cached global
 * ordinal map, and builds that counter.
 */
public final class TokenFacetSparseRouting {

  /**
   * Match Lucene's per-segment sparse/dense split in {@link SortedSetDocValuesFacetCounts}: a
   * result set of at most 10% of the corpus is sparse. Inclusive, so a result set of exactly 10%
   * still takes the query-time path.
   *
   * <p>Fixed to that split rather than a config knob, so this route and the dense counter stay on
   * the same hit ratio.
   */
  static final int SPARSE_HIT_RATIO_DENOMINATOR = 10;

  private TokenFacetSparseRouting() {}

  /**
   * Returns whether {@code totalHits} is small enough, and {@code luceneField} is large enough, to
   * count without a global ordinal map.
   *
   * <p>The hit-ratio half matches the sparse count storage in {@link
   * SortedSetDocValuesFacetCounts}. The cardinality half uses the same {@link
   * SortedSetDocValuesFacetCounts#DENSE_CARDINALITY_LIMIT}: below it the map is cheap and the dense
   * counter is used. Cardinality is the sum of per-segment value counts, which does not build the
   * global map. Overlapping terms are counted once per segment, so a field can take this path when
   * its true global cardinality is still under the limit.
   */
  public static boolean isSparse(long totalHits, int maxDoc, IndexReader reader, String luceneField)
      throws IOException {
    return isHitRatioSparse(totalHits, maxDoc) && exceedsDenseCardinalityLimit(reader, luceneField);
  }

  /**
   * Returns whether {@code totalHits} is at most one tenth of {@code maxDoc}. An empty index is
   * sparse. {@code totalHits} is the exact hit count from the facet collector.
   */
  static boolean isHitRatioSparse(long totalHits, int maxDoc) {
    return maxDoc == 0 || totalHits * SPARSE_HIT_RATIO_DENOMINATOR <= maxDoc;
  }

  /**
   * Counts {@code luceneField} over {@code hits} using per-segment ordinals. When {@code executor}
   * is present, segments are counted on that executor.
   */
  public static Facets count(
      String luceneField, FacetsCollector hits, Optional<? extends ExecutorService> executor)
      throws IOException, InterruptedException {
    if (executor.isPresent()) {
      return ConcurrentQueryTimeStringFacetCounts.forTokenField(luceneField, hits, executor.get());
    }
    return QueryTimeStringFacetCounts.forTokenField(luceneField, hits);
  }

  /**
   * Sums per-segment sorted-set value counts until they reach {@link
   * SortedSetDocValuesFacetCounts#DENSE_CARDINALITY_LIMIT}. Does not build a global ordinal map.
   */
  private static boolean exceedsDenseCardinalityLimit(IndexReader reader, String luceneField)
      throws IOException {
    @Var long total = 0;
    for (var leaf : reader.leaves()) {
      SortedSetDocValues values = DocValues.getSortedSet(leaf.reader(), luceneField);
      if (values == null) {
        continue;
      }
      total += values.getValueCount();
      if (total >= SortedSetDocValuesFacetCounts.DENSE_CARDINALITY_LIMIT) {
        return true;
      }
    }
    return false;
  }
}
