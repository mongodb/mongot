package com.xgen.mongot.index.lucene.util;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Helper to build top-level (cross-segment) {@link SortedSetDocValues} for a lucene field. Lives in
 * a low-level package so both string faceting and aggregation can share it without depending on the
 * facet package. Logic forked from {@link org.apache.lucene.facet.StringDocValuesReaderState}.
 */
public final class TopLevelDocValues {
  private TopLevelDocValues() {}

  /**
   * Build top-level (cross-segment) doc values for a lucene field. If an {@link OrdinalMap} is
   * supplied it is reused; otherwise the global map is constructed from scratch.
   */
  public static SortedSetDocValues getDocValues(
      IndexReader reader, String luceneFieldName, Optional<OrdinalMap> ordinalMap)
      throws IOException {
    List<LeafReaderContext> leaves = reader.leaves();
    int leafCount = leaves.size();
    if (leafCount == 0) {
      return DocValues.emptySortedSet();
    }

    if (leafCount == 1) {
      return DocValues.getSortedSet(leaves.getFirst().reader(), luceneFieldName);
    }
    SortedSetDocValues[] docValues = new SortedSetDocValues[leafCount];
    int[] starts = new int[leafCount + 1];
    @Var long cost = 0;
    for (int i = 0; i < leafCount; i++) {
      LeafReaderContext context = reader.leaves().get(i);
      docValues[i] = DocValues.getSortedSet(context.reader(), luceneFieldName);
      starts[i] = context.docBase;
      cost += docValues[i].cost();
    }
    starts[leafCount] = reader.maxDoc();
    IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
    IndexReader.CacheKey owner = cacheHelper == null ? null : cacheHelper.getKey();
    OrdinalMap map =
        ordinalMap.isPresent()
            ? ordinalMap.get()
            : OrdinalMap.build(owner, docValues, PackedInts.DEFAULT);

    return new MultiDocValues.MultiSortedSetDocValues(docValues, starts, map, cost);
  }
}
