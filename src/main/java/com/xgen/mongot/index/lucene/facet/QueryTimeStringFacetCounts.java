package com.xgen.mongot.index.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Query-time string facet counting for token fields, used when the matching doc set is sparse
 * relative to the index.
 *
 * <p>Unlike {@link SortedSetDocValuesFacetCounts}, this class does not use a {@link
 * org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState} and never builds a global {@link
 * org.apache.lucene.index.OrdinalMap}. For each matching segment it counts per-segment ordinals of
 * the matching documents, resolves those ordinals with {@code lookupOrd}, and merges counts by term
 * string. The same value at different per-segment ordinals therefore combines without a global map.
 * Equal counts are ordered by Lucene ordinal (unsigned UTF-8 bytes), matching {@link
 * SortedSetDocValuesFacetCounts}.
 *
 * <p>Token fields use one Lucene field per facet, and the sorted-set terms are the facet values. A
 * segment with no sorted-set values for the field contributes nothing.
 */
public final class QueryTimeStringFacetCounts extends Facets {

  private final String dim;
  private final String readField;
  private final Map<BytesRef, Long> termCounts = new HashMap<>();

  private QueryTimeStringFacetCounts(String dim, String readField, FacetsCollector hits)
      throws IOException {
    this.dim = dim;
    this.readField = readField;
    count(hits);
  }

  /**
   * Query-time counter for a token facet field.
   *
   * @param luceneField dedicated Lucene field whose sorted-set terms are the facet values
   */
  public static QueryTimeStringFacetCounts forTokenField(String luceneField, FacetsCollector hits)
      throws IOException {
    return new QueryTimeStringFacetCounts(luceneField, luceneField, hits);
  }

  private void count(FacetsCollector hits) throws IOException {
    for (MatchingDocs matchingDocs : hits.getMatchingDocs()) {
      countOneSegment(matchingDocs);
    }
  }

  private void countOneSegment(MatchingDocs matchingDocs) throws IOException {
    if (matchingDocs.totalHits() == 0) {
      return;
    }
    LeafReader reader = matchingDocs.context().reader();
    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, this.readField);
    if (multiValues == null) {
      return;
    }

    // Counting in segment-ordinal space is cheaper against SortedDocValues when the field is
    // single-valued (LUCENE-5309).
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;
    DocIdSetIterator it =
        ConjunctionUtils.intersectIterators(
            Arrays.asList(matchingDocs.bits().iterator(), valuesIt));

    IntIntHashMap segCounts = new IntIntHashMap();
    if (singleValues != null) {
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        segCounts.addTo(singleValues.ordValue(), 1);
      }
    } else {
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        for (int i = 0; i < multiValues.docValueCount(); i++) {
          segCounts.addTo((int) multiValues.nextOrd(), 1);
        }
      }
    }

    for (IntCursor cursor : segCounts.keys()) {
      int segOrd = cursor.value;
      int count = segCounts.get(segOrd);
      if (count == 0) {
        continue;
      }
      // lookupOrd reuses its BytesRef. Copy before using it as a map key.
      BytesRef term = BytesRef.deepCopyOf(multiValues.lookupOrd(segOrd));
      this.termCounts.merge(term, (long) count, Long::sum);
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    validateDim(dim);

    List<Map.Entry<BytesRef, Long>> sorted = new ArrayList<>(this.termCounts.entrySet());
    // Tie-break by Lucene ordinal order (unsigned UTF-8), not Java String order.
    sorted.sort(
        Comparator.comparingLong((Map.Entry<BytesRef, Long> entry) -> entry.getValue())
            .reversed()
            .thenComparing(Map.Entry::getKey));

    int resultCount = Math.min(topN, sorted.size());
    LabelAndValue[] labelValues = new LabelAndValue[resultCount];
    for (int i = 0; i < resultCount; i++) {
      Map.Entry<BytesRef, Long> entry = sorted.get(i);
      labelValues[i] = new LabelAndValue(entry.getKey().utf8ToString(), entry.getValue());
    }
    return new FacetResult(
        this.dim, new String[0], totalCount(), labelValues, this.termCounts.size());
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    validateDim(dim);

    List<Map.Entry<BytesRef, Long>> sorted = new ArrayList<>(this.termCounts.entrySet());
    sorted.sort(Map.Entry.comparingByKey());

    LabelAndValue[] labelValues = new LabelAndValue[sorted.size()];
    for (int i = 0; i < sorted.size(); i++) {
      Map.Entry<BytesRef, Long> entry = sorted.get(i);
      labelValues[i] = new LabelAndValue(entry.getKey().utf8ToString(), entry.getValue());
    }
    return new FacetResult(
        this.dim, new String[0], totalCount(), labelValues, this.termCounts.size());
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    return this.termCounts.getOrDefault(new BytesRef(path[0]), 0L);
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return List.of(getTopChildren(topN, this.dim));
  }

  private long totalCount() {
    return this.termCounts.values().stream().mapToLong(Long::longValue).sum();
  }

  private void validateDim(String queriedDim) {
    if (!queriedDim.equals(this.dim)) {
      throw new IllegalArgumentException(
          "invalid dim \"" + queriedDim + "\"; should be \"" + this.dim + "\"");
    }
  }
}
