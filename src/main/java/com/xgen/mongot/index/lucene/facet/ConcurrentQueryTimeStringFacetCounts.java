package com.xgen.mongot.index.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.lucene.util.IOUtils;

/**
 * Concurrent variant of {@link QueryTimeStringFacetCounts}.
 *
 * <p>Each segment is counted on the caller-supplied executor. A segment task counts local ordinals
 * into its own map, then resolves them to terms and adds those counts to a shared map. There is no
 * separate merge step.
 */
public final class ConcurrentQueryTimeStringFacetCounts extends Facets {

  private final String dim;
  private final String readField;
  private final ConcurrentHashMap<BytesRef, AtomicInteger> termCounts = new ConcurrentHashMap<>();

  private ConcurrentQueryTimeStringFacetCounts(
      String dim, String readField, FacetsCollector hits, ExecutorService executor)
      throws IOException, InterruptedException {
    this.dim = dim;
    this.readField = readField;
    count(hits, executor);
  }

  /**
   * Query-time counter for a token facet field, counted concurrently across segments.
   *
   * @param luceneField dedicated Lucene field whose sorted-set terms are the facet values
   * @param hits matching documents from the preceding search
   * @param executor executor used to count segments in parallel
   */
  public static ConcurrentQueryTimeStringFacetCounts forTokenField(
      String luceneField, FacetsCollector hits, ExecutorService executor)
      throws IOException, InterruptedException {
    return new ConcurrentQueryTimeStringFacetCounts(luceneField, luceneField, hits, executor);
  }

  private void count(FacetsCollector hits, ExecutorService executor)
      throws IOException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>();
    for (MatchingDocs matchingDocs : hits.getMatchingDocs()) {
      futures.add(executor.submit(new CountOneSegment(matchingDocs)));
    }
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException executionException) {
        Throwable cause = executionException.getCause();
        throw IOUtils.rethrowAlways(cause != null ? cause : executionException);
      }
    }
  }

  private final class CountOneSegment implements Callable<Void> {
    private final MatchingDocs matchingDocs;

    private CountOneSegment(MatchingDocs matchingDocs) {
      this.matchingDocs = matchingDocs;
    }

    @Override
    public Void call() throws IOException {
      if (this.matchingDocs.totalHits() == 0) {
        return null;
      }
      LeafReader reader = this.matchingDocs.context().reader();
      SortedSetDocValues multiValues =
          DocValues.getSortedSet(reader, ConcurrentQueryTimeStringFacetCounts.this.readField);
      if (multiValues == null) {
        return null;
      }

      SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
      DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;
      DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(
              Arrays.asList(this.matchingDocs.bits().iterator(), valuesIt));

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
        ConcurrentQueryTimeStringFacetCounts.this
            .termCounts
            .computeIfAbsent(term, ignored -> new AtomicInteger())
            .addAndGet(count);
      }
      return null;
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    validateDim(dim);

    List<Map.Entry<BytesRef, Integer>> sorted = snapshotSortedByCount();
    int resultCount = Math.min(topN, sorted.size());
    LabelAndValue[] labelValues = new LabelAndValue[resultCount];
    for (int i = 0; i < resultCount; i++) {
      Map.Entry<BytesRef, Integer> entry = sorted.get(i);
      labelValues[i] = new LabelAndValue(entry.getKey().utf8ToString(), entry.getValue());
    }
    return new FacetResult(this.dim, new String[0], totalCount(), labelValues, sorted.size());
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    validateDim(dim);

    List<BytesRef> labels = new ArrayList<>(this.termCounts.keySet());
    labels.sort(Comparator.naturalOrder());

    LabelAndValue[] labelValues = new LabelAndValue[labels.size()];
    for (int i = 0; i < labels.size(); i++) {
      BytesRef label = labels.get(i);
      labelValues[i] = new LabelAndValue(label.utf8ToString(), this.termCounts.get(label).get());
    }
    return new FacetResult(
        this.dim, new String[0], totalCount(), labelValues, this.termCounts.size());
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    AtomicInteger counter = this.termCounts.get(new BytesRef(path[0]));
    return counter == null ? 0 : counter.get();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return List.of(getTopChildren(topN, this.dim));
  }

  private List<Map.Entry<BytesRef, Integer>> snapshotSortedByCount() {
    List<Map.Entry<BytesRef, Integer>> sorted = new ArrayList<>(this.termCounts.size());
    for (Map.Entry<BytesRef, AtomicInteger> entry : this.termCounts.entrySet()) {
      sorted.add(Map.entry(entry.getKey(), entry.getValue().get()));
    }
    // Tie-break by Lucene ordinal order (unsigned UTF-8), not Java String order.
    sorted.sort(
        Comparator.comparingInt((Map.Entry<BytesRef, Integer> entry) -> entry.getValue())
            .reversed()
            .thenComparing(Map.Entry::getKey));
    return sorted;
  }

  private int totalCount() {
    return this.termCounts.values().stream().mapToInt(AtomicInteger::get).sum();
  }

  private void validateDim(String queriedDim) {
    if (!queriedDim.equals(this.dim)) {
      throw new IllegalArgumentException(
          "invalid dim \"" + queriedDim + "\"; should be \"" + this.dim + "\"");
    }
  }
}
