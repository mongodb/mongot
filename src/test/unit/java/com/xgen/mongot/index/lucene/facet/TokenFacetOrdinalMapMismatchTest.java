package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import com.google.errorprone.annotations.Var;
import com.xgen.testing.LuceneIndexRule;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Minimal Lucene-only repros for token field faceting issues.
 *
 * <p>The repros in this class include:
 *
 * <ul>
 *   <li>Builds an index where the facet counting code selects the <b>dense</b> counting strategy
 *       and assertions pass.
 *   <li>Then mutates the same index so the facet counting code switches to the <b>sparse</b>
 *       counting strategy, where incorrect ordinal iteration can emit duplicate ordinals or
 *       non-keys, leading to duplicated facet buckets and/or inflated totals ({@link
 *       #testDenseThenSparseFlipDoesNotDuplicateBuckets()}).
 *   <li>Separately verifies that the sparse ordinal iterator correctly includes {@code ord=0},
 *       which is a valid facet ordinal and must not be skipped or mishandled ({@link
 *       #testSparseIteratorHandlesZeroOrdinalCorrectly()}).
 * </ul>
 */
public class TokenFacetOrdinalMapMismatchTest {
  private static final String GROUP_FIELD = "group";
  private static final String FACET_FIELD = "foo";

  @Test
  public void testDenseThenSparseFlipDoesNotDuplicateBuckets() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, newWriterConfig())) {
      // Step 1: Create a non-sparse scenario for the query (dense counting expected).
      //
      // totalDocs = 5000, totalHits = 1000 => totalHits >= totalDocs/10 (1000 >= 500) => dense
      // cardinality = 2000 (> 1024) => we are in the high-cardinality branch, but still dense
      seedHighCardinalityWithManyHits(writer, 5000, 1000, 2000);
      writer.forceMerge(1);

      // Step 2: Run assertions and show they pass in dense mode.
      try (IndexReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        var dense = runFacet(searcher, 1000);
        assertThat(dense.hits()).isEqualTo(1000);
        assertThat(dense.result().childCount).isEqualTo(1);
        assertThat(dense.result().value.intValue()).isEqualTo(1000);
        assertThat(dense.result().labelValues).hasLength(1);
        assertThat(dense.result().labelValues[0].label).isEqualTo("00000");
        assertThat(dense.result().labelValues[0].value.intValue()).isEqualTo(1000);
      }

      // Step 3: Add docs that make this query sparse.
      // totalDocs grows but totalHits stays at 1000.
      // After adding 10000 docs: totalDocs = 15000, totalHits = 1000 => totalHits < totalDocs/10
      // (1000 < 1500) => sparse
      seedAdditionalMissDocs(writer, 10000, 2000);

      // Step 4: Force merge, reopen reader, and observe the flip to sparse behavior.
      writer.forceMerge(1);

      // Step 5: In sparse mode, results must match dense mode (no duplicate buckets).
      try (IndexReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        var sparse = runFacet(searcher, 1000);
        assertThat(sparse.hits()).isEqualTo(1000);

        // Sparse iteration must not emit duplicate labels.
        var labels = Arrays.stream(sparse.result().labelValues).map(lv -> lv.label).toList();
        assertThat(new HashSet<>(labels).size()).isEqualTo(labels.size());

        // Expected behavior: a single bucket with the same label/value as dense mode.
        assertThat(sparse.result().childCount).isEqualTo(1);
        assertThat(sparse.result().labelValues).hasLength(1);
        assertThat(sparse.result().labelValues[0].label).isEqualTo("00000");
        assertThat(sparse.result().labelValues[0].value.intValue()).isEqualTo(1000);
      }
    }
  }

  @Test
  public void testSparseIteratorHandlesZeroOrdinalCorrectly() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, newWriterConfig())) {

      // Step 1: Create sparse scenario
      // totalDocs = 2000, totalHits = 100 (< 200) -> sparse
      // Include ord=0 (facet "00000") as a hit
      for (int i = 0; i < 100; i++) {
        writer.addDocument(doc("hit", "00000")); // ord=0
      }
      // add 1900 "miss" docs with different facet values to ensure cardinality > 1024
      for (int i = 0; i < 1900; i++) {
        writer.addDocument(doc("miss", String.format("%05d", i + 1)));
      }
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Step 2: Collect hits for the "hit" term
        var fcm = new FacetsCollectorManager();
        var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
        var result = FacetsCollectorManager.search(searcher, query, 1000, fcm);
        var topDocs = result.topDocs();
        var collector = result.facetsCollector();
        assertThat(topDocs.totalHits.value()).isEqualTo(100);

        // Step 3: Run facets
        var state =
            TokenSsdvFacetState.create(searcher.getIndexReader(), FACET_FIELD, Optional.empty())
                .get();
        var counts = new SortedSetDocValuesFacetCounts(state, collector);

        // Step 4: Verify top facet counts
        var facets = counts.getTopChildren(10, FACET_FIELD);
        assertThat(facets.childCount).isEqualTo(1);
        assertThat(facets.labelValues).hasLength(1);
        assertThat(facets.labelValues[0].label).isEqualTo("00000");
        assertThat(facets.labelValues[0].value.intValue()).isEqualTo(100);

        // Step 5: Verify sparse iterator correctly returns ord=0
        var iter = counts.getOrdIterator();
        @Var boolean sawZero = false;
        while (iter.hasNext()) {
          int ord = iter.nextInt();
          assertThat(ord).isAtLeast(0);
          assertThat(ord).isLessThan(2000);
          if (ord == 0) {
            sawZero = true;
          }
        }
        assertThat(sawZero).isTrue();
      }
    }
  }

  private static IndexWriterConfig newWriterConfig() {
    return LuceneIndexRule.getIndexWriterConfig();
  }

  private static Document doc(String group, String facetValue) {
    Document doc = new Document();
    doc.add(new StringField(GROUP_FIELD, group, Field.Store.NO));
    doc.add(new SortedSetDocValuesField(FACET_FIELD, new BytesRef(facetValue)));
    return doc;
  }

  private record FacetingOutput(long hits, FacetResult result) {}

  private static FacetingOutput runFacet(IndexSearcher searcher, int topN) throws Exception {
    var fcm = new FacetsCollectorManager();
    var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
    var result = FacetsCollectorManager.search(searcher, query, topN, fcm);
    var topDocs = result.topDocs();
    var collector = result.facetsCollector();
    var state =
        TokenSsdvFacetState.create(searcher.getIndexReader(), FACET_FIELD, Optional.empty());
    assertThat(state).isPresent();

    var facets = new SortedSetDocValuesFacetCounts(state.get(), collector);
    return new FacetingOutput(topDocs.totalHits.value(), facets.getTopChildren(10, FACET_FIELD));
  }

  private static void seedHighCardinalityWithManyHits(
      IndexWriter writer, int totalDocs, int hitDocs, int cardinality) throws Exception {
    // All hits use the smallest lexicographic facet value -> ord=0.
    for (int i = 0; i < hitDocs; i++) {
      writer.addDocument(doc("hit", "00000"));
    }

    for (int i = hitDocs; i < totalDocs; i++) {
      // High overall cardinality ensured by misses having many distinct values.
      String facetValue = String.format("%05d", i % cardinality);
      writer.addDocument(doc("miss", facetValue));
    }
    writer.commit();
  }

  private static void seedAdditionalMissDocs(IndexWriter writer, int docsToAdd, int cardinality)
      throws Exception {
    for (int i = 0; i < docsToAdd; i++) {
      String facetValue = String.format("%05d", (i + 12345) % cardinality);
      writer.addDocument(doc("miss", facetValue));
    }
    writer.commit();
  }
}
