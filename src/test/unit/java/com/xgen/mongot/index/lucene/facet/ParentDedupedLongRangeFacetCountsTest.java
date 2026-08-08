package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

/**
 * Verifies the distinct-parent-per-bucket (and per-root default-bucket) counting semantic of {@link
 * ParentDedupedLongRangeFacetCounts} for embeddedDoc-scoped number/date facets.
 */
public class ParentDedupedLongRangeFacetCountsTest {

  private static final String FACET_FIELD = "price";
  private static final String IN_STOCK_FIELD = "inStock";
  private static final String DOC_TYPE_FIELD = "docType";
  private static final String PARENT_MARKER = "parent";
  private static final String NAME_FIELD = "name";

  // Buckets [0,25), [25,50), [50,100); values outside these fall in the default bucket.
  private static final LongRange[] RANGES =
      new LongRange[] {
        new LongRange("[0,25)", 0, true, 25, false),
        new LongRange("[25,50)", 25, true, 50, false),
        new LongRange("[50,100)", 50, true, 100, false),
      };

  /**
   * Per-root dedup across range buckets, plus the default bucket. P1 has two in-stock variants in
   * [25,50) (30, 40) — counted once there; its out-of-stock 10 is excluded. P2 (20) and P3 (15)
   * each land in [0,25). P4 (150 in-stock) is outside every range → default. Expected: [0,25):2,
   * [25,50):1, [50,100):0, default:1.
   */
  @Test
  public void countsDistinctParentsPerBucket_withDefault() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      writer.addDocuments(
          List.of(variant(30, true), variant(40, true), variant(10, false), product("p1")));
      writer.addDocuments(List.of(variant(20, true), product("p2")));
      writer.addDocuments(List.of(variant(15, true), variant(12, false), product("p3")));
      writer.addDocuments(List.of(variant(150, true), product("p4")));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        var counts = count(searcher, reader, allProductsAsResultParents(searcher));

        assertThat(counts.counts()[0]).isEqualTo(2); // [0,25): p2, p3
        assertThat(counts.counts()[1]).isEqualTo(1); // [25,50): p1 (30 & 40 dedup)
        assertThat(counts.counts()[2]).isEqualTo(0); // [50,100): none
        assertThat(counts.defaultCount()).isEqualTo(1); // p4 (150)
      }
    }
  }

  /**
   * Counting is gated to result parents: an in-stock variant of a root that did not satisfy the
   * main query is excluded. Only product X is a result; Y's in-stock 20 must not be counted.
   */
  @Test
  public void gatesCountsToResultParents() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(variant(20, true), product("x")));
      writer.addDocuments(List.of(variant(20, true), product("y")));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        FacetsCollector resultParents =
            searcher.search(new TermQuery(new Term(NAME_FIELD, "x")), new FacetsCollectorManager());
        var counts = count(searcher, reader, resultParents);

        assertThat(counts.counts()[0]).isEqualTo(1); // [0,25): only X, not Y
        assertThat(counts.defaultCount()).isEqualTo(0);
      }
    }
  }

  /**
   * A matching child that lacks a value for the facet field contributes to the default bucket, as a
   * document-indexed boundary facet would count a missing field. P0 has an in-stock variant with no
   * price (→ default); P1 has an in-stock 20 (→ [0,25)).
   */
  @Test
  public void matchingChildMissingValue_countsInDefaultBucket() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(variantNoPrice(true), product("p0")));
      writer.addDocuments(List.of(variant(20, true), product("p1")));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        var counts = count(searcher, reader, allProductsAsResultParents(searcher));

        assertThat(counts.counts()[0]).isEqualTo(1); // [0,25): p1
        assertThat(counts.defaultCount()).isEqualTo(1); // p0's in-stock variant has no price
      }
    }
  }

  private static ParentDedupedLongRangeFacetCounts count(
      IndexSearcher searcher, IndexReader reader, FacetsCollector resultParents) throws Exception {
    // The facet's embeddedDocument.operator: in-stock variants.
    Query childOperator = new TermQuery(new Term(IN_STOCK_FIELD, "true"));
    FacetsCollector childHits = searcher.search(childOperator, new FacetsCollectorManager());
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)));
    return new ParentDedupedLongRangeFacetCounts(
        FACET_FIELD, RANGES, childHits, parentFilter, resultParents, reader);
  }

  private static Document variant(long price, boolean inStock) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(FACET_FIELD, price));
    doc.add(new StringField(IN_STOCK_FIELD, Boolean.toString(inStock), StringField.Store.NO));
    return doc;
  }

  /**
   * A child matching the operator (has the inStock field) but with no value for the facet field.
   */
  private static Document variantNoPrice(boolean inStock) {
    Document doc = new Document();
    doc.add(new StringField(IN_STOCK_FIELD, Boolean.toString(inStock), StringField.Store.NO));
    return doc;
  }

  private static Document product(String name) {
    Document doc = new Document();
    doc.add(new StringField(DOC_TYPE_FIELD, PARENT_MARKER, StringField.Store.NO));
    doc.add(new StringField(NAME_FIELD, name, StringField.Store.NO));
    return doc;
  }

  private static FacetsCollector allProductsAsResultParents(IndexSearcher searcher)
      throws java.io.IOException {
    return searcher.search(
        new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)), new FacetsCollectorManager());
  }
}
