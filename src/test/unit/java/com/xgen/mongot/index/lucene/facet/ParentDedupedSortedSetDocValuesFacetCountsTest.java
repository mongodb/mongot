package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Verifies the distinct-parent-per-bucket counting semantic of {@link
 * ParentDedupedSortedSetDocValuesFacetCounts} against the canonical "customer F" fixture from the
 * embeddedDoc-scoped facet QA plan.
 */
public class ParentDedupedSortedSetDocValuesFacetCountsTest {

  private static final String FACET_FIELD = "branchCRD";
  private static final String CRD_TERM_FIELD = "crd";
  private static final String DOC_TYPE_FIELD = "docType";
  private static final String PARENT_MARKER = "parent";
  private static final String NAME_FIELD = "name";

  /**
   * Canonical fixture: employee X = [{123, 2025}, {123, 2031}, {000, 2020}], employee Y = [{123,
   * 2021}, {456, 2028}]. The child operator matches branches with branchCRD in {123, 456}. Faceting
   * on branchCRD, each employee counted at most once per bucket, must yield 123: 2, 456: 1 (X's two
   * matching 123 branches dedup to one; the non-matching 000 branch is excluded).
   */
  @Test
  public void customerFFixture_countsDistinctParentsPerBucket() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Each employee is one block: its branch (child) docs followed by the employee (parent) doc.
      writer.addDocuments(
          List.of(branchDoc("123"), branchDoc("123"), branchDoc("000"), employeeDoc()));
      writer.addDocuments(List.of(branchDoc("123"), branchDoc("456"), employeeDoc()));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);

        // The facet's embeddedDocument.operator: branches with branchCRD in {123, 456}.
        Query childOperator =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "123")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "456")), BooleanClause.Occur.SHOULD)
                .build();
        FacetsCollector childHits = searcher.search(childOperator, new FacetsCollectorManager());

        BitSetProducer parentFilter =
            new QueryBitSetProducer(new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)));

        var state = TokenSsdvFacetState.create(reader, FACET_FIELD, Optional.empty()).orElseThrow();
        var counts =
            new ParentDedupedSortedSetDocValuesFacetCounts(
                state, childHits, parentFilter, allEmployeesAsResultParents(searcher));

        FacetResult result = counts.getAllChildren(FACET_FIELD);

        Map<String, Long> byLabel = new HashMap<>();
        for (LabelAndValue lv : result.labelValues) {
          byLabel.put(lv.label, lv.value.longValue());
        }
        assertThat(byLabel).containsEntry("123", 2L);
        assertThat(byLabel).containsEntry("456", 1L);
        // The non-matching 000 branch is excluded by the child operator, so it forms no bucket.
        assertThat(byLabel).doesNotContainKey("000");
      }
    }
  }

  /**
   * A parent with matching children in two buckets is counted in both; a parent whose only child is
   * non-matching contributes to no bucket at all.
   */
  @Test
  public void parentInMultipleBuckets_andParentWithNoMatchingChild() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Employee A: matching children in two buckets (123 and 456) -> counted in both.
      writer.addDocuments(List.of(branchDoc("123"), branchDoc("456"), employeeDoc()));
      // Employee B: only a non-matching child (000) -> contributes to no bucket.
      writer.addDocuments(List.of(branchDoc("000"), employeeDoc()));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        Query childOperator =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "123")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "456")), BooleanClause.Occur.SHOULD)
                .build();
        FacetsCollector childHits = searcher.search(childOperator, new FacetsCollectorManager());
        BitSetProducer parentFilter =
            new QueryBitSetProducer(new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)));
        var state = TokenSsdvFacetState.create(reader, FACET_FIELD, Optional.empty()).orElseThrow();
        var counts =
            new ParentDedupedSortedSetDocValuesFacetCounts(
                state, childHits, parentFilter, allEmployeesAsResultParents(searcher));

        Map<String, Long> byLabel = new HashMap<>();
        for (LabelAndValue lv : counts.getAllChildren(FACET_FIELD).labelValues) {
          byLabel.put(lv.label, lv.value.longValue());
        }
        assertThat(byLabel).containsEntry("123", 1L);
        assertThat(byLabel).containsEntry("456", 1L);
        assertThat(byLabel).doesNotContainKey("000");
      }
    }
  }

  /**
   * Counting is gated to result parents: children of roots that did not satisfy the main query are
   * excluded, even when they match the facet's child operator. Without gating this would report
   * {@code 123: 2} (X and Y); with gating it reports {@code 123: 1} because only X is a result.
   */
  @Test
  public void gatesCountsToResultParents() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Employee X (a result) and employee Y (NOT a result) both have an in-scope 123 branch.
      writer.addDocuments(List.of(branchDoc("123"), branchDoc("456"), employeeDoc("x")));
      writer.addDocuments(List.of(branchDoc("123"), employeeDoc("y")));
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);

        Query childOperator =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "123")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(CRD_TERM_FIELD, "456")), BooleanClause.Occur.SHOULD)
                .build();
        FacetsCollector childHits = searcher.search(childOperator, new FacetsCollectorManager());
        BitSetProducer parentFilter =
            new QueryBitSetProducer(new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)));

        // Main query returns only employee X, so Y's matching children must not be counted.
        FacetsCollector resultParents =
            searcher.search(new TermQuery(new Term(NAME_FIELD, "x")), new FacetsCollectorManager());

        var state = TokenSsdvFacetState.create(reader, FACET_FIELD, Optional.empty()).orElseThrow();
        var counts =
            new ParentDedupedSortedSetDocValuesFacetCounts(
                state, childHits, parentFilter, resultParents);

        Map<String, Long> byLabel = new HashMap<>();
        for (LabelAndValue lv : counts.getAllChildren(FACET_FIELD).labelValues) {
          byLabel.put(lv.label, lv.value.longValue());
        }
        assertThat(byLabel).containsEntry("123", 1L); // only X, not Y
        assertThat(byLabel).containsEntry("456", 1L); // X
      }
    }
  }

  private static Document branchDoc(String crd) {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField(FACET_FIELD, new BytesRef(crd)));
    doc.add(new StringField(CRD_TERM_FIELD, crd, StringField.Store.NO));
    return doc;
  }

  private static Document employeeDoc() {
    return employeeDoc("");
  }

  private static Document employeeDoc(String name) {
    Document doc = new Document();
    doc.add(new StringField(DOC_TYPE_FIELD, PARENT_MARKER, StringField.Store.NO));
    if (!name.isEmpty()) {
      doc.add(new StringField(NAME_FIELD, name, StringField.Store.NO));
    }
    return doc;
  }

  /** The main query's result parents when every root document is a result (all employees). */
  private static FacetsCollector allEmployeesAsResultParents(IndexSearcher searcher)
      throws java.io.IOException {
    return searcher.search(
        new TermQuery(new Term(DOC_TYPE_FIELD, PARENT_MARKER)), new FacetsCollectorManager());
  }
}
