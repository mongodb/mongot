package com.xgen.mongot.index.lucene.query.sort;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/**
 * Shared real-index test fixture: builds a nullness-prefixed index sort - a {@code
 * $meta/nullness}-style prefix field ahead of a value field in the physical index sort - exactly
 * the shape {@code LuceneIndexSortFactory} produces for {@code NUMBER_INT64_V2}/{@code DATE_V2}
 * fields. Shared by {@link NullnessAwareIndexSortRangeQueryTest} and {@link IndexSortUtilsTest} so
 * both exercise this same production shape without duplicating it.
 */
final class NullnessPrefixedIndexTestUtils {

  static final String NULLNESS_FIELD = "$meta/nullness/value";
  static final String VALUE_FIELD = "value";
  // Today's production (pre-V2) shape for this same logical field: a legacy NUMERIC-indexed field
  // with no index sort at all. LEGACY_VALUE_MULTI_FIELD is never populated (no doc in this test has
  // an array value) - RangeQueryUtils just needs a name for its multi-value branch.
  static final String LEGACY_VALUE_FIELD = "legacyValue";
  static final String LEGACY_VALUE_MULTI_FIELD = "legacyValueMulti";
  private static final long NULLNESS_PRESENT = 0L;
  private static final long NULLNESS_MISSING_SENTINEL = Long.MAX_VALUE;

  static final int NUM_PRESENT_DOCS = 2000;
  static final int NUM_MISSING_DOCS = 200;
  // value(i) = i * 10 for i in [0, NUM_PRESENT_DOCS), so the query range below selects a known,
  // exact, contiguous slice of present documents.
  static final long QUERY_LOWER = 5000L;
  static final long QUERY_UPPER = 5990L;
  static final int EXPECTED_MATCH_COUNT = 100; // i in [500, 599]

  private NullnessPrefixedIndexTestUtils() {}

  /**
   * Builds an index whose physical document order is: all "present" docs (nullness = 0), sorted
   * ascending by {@code VALUE_FIELD}, followed by all "missing" docs (no nullness field, no value
   * field at all) - exactly mirroring what {@code LuceneIndexSortFactory} produces for a
   * NUMBER_INT64_V2/DATE_V2 field whose sort needs a nullness prefix.
   */
  static Directory buildNullnessPrefixedIndex() throws IOException {
    Directory directory = new ByteBuffersDirectory();

    SortField nullnessSortField =
        new SortedNumericSortField(
            NULLNESS_FIELD, SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
    nullnessSortField.setMissingValue(NULLNESS_MISSING_SENTINEL);
    SortField valueSortField =
        new SortedNumericSortField(
            VALUE_FIELD, SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setIndexSort(new Sort(nullnessSortField, valueSortField));

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < NUM_PRESENT_DOCS; i++) {
        long value = i * 10L;
        Document doc = new Document();
        doc.add(new StoredField("id", "present-" + i));
        doc.add(new SortedNumericDocValuesField(NULLNESS_FIELD, NULLNESS_PRESENT));
        // LongField bundles both the point dimension and SORTED_NUMERIC doc values in one field,
        // matching how production V2 fields are actually indexed (IndexableFieldFactory).
        doc.add(new LongField(VALUE_FIELD, value, Field.Store.NO));
        // Today's legacy (pre-V2) shape for the same logical value, held alongside the V2 field on
        // the same document so a search against either returns the same "id".
        doc.add(new LongPoint(LEGACY_VALUE_FIELD, value));
        doc.add(new NumericDocValuesField(LEGACY_VALUE_FIELD, value));
        writer.addDocument(doc);
      }
      for (int i = 0; i < NUM_MISSING_DOCS; i++) {
        Document doc = new Document();
        doc.add(new StoredField("id", "missing-" + i));
        // No nullness field, no value field: mirrors a document missing the sorted value.
        writer.addDocument(doc);
      }
    }

    return directory;
  }

  /** The exact query the range should have matched, computed independently of any query class. */
  static Set<String> expectedMatchingIds() {
    Set<String> expected = new TreeSet<>();
    for (int i = 0; i < NUM_PRESENT_DOCS; i++) {
      long value = i * 10L;
      if (value >= QUERY_LOWER && value <= QUERY_UPPER) {
        expected.add("present-" + i);
      }
    }
    return expected;
  }

  static Set<String> idsOf(IndexSearcher searcher, Query query) throws IOException {
    TopDocs topDocs = searcher.search(query, NUM_PRESENT_DOCS + NUM_MISSING_DOCS + 1);
    Set<String> ids = new TreeSet<>();
    for (var scoreDoc : topDocs.scoreDocs) {
      ids.add(searcher.storedFields().document(scoreDoc.doc).get("id"));
    }
    return ids;
  }
}
