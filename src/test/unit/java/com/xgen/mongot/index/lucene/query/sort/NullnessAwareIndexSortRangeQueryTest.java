package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.EXPECTED_MATCH_COUNT;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.LEGACY_VALUE_FIELD;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.LEGACY_VALUE_MULTI_FIELD;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.QUERY_LOWER;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.QUERY_UPPER;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.VALUE_FIELD;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.buildNullnessPrefixedIndex;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.expectedMatchingIds;
import static com.xgen.mongot.index.lucene.query.sort.NullnessPrefixedIndexTestUtils.idsOf;

import com.xgen.mongot.index.lucene.query.custom.NullnessAwareIndexSortRangeQuery;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/**
 * Tests for {@link NullnessAwareIndexSortRangeQuery}.
 *
 * <p>Builds a real index whose index sort has a {@code $meta/nullness}-style prefix field ahead of
 * the range-queried value field - i.e. exactly the shape {@code LuceneIndexSortFactory} produces
 * for {@code NUMBER_INT64_V2}/{@code DATE_V2} fields - and verifies that {@link
 * IndexSortSortedNumericDocValuesRangeQuery}'s fast path, which is otherwise blocked by that prefix
 * field occupying {@code getSort()[0]}, fires once wrapped in {@link
 * NullnessAwareIndexSortRangeQuery}, while producing identical results to both the unwrapped
 * (blocked) query and to today's actual production baseline - the legacy NUMERIC-indexed field
 * queried via {@code RangeQueryUtils}, with no index sort or V2 encoding involved at all.
 */
public class NullnessAwareIndexSortRangeQueryTest {

  @Test
  public void wrappedQuery_usesFastPath_andMatchesExpectedResults() throws IOException {
    try (Directory directory = buildNullnessPrefixedIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      IndexSortSortedNumericDocValuesRangeQuery delegate =
          (IndexSortSortedNumericDocValuesRangeQuery)
              LongField.newRangeQuery(VALUE_FIELD, QUERY_LOWER, QUERY_UPPER);
      NullnessAwareIndexSortRangeQuery wrapped = new NullnessAwareIndexSortRangeQuery(delegate);

      Weight weight = searcher.createWeight(searcher.rewrite(wrapped), ScoreMode.COMPLETE, 1f);
      LeafReaderContext context = reader.leaves().get(0);
      ScorerSupplier scorerSupplier = weight.scorerSupplier(context);

      // The fast path computes the matching range via binary search and reports its *exact* size
      // as cost() - the fallback query would instead report a BKD/doc-values cost *estimate* for
      // the same bounds. An exact match here is only possible if the wrapper actually let the fast
      // path engage despite the nullness field occupying getSort()[0] in the real index sort.
      assertThat(scorerSupplier.cost()).isEqualTo(EXPECTED_MATCH_COUNT);

      assertThat(idsOf(searcher, wrapped)).isEqualTo(expectedMatchingIds());
    }
  }

  @Test
  public void unwrappedDelegate_doesNotUseFastPath_dueToNullnessPrefix_butResultsStillCorrect()
      throws IOException {
    try (Directory directory = buildNullnessPrefixedIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      // Run the *unwrapped* delegate directly against the real (nullness-prefixed) index sort.
      // Its internal eligibility check compares indexSort.getSort()[0].getField() (the nullness
      // field) against the queried field name and fails, so it transparently falls back to its
      // own fallbackQuery - correct results, but without the fast path.
      Query delegate = LongField.newRangeQuery(VALUE_FIELD, QUERY_LOWER, QUERY_UPPER);

      Weight weight = searcher.createWeight(searcher.rewrite(delegate), ScoreMode.COMPLETE, 1f);
      LeafReaderContext context = reader.leaves().get(0);
      ScorerSupplier scorerSupplier = weight.scorerSupplier(context);

      // Known, accepted risk (see plan doc section 6a): cost() comparison isn't a logical proof
      // that the fast path was skipped, just a best-effort signal - it's included here for
      // documentation purposes, not as the primary correctness check.
      assertThat(scorerSupplier.cost()).isNotEqualTo((long) EXPECTED_MATCH_COUNT);

      assertThat(idsOf(searcher, delegate)).isEqualTo(expectedMatchingIds());
    }
  }

  @Test
  public void wrappedQuery_matchesTodaysProductionBaseline_legacyFieldNoIndexSort()
      throws IOException {
    try (Directory directory = buildNullnessPrefixedIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      IndexSortSortedNumericDocValuesRangeQuery delegate =
          (IndexSortSortedNumericDocValuesRangeQuery)
              LongField.newRangeQuery(VALUE_FIELD, QUERY_LOWER, QUERY_UPPER);
      NullnessAwareIndexSortRangeQuery wrapped = new NullnessAwareIndexSortRangeQuery(delegate);

      // Today's actual production baseline: the legacy NUMERIC-indexed field, queried with the
      // real RangeQueryUtils production code, with no index sort/V2/optimization involved at all.
      // Confirms that promoting to V2 + this optimization is a pure performance change, not a
      // semantic one - same query, same documents, same results as what ships today.
      Query todaysProductionQuery =
          RangeQueryUtils.createLuceneRangeQuery(
              LEGACY_VALUE_FIELD, LEGACY_VALUE_MULTI_FIELD, QUERY_LOWER, QUERY_UPPER);

      assertThat(idsOf(searcher, wrapped)).isEqualTo(idsOf(searcher, todaysProductionQuery));
      assertThat(idsOf(searcher, wrapped)).isEqualTo(expectedMatchingIds());
    }
  }

  // --- Known limitation: NUMBER_DOUBLE_V2 vs legacy NUMBER_DOUBLE NaN encoding mismatch ---
  //
  // Unrelated to the nullness-prefix problem NullnessAwareIndexSortRangeQuery solves, but
  // documented in this suite since it surfaced from the same investigation: mongot's legacy
  // NUMBER_DOUBLE field (LuceneDoubleConversionUtils#toLong) and the V2 field
  // (LuceneDoubleConversionUtils#toMqlSortableLong) encode NaN completely differently - toLong has
  // no dedicated NaN handling and happens to place canonical NaN near the top of the positive
  // range, while toMqlSortableLong maps it to a dedicated sentinel below every real value. A range
  // query for "large" values therefore incorrectly matches a NaN document under the legacy
  // encoding, but correctly excludes it under the V2 encoding. This is a known and will addressed
  // in a follow-up.

  private static final String LEGACY_DOUBLE_FIELD = "legacyDouble";
  private static final String LEGACY_DOUBLE_MULTI_FIELD = "legacyDoubleMulti";
  private static final String V2_DOUBLE_FIELD = "v2Double";

  private static Directory buildDoubleIndexWithNaN() throws IOException {
    Directory directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (double value : new double[] {500.0, 1500.0, Double.NaN}) {
        Document doc = new Document();
        doc.add(new StoredField("id", Double.toString(value)));
        long legacyEncoded = LuceneDoubleConversionUtils.toLong(value);
        doc.add(new LongPoint(LEGACY_DOUBLE_FIELD, legacyEncoded));
        doc.add(new NumericDocValuesField(LEGACY_DOUBLE_FIELD, legacyEncoded));
        long v2Encoded = LuceneDoubleConversionUtils.toMqlSortableLong(value);
        doc.add(new LongField(V2_DOUBLE_FIELD, v2Encoded, Field.Store.NO));
        writer.addDocument(doc);
      }
    }
    return directory;
  }

  @Test
  public void doubleRangeQuery_withNaN_legacyAndV2EncodingsDisagree_knownLimitation()
      throws IOException {
    try (Directory directory = buildDoubleIndexWithNaN();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      long lowerBound = 1000L; // querying "value >= 1000.0"
      Query legacyQuery =
          RangeQueryUtils.createLuceneRangeQuery(
              LEGACY_DOUBLE_FIELD,
              LEGACY_DOUBLE_MULTI_FIELD,
              LuceneDoubleConversionUtils.toLong(lowerBound),
              Long.MAX_VALUE);
      Query v2Query =
          LongField.newRangeQuery(
              V2_DOUBLE_FIELD,
              LuceneDoubleConversionUtils.toMqlSortableLong(lowerBound),
              Long.MAX_VALUE);

      Set<String> legacyResults = idsOf(searcher, legacyQuery);
      Set<String> v2Results = idsOf(searcher, v2Query);

      assertThat(legacyResults).contains(Double.toString(1500.0));
      assertThat(v2Results).contains(Double.toString(1500.0));
      assertThat(legacyResults).doesNotContain(Double.toString(500.0));
      assertThat(v2Results).doesNotContain(Double.toString(500.0));

      // The actual, currently-mismatched behavior: legacy's toLong has no NaN handling and happens
      // to encode NaN as a large positive long, incorrectly matching a ">= 1000.0" range query.
      // v2's toMqlSortableLong maps NaN to a dedicated sentinel below every real value, correctly
      // excluding it. NOT fixed here; see the class comment above.
      assertThat(legacyResults).contains(Double.toString(Double.NaN));
      assertThat(v2Results).doesNotContain(Double.toString(Double.NaN));
      assertThat(legacyResults).isNotEqualTo(v2Results);
    }
  }
}
