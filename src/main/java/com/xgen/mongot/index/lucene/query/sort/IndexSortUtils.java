package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.NullnessAwareIndexSortRangeQuery;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.util.FieldPath;
import io.micrometer.core.instrument.Counter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/** Utility class for index sort operation. */
public class IndexSortUtils {

  /**
   * Extracts the index sort metadata from the first leaf reader.
   *
   * <p>Since index sort is applied uniformly across all segments in index, checking the first leaf
   * reader is sufficient. If the first segment has no index sort, then no segment has index sort.
   *
   * @param reader the index reader
   * @return the index sort metadata
   */
  public static Optional<Sort> extractFirstIndexSort(IndexReader reader) {
    if (reader.leaves().isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(reader.leaves().get(0).reader().getMetaData().sort());
  }

  /**
   * Determines if a query sort uses index sort optimization.
   *
   * <p>this method is the same as TopFieldCollector#canEarlyTerminateOnPrefix.
   */
  public static boolean usesIndexSort(
      org.apache.lucene.search.Sort querySort, org.apache.lucene.search.Sort indexSort) {
    SortField[] querySortFields = querySort.getSort();
    SortField[] indexSortFields = indexSort.getSort();

    // early termination is possible if querySortFields is a prefix of indexSortFields
    if (querySortFields.length > indexSortFields.length) {
      return false;
    }

    return Arrays.asList(querySortFields)
        .equals(Arrays.asList(indexSortFields).subList(0, querySortFields.length));
  }

  /**
   * Compares two {@link SortField} instances by their semantic properties (field name, type,
   * reverse, comparator source, missing value) without checking {@code getClass()}.
   *
   * <p>Shared by {@link MqlSortedNumericSortField} and {@link MqlSortedSetSortField} to avoid
   * duplicating the SortField-level equality logic.
   */
  static boolean sortFieldBaseEquals(SortField self, SortField other) {
    if (!Objects.equals(self.getField(), other.getField())) {
      return false;
    }
    if (self.getType() != other.getType()) {
      return false;
    }
    if (self.getReverse() != other.getReverse()) {
      return false;
    }
    if (!Objects.equals(self.getComparatorSource(), other.getComparatorSource())) {
      return false;
    }
    return Objects.equals(self.getMissingValue(), other.getMissingValue());
  }

  /**
   * Checks whether expanding the query sort with nullness fields would produce a sort that aligns
   * with the index sort. Splits the index sort into nullness fields and value fields, then verifies
   * that each query field's base field path (stripped of the {@code $type:} prefix) matches the
   * corresponding value field at the same position.
   *
   * <p>Comparison uses the base field path rather than the full Lucene field name so that per-shard
   * type differences (e.g., one shard has INT64 data, another has TOKEN data) do not produce
   * inconsistent expansion decisions across shards.
   *
   * @return {@code true} if the expanded query sort would be a valid prefix of the index sort
   */
  public static boolean expandedSortAlignsWithIndexSort(
      List<MongotSortField> queryFields, Sort indexSort) {
    SortField[] allIndexFields = indexSort.getSort();
    String nullnessPrefix = FieldName.MetaField.NULLNESS.getLuceneFieldName();

    List<String> indexValueNames =
        Arrays.stream(allIndexFields)
            .map(SortField::getField)
            .filter(name -> name == null || !name.startsWith(nullnessPrefix))
            .toList();

    if (queryFields.size() > indexValueNames.size()) {
      return false;
    }

    for (int i = 0; i < queryFields.size(); i++) {
      MongotSortField field = queryFields.get(i);
      String indexFieldName = indexValueNames.get(i);
      Optional<FieldName.TypeField> indexType = FieldName.TypeField.getTypeOf(indexFieldName);
      if (indexType.isPresent()) {
        if (!indexType.get().stripPrefix(indexFieldName).equals(field.field().toString())) {
          return false;
        }
      } else if (!indexFieldName.equals(field.field().toString())) {
        // MqlMixedSort uses the plain field path as Lucene field name (no $type: prefix).
        return false;
      }
    }
    return true;
  }

  /**
   * Builds a range query over a V2 numeric/date field stored as a {@code long}.
   *
   * <p>When the index has an index sort and the optimization is enabled, uses the binary-search
   * fast path ({@link LongField#newRangeQuery}), wrapping the result in {@link
   * NullnessAwareIndexSortRangeQuery} so it still fires when a {@code $nullness} prefix field
   * precedes it in the index sort. Otherwise falls back to {@code fallbackQueryProvider} - a lazy
   * supplier so the fallback query is only built when actually needed, and so this method stays
   * agnostic to what shape a caller's fallback query takes.
   *
   * <p>Increments {@code indexSortRangeQueryOptimizationAppliedCounter} when the range query can
   * use index sort to speed up. We can't blindly increment it when the rangeQuery is
   * IndexSortSortedNumericDocValuesRangeQuery since its internal check can skip the optimization.
   */
  public static Query rangeQuery(
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      String field,
      long lower,
      long upper,
      Optional<com.xgen.mongot.index.query.sort.Sort> indexSort,
      boolean optimizationEnabled,
      Counter indexSortRangeQueryOptimizationAppliedCounter,
      Supplier<Query> fallbackQueryProvider) {
    if (indexSort.isPresent() && optimizationEnabled) {
      Query rangeQuery = LongField.newRangeQuery(field, lower, upper);
      if (rangeQuery instanceof IndexSortSortedNumericDocValuesRangeQuery indexSortQuery) {
        if (isPrimaryIndexSortField(indexSort.get(), path, embeddedRoot)) {
          indexSortRangeQueryOptimizationAppliedCounter.increment();
        }
        return new NullnessAwareIndexSortRangeQuery(indexSortQuery);
      }
      return rangeQuery;
    }
    return fallbackQueryProvider.get();
  }

  /**
   * Returns whether {@code path} is the first field of {@code indexSort}'s spec, i.e. the field the
   * physical Lucene index sort is actually ordered by first (index sort fields are always
   * top-level, so any {@code embeddedRoot} disqualifies a match).
   */
  private static boolean isPrimaryIndexSortField(
      com.xgen.mongot.index.query.sort.Sort indexSort,
      FieldPath path,
      Optional<FieldPath> embeddedRoot) {
    if (embeddedRoot.isPresent() || indexSort.getSortFields().isEmpty()) {
      return false;
    }
    return indexSort.getSortFields().getFirst().field().equals(path);
  }
}
