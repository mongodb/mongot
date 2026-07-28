package com.xgen.mongot.index.lucene.query.range;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;

public final class RangeQueryUtils {

  // Records whether numeric int/float field pairs happen to store non-overlapping (disjoint)
  // sets of documents. Needed only for deciding whether to apply the Range Merge optimization.
  private static final ConcurrentHashMap<SegmentFieldGroup, Boolean> DISJOINT_FIELDS_CACHE =
      new ConcurrentHashMap<>();

  public static Query createLuceneRangeQuery(
      String singleValueField, String multiValueField, long lowerBound, long upperBound) {

    // Lucene selects LongPoint or DocValues approach based on runtime statistics
    var singleValueQuery =
        new IndexOrDocValuesQuery(
            createLongPointQuery(singleValueField, lowerBound, upperBound),
            createDocValuesQuery(singleValueField, lowerBound, upperBound));

    // we do not apply optimization for array fields because they are not indexed as DocValues
    var multiValueQuery = createLongPointQuery(multiValueField, lowerBound, upperBound);

    return BooleanComposer.constantScoreDisjunction(singleValueQuery, multiValueQuery);
  }

  public static Query createLongPointQuery(String fieldName, long lowerBound, long upperBound) {
    return org.apache.lucene.document.LongPoint.newRangeQuery(fieldName, lowerBound, upperBound);
  }

  public static Query createDocValuesQuery(String fieldName, long lowerBound, long upperBound) {
    return NumericDocValuesField.newSlowRangeQuery(fieldName, lowerBound, upperBound);
  }

  /**
   * Determines if the point fields in {@code fieldNames} provably hold no array data. Usually this
   * calculation is as simple as: {@link PointValues#size()} equal to {@link
   * PointValues#getDocCount()}. In the case of int+float field pairs (which are implicitly combined
   * by mongot) also checks for overlapping docIDs.
   */
  static boolean hasNoArrayDataInFields(IndexReader reader, List<String> fieldNames)
      throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      @Var int fieldsWithPoints = 0;
      for (String fieldName : fieldNames) {
        PointValues pv = ctx.reader().getPointValues(fieldName);
        if (pv == null) {
          continue;
        }
        if (pv.size() != pv.getDocCount()) {
          return false;
        }
        fieldsWithPoints++;
      }
      if (fieldsWithPoints > 1 && !cachedHasNoOverlappingDocIds(ctx.reader(), fieldNames)) {
        return false;
      }
    }
    return true;
  }

  private static boolean cachedHasNoOverlappingDocIds(LeafReader reader, List<String> fieldNames)
      throws IOException {
    IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
    if (cacheHelper == null) {
      return false;
    }
    SegmentFieldGroup key = new SegmentFieldGroup(cacheHelper.getKey(), List.copyOf(fieldNames));
    Boolean cached = DISJOINT_FIELDS_CACHE.get(key);
    if (cached != null) {
      return cached;
    }
    boolean disjoint = hasNoOverlappingDocIds(reader, fieldNames);
    if (DISJOINT_FIELDS_CACHE.putIfAbsent(key, disjoint) == null) {
      cacheHelper.addClosedListener(
          closed -> DISJOINT_FIELDS_CACHE.keySet().removeIf(k -> k.segment().equals(closed)));
    }
    return disjoint;
  }

  // Calculates whether numeric int/float field pairs happen to store non-overlapping (disjoint)
  // sets of documents. Needed only for deciding whether to apply the Range Merge optimization.
  private static boolean hasNoOverlappingDocIds(LeafReader reader, List<String> fieldNames)
      throws IOException {
    FixedBitSet docsSeen = new FixedBitSet(reader.maxDoc());
    PointValues.IntersectVisitor visitor =
        new PointValues.IntersectVisitor() {
          @Override
          public void visit(int docId) {
            if (docsSeen.getAndSet(docId)) {
              throw new OverlapFoundException();
            }
          }

          @Override
          public void visit(int docId, byte[] packedValue) {
            visit(docId);
          }

          @Override
          public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_INSIDE_QUERY;
          }
        };
    try {
      for (String fieldName : fieldNames) {
        PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues != null) {
          pointValues.intersect(visitor);
        }
      }
    } catch (OverlapFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if there is provably no array data stored in the query's documents.
   *
   * <p>Only {@link NumericRangeBound} and {@link DateRangeBound} are supported. Other bound types
   * throw {@link IllegalStateException}. {@code embeddedRoot} determines the Lucene field names to
   * inspect and must match the embedded root the resulting query will run under; pass {@link
   * Optional#empty()} for queries outside embedded documents.
   */
  public static boolean hasNoArrayData(
      IndexReader reader,
      RangeBound<?> bounds,
      List<FieldPath> paths,
      Optional<FieldPath> embeddedRoot)
      throws IOException {
    for (FieldPath path : paths) {
      if (!hasNoArrayDataInFields(reader, luceneFieldNames(bounds, path, embeddedRoot))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> luceneFieldNames(
      RangeBound<?> bounds, FieldPath path, Optional<FieldPath> embeddedRoot) {
    return switch (bounds) {
      case NumericRangeBound ignored ->
          List.of(
              FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
              FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(path, embeddedRoot));
      case DateRangeBound ignored ->
          List.of(FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, embeddedRoot));
      case BooleanRangeBound ignored -> throw unsupportedBoundType(bounds);
      case ObjectIdRangeBound ignored -> throw unsupportedBoundType(bounds);
      case StringRangeBound ignored -> throw unsupportedBoundType(bounds);
      case UuidRangeBound ignored -> throw unsupportedBoundType(bounds);
    };
  }

  private static IllegalStateException unsupportedBoundType(RangeBound<?> bounds) {
    return new IllegalStateException(
        "hasNoArrayData: unsupported bound type: " + bounds.getClass().getSimpleName());
  }

  private record SegmentFieldGroup(IndexReader.CacheKey segment, List<String> fields) {}

  private static final class OverlapFoundException extends RuntimeException {}
}
