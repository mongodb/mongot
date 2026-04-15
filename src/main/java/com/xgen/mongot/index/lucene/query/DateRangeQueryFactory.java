package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.range.InclusiveRangeFactory;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToDateTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Date;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

public class DateRangeQueryFactory {

  private static final InclusiveRangeFactory<DatePoint, Date> DATE_RANGE_SIMPLIFIER =
      new InclusiveRangeFactory<>(
          new DatePoint(new Date(Long.MIN_VALUE)),
          new DatePoint(new Date(Long.MAX_VALUE)),
          RangeBoundToDateTransformer::getLower,
          RangeBoundToDateTransformer::getUpper);

  private final IndexCapabilities indexCapabilities;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  public DateRangeQueryFactory(
      IndexCapabilities indexCapabilities, DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.indexCapabilities = indexCapabilities;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  /**
   * Returns a function that produces queries based on a path option definition.
   *
   * @param bound the bounds of the range to be applied in the returned function.
   * @return a query producing function.
   */
  public CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException> fromBounds(
      RangeBound<DatePoint> bound) {
    Optional<Range<Date>> inclusiveRange = DATE_RANGE_SIMPLIFIER.createRange(bound);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                (simpleBounds) -> {
                  long lower = simpleBounds.getMinimum().getTime();
                  long upper = simpleBounds.getMaximum().getTime();
                  boolean numericV2Enabled =
                      this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                          DynamicFeatureFlags.NUMERIC_V2_SEMANTICS);
                  if (this.indexCapabilities.supportsEmbeddedNumericAndDateV2()
                      && numericV2Enabled) {
                    var v2Field =
                        FieldName.TypeField.DATE_V2.getLuceneFieldName(path, embeddedRoot);
                    return new IndexOrDocValuesQuery(
                        LongPoint.newRangeQuery(v2Field, lower, upper),
                        SortedNumericDocValuesField.newSlowRangeQuery(v2Field, lower, upper));
                  }
                  return RangeQueryUtils.createLuceneRangeQuery(
                      FieldName.TypeField.DATE.getLuceneFieldName(path, embeddedRoot),
                      FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
                      lower,
                      upper);
                })
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bound + " bounds are outside representable range of date"));
  }
}
