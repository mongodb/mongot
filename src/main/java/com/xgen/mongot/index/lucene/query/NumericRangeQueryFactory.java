package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.range.InclusiveRangeFactory;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToDoubleTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToLongTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.query.sort.IndexSortUtils;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.commons.lang3.Range;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

public class NumericRangeQueryFactory {

  private static final InclusiveRangeFactory<NumericPoint, Long> LONG_INCLUSIVE_RANGE_FACTORY =
      new InclusiveRangeFactory<>(
          new LongPoint(Long.MIN_VALUE),
          new LongPoint(Long.MAX_VALUE),
          RangeBoundToLongTransformer::getLower,
          RangeBoundToLongTransformer::getUpper);

  private static final InclusiveRangeFactory<NumericPoint, Double> DOUBLE_INCLUSIVE_RANGE_FACTORY =
      new InclusiveRangeFactory<>(
          new DoublePoint(-1.0 * Double.MAX_VALUE),
          new DoublePoint(Double.MAX_VALUE),
          RangeBoundToDoubleTransformer::getLower,
          RangeBoundToDoubleTransformer::getUpper);

  private final QueryFactoryContext queryFactoryContext;
  private final IndexCapabilities indexCapabilities;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  public NumericRangeQueryFactory(
      QueryFactoryContext queryFactoryContext,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.queryFactoryContext = queryFactoryContext;
    this.indexCapabilities = queryFactoryContext.getIndexCapabilities();
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  /**
   * Returns a function that produces queries based on a path option definition.
   *
   * @param bound the bounds of the range to be applied in the returned function.
   * @return a query producing function.
   */
  public CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException> fromBounds(
      RangeBound<NumericPoint> bound) {
    return (path, embeddedRoot) ->
        BooleanComposer.constantScoreDisjunction(
            longQuery(bound).apply(path, embeddedRoot),
            doubleQuery(bound).apply(path, embeddedRoot));
  }

  private BiFunction<FieldPath, Optional<FieldPath>, Query> longQuery(
      RangeBound<NumericPoint> bounds) {
    Optional<Range<Long>> inclusiveRange = LONG_INCLUSIVE_RANGE_FACTORY.createRange(bounds);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                simpleBounds -> {
                  Supplier<Query> legacyQuery =
                      () ->
                          RangeQueryUtils.createLuceneRangeQuery(
                              FieldName.TypeField.NUMBER_INT64.getLuceneFieldName(
                                  path, embeddedRoot),
                              FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
                                  path, embeddedRoot),
                              simpleBounds.getMinimum(),
                              simpleBounds.getMaximum());

                  boolean numericV2Enabled =
                      this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                          DynamicFeatureFlags.NUMERIC_V2_SEMANTICS);
                  if (this.indexCapabilities.supportsEmbeddedNumericAndDateV2()
                      && numericV2Enabled) {
                    var v2Field =
                        FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(path, embeddedRoot);
                    return rangeQuery(
                        path,
                        embeddedRoot,
                        v2Field,
                        simpleBounds.getMinimum(),
                        simpleBounds.getMaximum(),
                        legacyQuery);
                  }
                  return legacyQuery.get();
                })
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bounds + " bounds are outside representable range of long"));
  }

  private BiFunction<FieldPath, Optional<FieldPath>, Query> doubleQuery(
      RangeBound<NumericPoint> bounds) {
    Optional<Range<Double>> inclusiveRange = DOUBLE_INCLUSIVE_RANGE_FACTORY.createRange(bounds);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                simpleBounds -> {
                  Supplier<Query> legacyQuery =
                      () ->
                          RangeQueryUtils.createLuceneRangeQuery(
                              FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(
                                  path, embeddedRoot),
                              FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(
                                  path, embeddedRoot),
                              LuceneDoubleConversionUtils.toLong(simpleBounds.getMinimum()),
                              LuceneDoubleConversionUtils.toLong(simpleBounds.getMaximum()));

                  boolean numericV2Enabled =
                      this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                          DynamicFeatureFlags.NUMERIC_V2_SEMANTICS);
                  if (this.indexCapabilities.supportsEmbeddedNumericAndDateV2()
                      && numericV2Enabled) {
                    var v2Field =
                        FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(path, embeddedRoot);
                    long lower =
                        LuceneDoubleConversionUtils.toMqlSortableLong(simpleBounds.getMinimum());
                    long upper =
                        LuceneDoubleConversionUtils.toMqlSortableLong(simpleBounds.getMaximum());
                    return rangeQuery(path, embeddedRoot, v2Field, lower, upper, legacyQuery);
                  }
                  return legacyQuery.get();
                })
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bounds + " bounds are outside representable range of double"));
  }

  private Query rangeQuery(
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      String v2Field,
      long lower,
      long upper,
      Supplier<Query> fallbackQueryProvider) {
    boolean rangeOptimizationEnabled =
        this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.INDEX_SORT_RANGE_QUERY_OPTIMIZATION_ENABLED);
    return IndexSortUtils.rangeQuery(
        path,
        embeddedRoot,
        v2Field,
        lower,
        upper,
        this.queryFactoryContext.getIndexSort(),
        rangeOptimizationEnabled,
        this.queryFactoryContext.getMetrics().getIndexSortRangeQueryOptimizationAppliedCounter(),
        fallbackQueryProvider);
  }
}
