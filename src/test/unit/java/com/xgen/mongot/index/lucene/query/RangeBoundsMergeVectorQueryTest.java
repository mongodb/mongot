package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * End-to-end translation tests for the {@code RangeBoundsMergeMqlSimpleClauseRule} optimizer rule.
 *
 * <p>The harness builds a real (in-memory, on-disk via {@code MMapDirectory}) Lucene index for the
 * vector + filter shape and translates a {@link VectorSearchQuery} all the way through the factory.
 * The test fixtures verify two things:
 *
 * <ul>
 *   <li>When both DFFs are on and the filter field is single-valued (the default empty index
 *       satisfies this), the rule fires and the factory emits ONE merged closed-range query (one
 *       BKD scan per type per segment instead of two ANDed half-open scans).
 *   <li>When either DFF is off, the factory emits the original AND-of-two-opens shape unchanged
 *       (byte-identical to pre-PR translation).
 * </ul>
 *
 * <p>The multi-valued fallback case is not covered here because {@code LuceneVectorTranslation}
 * sets up an empty index; that branch is exercised in {@code RangeQueryUtilsTest} (helper) and in
 * the rule-level tests in {@code RangeBoundsMergeMqlSimpleClauseRuleTest}.
 */
public class RangeBoundsMergeVectorQueryTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater METRICS =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private static final String FILTER_PATH = "a";
  private static final String VECTOR_PATH = "vector";

  @Test
  public void gteAndLte_optimizerOn_emitsSingleMergedRange() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gte().value(ValueBuilder.doubleNumber(2)).build(),
            MqlFilterOperatorBuilder.lte().value(ValueBuilder.doubleNumber(9)).build());

    // The factory wraps each SimpleClause in a one-clause MUST BooleanQuery; the inner
    // ConstantScoreQuery (single ranged scan) is the optimization signal — no second MUST.
    var expected =
        knnQuery(singleClosedRangeMust(longRangeQueries(2, 9), doubleRangeQueries(2.0, 9.0)));

    translation(optimizerEnabled()).assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void gtAndLt_optimizerOn_emitsSingleMergedRangeWithExclusiveEndpoints() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)).build(),
            MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(100L)).build());

    var expected =
        knnQuery(
            singleClosedRangeMust(
                longRangeQueries(1, 99),
                doubleRangeQueries(Math.nextUp(0.0), Math.nextDown(100.0))));

    translation(optimizerEnabled()).assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void gtLongAndLtDouble_optimizerOn_emitsSingleMergedRange() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L)).build(),
            MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(10.5)).build());

    // Each endpoint of the merged bound converts per its own point type: the long variant of the
    // exclusive DoublePoint(10.5) upper bound is floor(nextDown(10.5)) = 10, and the double
    // variant of the exclusive LongPoint(1) lower bound is nextUp(1.0).
    var expected =
        knnQuery(
            singleClosedRangeMust(
                longRangeQueries(2, 10),
                doubleRangeQueries(Math.nextUp(1.0), Math.nextDown(10.5))));

    translation(optimizerEnabled()).assertTranslatedTo(vectorQuery(operators), expected);
  }

  /**
   * Wrap the merged single-range {@link ConstantScoreQuery} the way {@code
   * createLuceneFilterForSimpleClause} does: a one-clause {@code BooleanQuery} with the inner range
   * as a {@code MUST}. The wrapper is uniform for any number of operators on a {@link
   * SimpleClause}; what makes this the merged shape is that there is exactly one MUST.
   */
  private static Query singleClosedRangeMust(Query longRange, Query doubleRange) {
    return new Builder()
        .add(
            new ConstantScoreQuery(
                new Builder().add(longRange, Occur.SHOULD).add(doubleRange, Occur.SHOULD).build()),
            Occur.MUST)
        .build();
  }

  @Test
  public void gteAndLte_optimizerOff_keepsAndOfTwoOpens() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gte().value(ValueBuilder.doubleNumber(2)).build(),
            MqlFilterOperatorBuilder.lte().value(ValueBuilder.doubleNumber(9)).build());

    var expected = knnQuery(twoOpensFor(2.0, true, 9.0, true));

    translation(DynamicFeatureFlagRegistry.empty())
        .assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void gtAndLt_perRuleFlagOff_keepsAndOfTwoOpens() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)).build(),
            MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(100L)).build());

    var expected = knnQuery(twoOpensFor(0.0, false, 100.0, false));

    // Master optimizer flag on; per-rule flag off. The rule is filtered out at registry-build
    // time, so the optimizer becomes a no-op and the factory translates the original
    // $gt/$lt operators as it would pre-PR.
    translation(masterOnRuleOff()).assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void gtLongAndLtDouble_multiValuedField_fallsBackToAndOfTwoOpens() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L)).build(),
            MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(10.5)).build());

    // One document with two values for the int64Multiple variant makes the field multi-valued, so
    // the factory must decompose the merged closed range instead of emitting one scan.
    Document multiValued = new Document();
    multiValued.add(
        new org.apache.lucene.document.LongPoint("$type:int64Multiple/" + FILTER_PATH, 3L));
    multiValued.add(
        new org.apache.lucene.document.LongPoint("$type:int64Multiple/" + FILTER_PATH, 7L));

    // Unlike the flag-off shape (two sibling MUSTs, one per original operator), the fallback is
    // nested one level deeper: the single closed-range operator translates to a BooleanQuery of
    // the two reconstructed opens, each half keeping its endpoint's original point type.
    var expected =
        knnQuery(
            new Builder()
                .add(
                    new Builder()
                        .add(
                            new ConstantScoreQuery(
                                new Builder()
                                    .add(longRangeQueries(2, Long.MAX_VALUE), Occur.SHOULD)
                                    .add(
                                        doubleRangeQueries(
                                            Math.nextUp(1.0), 1.0 * Double.MAX_VALUE),
                                        Occur.SHOULD)
                                    .build()),
                            Occur.MUST)
                        .add(
                            new ConstantScoreQuery(
                                new Builder()
                                    .add(longRangeQueries(Long.MIN_VALUE, 10), Occur.SHOULD)
                                    .add(
                                        doubleRangeQueries(
                                            -1.0 * Double.MAX_VALUE, Math.nextDown(10.5)),
                                        Occur.SHOULD)
                                    .build()),
                            Occur.MUST)
                        .build(),
                    Occur.MUST)
                .build());

    translation(optimizerEnabled())
        .withDocuments(List.of(multiValued))
        .assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void singleOperator_optimizerOn_unchanged() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(5L)).build());

    Query result = translation(optimizerEnabled()).translate(vectorQuery(operators));

    var expected =
        knnQuery(
            new Builder()
                .add(
                    new ConstantScoreQuery(
                        new Builder()
                            .add(longRangeQueries(5, Long.MAX_VALUE), Occur.SHOULD)
                            .add(doubleRangeQueries(5.0, 1.0 * Double.MAX_VALUE), Occur.SHOULD)
                            .build()),
                    Occur.MUST)
                .build());

    assertEquals(expected, result);
  }

  @Test
  public void twoLowerBounds_optimizerOn_unchanged() throws Exception {
    List<MqlFilterOperator> operators =
        List.of(
            MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)).build(),
            MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(5L)).build());

    var expected =
        knnQuery(
            new Builder()
                .add(
                    new ConstantScoreQuery(
                        new Builder()
                            .add(longRangeQueries(1, Long.MAX_VALUE), Occur.SHOULD)
                            .add(
                                doubleRangeQueries(Math.nextUp(0.0), 1.0 * Double.MAX_VALUE),
                                Occur.SHOULD)
                            .build()),
                    Occur.MUST)
                .add(
                    new ConstantScoreQuery(
                        new Builder()
                            .add(longRangeQueries(5, Long.MAX_VALUE), Occur.SHOULD)
                            .add(doubleRangeQueries(5.0, 1.0 * Double.MAX_VALUE), Occur.SHOULD)
                            .build()),
                    Occur.MUST)
                .build());

    translation(optimizerEnabled()).assertTranslatedTo(vectorQuery(operators), expected);
  }

  @Test
  public void andOfNineCrossClausePairs_optimizerOn_translatesSameAsSameClauseForm()
      throws Exception {
    // Nine cross-clause pairs exceed the engine's fixed-point iteration cap if fused one pair per
    // iteration, so this equivalence holds only when the rule fuses every pair in one application.
    List<FieldPath> paths = ninePaths();

    Query crossClause =
        translation(optimizerEnabled(), paths).translate(vectorQuery(crossClauseForm(paths)));
    Query sameClause =
        translation(optimizerEnabled(), paths).translate(vectorQuery(sameClauseForm(paths)));

    assertEquals(sameClause, crossClause);
  }

  private static List<FieldPath> ninePaths() {
    List<FieldPath> paths = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      paths.add(FieldPath.parse(FILTER_PATH + i));
    }
    return paths;
  }

  /**
   * One {@code $gt} clause and one {@code $lt} clause per path: {@code {$and: [{aN: {$gt: 1}}, {aN:
   * {$lt: 10}}, ...]}}.
   */
  private static Clause crossClauseForm(List<FieldPath> paths) throws Exception {
    var and = ClauseBuilder.andClause().explicitAnd(true);
    for (FieldPath path : paths) {
      and.addClause(
          ClauseBuilder.simpleClause()
              .path(path)
              .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L)))
              .build());
      and.addClause(
          ClauseBuilder.simpleClause()
              .path(path)
              .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
              .build());
    }
    return and.build();
  }

  /** The equivalent same-clause shape: {@code {$and: [{aN: {$gt: 1, $lt: 10}}, ...]}}. */
  private static Clause sameClauseForm(List<FieldPath> paths) throws Exception {
    var and = ClauseBuilder.andClause().explicitAnd(true);
    for (FieldPath path : paths) {
      and.addClause(
          ClauseBuilder.simpleClause()
              .path(path)
              .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L)))
              .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
              .build());
    }
    return and.build();
  }

  private static MongotKnnFloatQuery knnQuery(Query inner) {
    return new MongotKnnFloatQuery(
        METRICS,
        FeatureFlags.getDefault(),
        "$type:knnVector/vector",
        new float[] {1, 2, 3},
        20,
        inner);
  }

  private static Query twoOpensFor(
      double lower, boolean lowerInclusive, double upper, boolean upperInclusive) {
    long longLow = lowerInclusive ? (long) lower : (long) lower + 1;
    long longHigh = upperInclusive ? (long) upper : (long) upper - 1;
    double doubleLow = lowerInclusive ? lower : Math.nextUp(lower);
    double doubleHigh = upperInclusive ? upper : Math.nextDown(upper);
    return new Builder()
        .add(
            new ConstantScoreQuery(
                new Builder()
                    .add(longRangeQueries(longLow, Long.MAX_VALUE), Occur.SHOULD)
                    .add(doubleRangeQueries(doubleLow, 1.0 * Double.MAX_VALUE), Occur.SHOULD)
                    .build()),
            Occur.MUST)
        .add(
            new ConstantScoreQuery(
                new Builder()
                    .add(longRangeQueries(Long.MIN_VALUE, longHigh), Occur.SHOULD)
                    .add(doubleRangeQueries(-1.0 * Double.MAX_VALUE, doubleHigh), Occur.SHOULD)
                    .build()),
            Occur.MUST)
        .build();
  }

  private VectorSearchQuery vectorQuery(List<MqlFilterOperator> operators) {
    return vectorQuery(
        ClauseBuilder.simpleClause()
            .path(FieldPath.parse(FILTER_PATH))
            .operators(operators)
            .build());
  }

  private VectorSearchQuery vectorQuery(Clause filterClause) {
    return VectorQueryBuilder.builder()
        .index("test")
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .path(FieldPath.parse(VECTOR_PATH))
                .numCandidates(20)
                .limit(10)
                .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                .filter(new VectorSearchFilter.ClauseFilter(filterClause))
                .build())
        .build();
  }

  private LuceneVectorTranslation translation(DynamicFeatureFlagRegistry dffRegistry) {
    return translation(dffRegistry, List.of(FieldPath.parse(FILTER_PATH)));
  }

  private LuceneVectorTranslation translation(
      DynamicFeatureFlagRegistry dffRegistry, List<FieldPath> filterPaths) {
    List<VectorIndexFieldDefinition> definitions = new ArrayList<>();
    definitions.add(
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse(VECTOR_PATH))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build());
    for (FieldPath filterPath : filterPaths) {
      definitions.add(VectorIndexFilterFieldDefinition.create(filterPath));
    }
    return new LuceneVectorTranslation(definitions, FeatureFlags.getDefault(), dffRegistry);
  }

  private static DynamicFeatureFlagRegistry optimizerEnabled() {
    return registryWith(
        flagConfig(DynamicFeatureFlags.QUERY_OPTIMIZER, Phase.ENABLED),
        flagConfig(DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION, Phase.ENABLED));
  }

  private static DynamicFeatureFlagRegistry masterOnRuleOff() {
    return registryWith(
        flagConfig(DynamicFeatureFlags.QUERY_OPTIMIZER, Phase.ENABLED),
        flagConfig(DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION, Phase.DISABLED));
  }

  private static DynamicFeatureFlagConfig flagConfig(DynamicFeatureFlags flag, Phase phase) {
    return new DynamicFeatureFlagConfig(
        flag.getName(), phase, List.of(), List.of(), 0, Scope.GROUP);
  }

  private static DynamicFeatureFlagRegistry registryWith(DynamicFeatureFlagConfig... configs) {
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(configs)),
        Optional.empty(),
        Optional.of(new ObjectId()),
        Optional.empty());
  }

  private static Query longRangeQueries(long lowerBound, long upperBound) {
    return BooleanComposer.constantScoreDisjunction(
        new IndexOrDocValuesQuery(
            org.apache.lucene.document.LongPoint.newRangeQuery(
                "$type:int64/" + FILTER_PATH, lowerBound, upperBound),
            NumericDocValuesField.newSlowRangeQuery(
                "$type:int64/" + FILTER_PATH, lowerBound, upperBound)),
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:int64Multiple/" + FILTER_PATH, lowerBound, upperBound));
  }

  private static Query doubleRangeQueries(double lowerBound, double upperBound) {
    long convertedLowerBound = LuceneDoubleConversionUtils.toLong(lowerBound);
    long convertedUpperBound = LuceneDoubleConversionUtils.toLong(upperBound);
    return BooleanComposer.constantScoreDisjunction(
        new IndexOrDocValuesQuery(
            org.apache.lucene.document.LongPoint.newRangeQuery(
                "$type:double/" + FILTER_PATH, convertedLowerBound, convertedUpperBound),
            NumericDocValuesField.newSlowRangeQuery(
                "$type:double/" + FILTER_PATH, convertedLowerBound, convertedUpperBound)),
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:doubleMultiple/" + FILTER_PATH, convertedLowerBound, convertedUpperBound));
  }
}
