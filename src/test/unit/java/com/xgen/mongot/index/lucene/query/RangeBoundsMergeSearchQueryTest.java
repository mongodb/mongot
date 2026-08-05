package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.operators.CompoundOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Translation tests for the {@code RangeBoundsMergeOperatorRule} optimizer rule through its two
 * production entry points: a {@code $search} compound operator translated by {@code
 * LuceneSearchQueryFactoryDistributor#createQuery}, and the operator-syntax filter of the {@code
 * vectorSearch} operator, which is optimized separately inside {@code
 * VectorSearchFilterQueryFactory#createLuceneFilter}.
 *
 * <p>Expectations are translation equivalences rather than hand-built Lucene trees: a fused
 * compound must translate byte-identically to the flags-off translation of the closed-range
 * operator the user could have written directly, and every non-firing case must translate
 * byte-identically to the flags-off translation of its own input.
 */
public class RangeBoundsMergeSearchQueryTest {

  private static final String PATH = "price";
  private static final String VECTOR_PATH = "embedding";

  /**
   * Dynamic mappings for the range field plus an explicit vector field for {@code vectorSearch}.
   */
  private static final DocumentFieldDefinition MAPPING =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(true)
          .field(
              VECTOR_PATH,
              FieldDefinitionBuilder.builder()
                  .knnVector(
                      KnnVectorFieldDefinitionBuilder.builder()
                          .similarity(VectorSimilarity.EUCLIDEAN)
                          .dimensions(3)
                          .build())
                  .build())
          .build();

  // ---- compound operator through $search createQuery ----

  @Test
  public void compoundFilter_gtAndLt_optimizerOn_translatesAsSingleClosedRange() throws Exception {
    Query optimized = translation(optimizerEnabled()).translate(twoHalfOpenFilter());
    Query closedRangeBaseline = baseline().translate(closedRangeFilter());

    assertEquals(closedRangeBaseline, optimized);
  }

  @Test
  public void compoundFilter_gtAndLt_perRuleFlagOff_translationUnchanged() throws Exception {
    Query gated = translation(masterOnRuleOff()).translate(twoHalfOpenFilter());
    Query unoptimized = baseline().translate(twoHalfOpenFilter());

    assertEquals(unoptimized, gated);
  }

  @Test
  public void compoundMust_gtAndLt_optimizerOn_translationUnchanged() throws Exception {
    // The rule never fuses must clauses (each contributes its own score); the optimized
    // translation must be byte-identical to the unoptimized one, scoring wrappers included.
    Query optimized = translation(optimizerEnabled()).translate(twoHalfOpenMust());
    Query unoptimized = baseline().translate(twoHalfOpenMust());

    assertEquals(unoptimized, optimized);
  }

  @Test
  public void compoundFilter_gtAndLt_multiValuedField_translationUnchanged() throws Exception {
    // One document with two points in the int64 multi-value variant makes the field multivalued:
    // the rule's single-valued check must block fusion, leaving the translation unchanged.
    Document multiValued = new Document();
    multiValued.add(new org.apache.lucene.document.LongPoint("$type:int64Multiple/" + PATH, 3L));
    multiValued.add(new org.apache.lucene.document.LongPoint("$type:int64Multiple/" + PATH, 7L));

    Query optimized =
        translation(optimizerEnabled())
            .translateWithLuceneDocuments(twoHalfOpenFilter(), List.of(multiValued));
    Query unoptimized =
        baseline().translateWithLuceneDocuments(twoHalfOpenFilter(), List.of(multiValued));

    assertEquals(unoptimized, optimized);
  }

  @Test
  public void compoundFilter_ninePairsNinePaths_optimizerOn_translatesAllAsClosedRanges()
      throws Exception {
    // Nine pairs exceed the engine's fixed-point iteration cap if fused one pair per iteration;
    // the rule must fuse the whole slot in one application for the last pairs to be reached.
    Query optimized = translation(optimizerEnabled()).translate(manyPairsFilter(9));
    Query closedRangesBaseline = baseline().translate(manyClosedRangesFilter(9));

    assertEquals(closedRangesBaseline, optimized);
  }

  // ---- operator-syntax filter of the vectorSearch operator ----

  @Test
  public void vectorSearchFilter_gtAndLt_optimizerOn_translatesAsSingleClosedRange()
      throws Exception {
    Query optimized =
        translation(optimizerEnabled()).translate(vectorSearchWithFilter(twoHalfOpenFilter()));
    Query closedRangeBaseline = baseline().translate(vectorSearchWithFilter(closedRangeFilter()));

    assertEquals(closedRangeBaseline, optimized);
  }

  @Test
  public void vectorSearchFilter_gtAndLt_perRuleFlagOff_translationUnchanged() throws Exception {
    Query gated =
        translation(masterOnRuleOff()).translate(vectorSearchWithFilter(twoHalfOpenFilter()));
    Query unoptimized = baseline().translate(vectorSearchWithFilter(twoHalfOpenFilter()));

    assertEquals(unoptimized, gated);
  }

  // ---- harness and operator helpers ----

  private static LuceneSearchTranslation translation(DynamicFeatureFlagRegistry registry) {
    return LuceneSearchTranslation.mappedWithDynamicFeatureFlags(MAPPING, registry);
  }

  private static LuceneSearchTranslation baseline() {
    return LuceneSearchTranslation.mapped(MAPPING);
  }

  private static Operator vectorSearchWithFilter(CompoundOperator filter) {
    return OperatorBuilder.vectorSearch()
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .path(VECTOR_PATH)
                .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                .numCandidates(20)
                .limit(10)
                .filter(new VectorSearchFilter.OperatorFilter(filter))
                .build())
        .build();
  }

  private static CompoundOperator twoHalfOpenFilter() {
    return OperatorBuilder.compound()
        .filter(lowerHalfOpen(PATH))
        .filter(upperHalfOpen(PATH))
        .build();
  }

  private static CompoundOperator twoHalfOpenMust() {
    return OperatorBuilder.compound().must(lowerHalfOpen(PATH)).must(upperHalfOpen(PATH)).build();
  }

  private static CompoundOperator closedRangeFilter() {
    return OperatorBuilder.compound().filter(closedRange(PATH)).build();
  }

  private static CompoundOperator manyPairsFilter(int pairs) {
    CompoundOperatorBuilder compound = OperatorBuilder.compound();
    for (int i = 0; i < pairs; i++) {
      compound.filter(lowerHalfOpen(PATH + i)).filter(upperHalfOpen(PATH + i));
    }
    return compound.build();
  }

  private static CompoundOperator manyClosedRangesFilter(int pairs) {
    CompoundOperatorBuilder compound = OperatorBuilder.compound();
    for (int i = 0; i < pairs; i++) {
      compound.filter(closedRange(PATH + i));
    }
    return compound.build();
  }

  private static RangeOperator closedRange(String path) {
    return OperatorBuilder.range()
        .path(path)
        .numericBounds(
            Optional.of(new LongPoint(1L)), Optional.of(new LongPoint(10L)), false, false)
        .build();
  }

  private static RangeOperator lowerHalfOpen(String path) {
    return OperatorBuilder.range()
        .path(path)
        .numericBounds(Optional.of(new LongPoint(1L)), Optional.empty(), false, false)
        .build();
  }

  private static RangeOperator upperHalfOpen(String path) {
    return OperatorBuilder.range()
        .path(path)
        .numericBounds(Optional.empty(), Optional.of(new LongPoint(10L)), false, false)
        .build();
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
}
