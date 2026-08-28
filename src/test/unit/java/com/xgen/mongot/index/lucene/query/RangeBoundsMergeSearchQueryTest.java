package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
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
 * operator the user could have written directly (or, on multivalued fields, of a hand-marked fused
 * operator taking the translation fallback), and every non-firing case must translate
 * byte-identically to the flags-off translation of its own input.
 */
public class RangeBoundsMergeSearchQueryTest {

  private static final String PATH = "price";
  private static final String VECTOR_PATH = "embedding";
  private static final String EMBEDDED_ROOT = "items";
  private static final String EMBEDDED_PATH = "items.price";

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
  public void compoundFilter_gtAndLt_multiValuedField_fallsBackToConjunctionOfTwoOpens()
      throws Exception {
    // One document with two points in the int64 multi-value variant makes the field multivalued:
    // the rule fuses anyway, and the translation fallback keyed off the fused operator's marker
    // decomposes it back into a conjunction of the two half-open scans instead of one closed scan.
    List<Document> documents = List.of(multiValuedDocument(int64MultipleField(PATH), 3L, 7L));

    Query optimized =
        translation(optimizerEnabled())
            .translateWithLuceneDocuments(twoHalfOpenFilter(), documents);
    Query fallbackBaseline =
        baseline().translateWithLuceneDocuments(fusedClosedRangeFilter(), documents);
    Query closedRangeBaseline =
        baseline().translateWithLuceneDocuments(closedRangeFilter(), documents);

    assertEquals(fallbackBaseline, optimized);
    assertNotEquals(
        "a fused operator on a multivalued field must not translate as a single closed scan",
        closedRangeBaseline,
        optimized);
  }

  @Test
  public void compoundFilter_gtAndLt_multiValuedField_arrayCounterexampleStillMatches()
      throws Exception {
    // { price: [0, 20] }: 20 satisfies gt 1 and 0 satisfies lt 10, but no single element lies in
    // (1, 10), so a single closed scan would drop the document. { price: [12, 15] } fails lt 10
    // and must not match either way.
    List<Document> documents =
        List.of(
            multiValuedDocument(int64MultipleField(PATH), 0L, 20L),
            multiValuedDocument(int64MultipleField(PATH), 12L, 15L));

    int optimized =
        translation(optimizerEnabled())
            .countMatchesWithLuceneDocuments(twoHalfOpenFilter(), documents);
    int unoptimized = baseline().countMatchesWithLuceneDocuments(twoHalfOpenFilter(), documents);

    assertEquals(unoptimized, optimized);
    assertEquals(1, optimized);
  }

  @Test
  public void compoundFilter_userWrittenClosedRange_multiValuedField_translationUnchanged()
      throws Exception {
    // A user-written closed range carries no optimizer marker: even with array data present it
    // keeps translating as a single closed scan — its per-element array semantics are documented
    // behavior and must not be decomposed.
    List<Document> documents = List.of(multiValuedDocument(int64MultipleField(PATH), 3L, 7L));

    Query optimized =
        translation(optimizerEnabled())
            .translateWithLuceneDocuments(closedRangeFilter(), documents);
    Query unoptimized = baseline().translateWithLuceneDocuments(closedRangeFilter(), documents);

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

  // ---- embeddedDocument scope ----

  @Test
  public void embeddedFilter_gtAndLt_optimizerOn_translatesAsSingleClosedRange() throws Exception {
    // Indexes with embedded fields now get the optimization: the fused operator's array-data
    // check runs at translation with the embedded root in hand, so the rule no longer skips them.
    Query optimized =
        embeddedTranslation(optimizerEnabled()).translate(embeddedFilter(twoHalfOpenEmbedded()));
    Query closedRangeBaseline = embeddedBaseline().translate(embeddedFilter(closedRangeEmbedded()));

    assertEquals(closedRangeBaseline, optimized);
  }

  @Test
  public void embeddedFilter_gtAndLt_multiValuedEmbeddedField_fallsBackToConjunctionOfTwoOpens()
      throws Exception {
    // The multivalued point data lives under the embedded-scoped Lucene field name. Detecting it
    // requires resolving field names with the embedded root: with the wrong (root) scope the
    // check would see no array data and wrongly emit the single closed scan.
    List<Document> documents = List.of(multiValuedDocument(embeddedInt64MultipleField(), 3L, 7L));

    Query optimized =
        embeddedTranslation(optimizerEnabled())
            .translateWithLuceneDocuments(embeddedFilter(twoHalfOpenEmbedded()), documents);
    Query fallbackBaseline =
        embeddedBaseline()
            .translateWithLuceneDocuments(embeddedFilter(fusedClosedRangeEmbedded()), documents);
    Query closedRangeBaseline =
        embeddedBaseline()
            .translateWithLuceneDocuments(embeddedFilter(closedRangeEmbedded()), documents);

    assertEquals(fallbackBaseline, optimized);
    assertNotEquals(
        "a fused operator on a multivalued embedded field must not translate as a single closed "
            + "scan",
        closedRangeBaseline,
        optimized);
  }

  // ---- harness and operator helpers ----

  private static LuceneSearchTranslation translation(DynamicFeatureFlagRegistry registry) {
    return LuceneSearchTranslation.mappedWithDynamicFeatureFlags(MAPPING, registry);
  }

  private static LuceneSearchTranslation baseline() {
    return LuceneSearchTranslation.mapped(MAPPING);
  }

  private static LuceneSearchTranslation embeddedTranslation(DynamicFeatureFlagRegistry registry) {
    return LuceneSearchTranslation.mappedWithDynamicFeatureFlags(embeddedMapping(), registry);
  }

  private static LuceneSearchTranslation embeddedBaseline() {
    return LuceneSearchTranslation.mapped(embeddedMapping());
  }

  private static DocumentFieldDefinition embeddedMapping() {
    return DocumentFieldDefinitionBuilder.builder()
        .field(
            EMBEDDED_ROOT,
            FieldDefinitionBuilder.builder()
                .embeddedDocuments(
                    EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                .build())
        .build();
  }

  private static Operator embeddedFilter(CompoundOperator inner) {
    return OperatorBuilder.embeddedDocument().path(EMBEDDED_ROOT).operator(inner).build();
  }

  private static CompoundOperator twoHalfOpenEmbedded() {
    return OperatorBuilder.compound()
        .filter(lowerHalfOpen(EMBEDDED_PATH))
        .filter(upperHalfOpen(EMBEDDED_PATH))
        .build();
  }

  private static CompoundOperator closedRangeEmbedded() {
    return OperatorBuilder.compound().filter(closedRange(EMBEDDED_PATH)).build();
  }

  private static CompoundOperator fusedClosedRangeEmbedded() {
    return OperatorBuilder.compound().filter(markFused(closedRange(EMBEDDED_PATH))).build();
  }

  private static CompoundOperator fusedClosedRangeFilter() {
    return OperatorBuilder.compound().filter(markFused(closedRange(PATH))).build();
  }

  /** Rebuilds the operator with the marker the optimizer sets on fused closed ranges. */
  private static RangeOperator markFused(RangeOperator operator) {
    return new RangeOperator(
        operator.score(), operator.paths(), operator.bounds(), operator.doesNotAffect(), true);
  }

  private static String int64MultipleField(String path) {
    return FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
        FieldPath.parse(path), Optional.empty());
  }

  private static String embeddedInt64MultipleField() {
    return FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
        FieldPath.parse(EMBEDDED_PATH), Optional.of(FieldPath.parse(EMBEDDED_ROOT)));
  }

  private static Document multiValuedDocument(String luceneFieldName, long... values) {
    Document document = new Document();
    for (long value : values) {
      document.add(new org.apache.lucene.document.LongPoint(luceneFieldName, value));
    }
    return document;
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
