package com.xgen.mongot.embedding;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import org.junit.Test;

public class SearchIndexAutoEmbedFieldMappingTest {

  private static final VectorAutoEmbedFieldSpecification SPEC =
      new VectorAutoEmbedFieldSpecification(
          1024,
          VectorSimilarity.COSINE,
          VectorAutoEmbedQuantization.FLOAT,
          new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
              VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS),
          "voyage-3-large",
          "text");

  private static final FieldPath EMBED_PATH = FieldPath.parse("title");
  private static final FieldPath FILTER_PATH = FieldPath.parse("genre");
  private static final FieldPath NESTED_FILTER_PATH = FieldPath.parse("meta.category");
  private static final FieldPath UNDECLARED_PATH = FieldPath.parse("random.field");

  private static final AutoEmbedField.EmbedField EMBED_FIELD =
      new AutoEmbedField.EmbedField(EMBED_PATH, SPEC);

  private static final DynamicDefinition DYNAMIC_ENABLED = new DynamicDefinition.Boolean(true);
  private static final DynamicDefinition DYNAMIC_DISABLED = new DynamicDefinition.Boolean(false);

  // ---- Dynamic mode ----

  @Test
  public void dynamic_onlyEmbedField_isPassthroughTrueForAnyNonEmbedPath() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(EMBED_PATH, EMBED_FIELD), ImmutableSet.of());

    assertThat(mapping.isEmbed(EMBED_PATH)).isTrue();
    assertThat(mapping.isPassthrough(EMBED_PATH)).isFalse();
    assertThat(mapping.isPassthrough(FILTER_PATH)).isTrue();
    assertThat(mapping.isPassthrough(UNDECLARED_PATH)).isTrue();
  }

  @Test
  public void dynamic_descendPredicate_returnsTrueForAnyPath() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(EMBED_PATH, EMBED_FIELD), ImmutableSet.of());

    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("x"))).isTrue();
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("x.y.z"))).isTrue();
    assertThat(mapping.passthroughDescendPredicate().test(EMBED_PATH)).isTrue();
  }

  @Test
  public void dynamic_childPathExists_trueForAnyPath() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(EMBED_PATH, EMBED_FIELD), ImmutableSet.of());

    assertThat(mapping.childPathExists(EMBED_PATH)).isTrue();
    assertThat(mapping.childPathExists(UNDECLARED_PATH)).isTrue();
    assertThat(mapping.subDocumentExists(FieldPath.parse("anything"))).isTrue();
  }

  @Test
  public void dynamic_withDeclaredNonEmbedField_stillTreatsUndeclaredAsPassthrough() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(FILTER_PATH));

    assertThat(mapping.isPassthrough(FILTER_PATH)).isTrue();
    assertThat(mapping.isPassthrough(UNDECLARED_PATH)).isTrue();
    assertThat(mapping.isEmbed(EMBED_PATH)).isTrue();
  }

  // ---- Static mode ----

  @Test
  public void static_declaredFilterField_isPassthrough() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(FILTER_PATH));

    assertThat(mapping.isEmbed(EMBED_PATH)).isTrue();
    assertThat(mapping.isPassthrough(EMBED_PATH)).isFalse();
    assertThat(mapping.isPassthrough(FILTER_PATH)).isTrue();
    assertThat(mapping.isPassthrough(UNDECLARED_PATH)).isFalse();
  }

  @Test
  public void static_descendPredicate_coversAncestorsExplicitsAndDescendants() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(NESTED_FILTER_PATH));

    // Ancestor of an explicit passthrough — descend so we can reach it.
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("meta"))).isTrue();
    // The explicit passthrough path itself — descend so an object/array passthrough copies in full.
    assertThat(mapping.passthroughDescendPredicate().test(NESTED_FILTER_PATH)).isTrue();
    // Descendant of an explicit passthrough — descend so deep subdocs copy in full.
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("meta.category.deep")))
        .isTrue();
    // Unrelated path.
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("other"))).isFalse();
  }

  @Test
  public void static_isPassthrough_treatsDescendantOfExplicitPassthroughAsPassthrough() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(NESTED_FILTER_PATH));

    assertThat(mapping.isPassthrough(NESTED_FILTER_PATH)).isTrue();
    assertThat(mapping.isPassthrough(FieldPath.parse("meta.category.deep"))).isTrue();
    assertThat(mapping.isPassthrough(FieldPath.parse("meta.category.deep.field"))).isTrue();
    assertThat(mapping.isPassthrough(FieldPath.parse("meta.unrelated"))).isFalse();
  }

  @Test
  public void static_passthroughAncestorPaths_onlyFromDeclaredFilters() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(NESTED_FILTER_PATH));

    assertThat(mapping.passthroughAncestorPaths()).contains(FieldPath.parse("meta"));
    // Top-level embed path has no ancestors in the passthrough set.
    assertThat(mapping.passthroughAncestorPaths()).doesNotContain(EMBED_PATH);
  }

  @Test
  public void static_childPathExists_onlyDeclared() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(NESTED_FILTER_PATH));

    assertThat(mapping.childPathExists(EMBED_PATH)).isTrue();
    assertThat(mapping.childPathExists(NESTED_FILTER_PATH)).isTrue();
    assertThat(mapping.childPathExists(FieldPath.parse("meta"))).isTrue();
    assertThat(mapping.childPathExists(UNDECLARED_PATH)).isFalse();
  }

  @Test
  public void static_subDocumentExists_onlyAncestorsOfPassthroughs() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(NESTED_FILTER_PATH));

    assertThat(mapping.subDocumentExists(FieldPath.parse("meta"))).isTrue();
    assertThat(mapping.subDocumentExists(FieldPath.parse("missing"))).isFalse();
  }

  @Test
  public void static_nestedEmbedPath_ancestorIsReachable() {
    FieldPath nestedEmbed = FieldPath.parse("body.text");
    AutoEmbedField.EmbedField nestedEmbedField = new AutoEmbedField.EmbedField(nestedEmbed, SPEC);
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED, ImmutableMap.of(nestedEmbed, nestedEmbedField), ImmutableSet.of());

    // Ancestor of the embed source must be recognized so the traversal descends into the subdoc.
    assertThat(mapping.subDocumentExists(FieldPath.parse("body"))).isTrue();
    assertThat(mapping.childPathExists(FieldPath.parse("body"))).isTrue();
    assertThat(mapping.childPathExists(nestedEmbed)).isTrue();
    // The passthrough-descent predicate must not cover embed ancestors (no passthrough to copy).
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("body"))).isFalse();
  }

  // ---- Shared ----

  @Test
  public void getField_knownEmbedPath_returnsField() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(FILTER_PATH));

    assertThat(mapping.getField(EMBED_PATH)).hasValue(EMBED_FIELD);
    assertThat(mapping.getField(FILTER_PATH))
        .hasValue(new AutoEmbedField.PassthroughField(FILTER_PATH));
    assertThat(mapping.getField(UNDECLARED_PATH)).isEmpty();
  }

  @Test
  public void embedFields_returnsOnlyEmbedFields() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(FILTER_PATH));

    assertThat(mapping.embedFields()).hasSize(1);
    assertThat(mapping.embedFields()).containsKey(EMBED_PATH);
  }

  @Test
  public void fieldMap_includesEmbedAndExplicitPassthrough() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED,
            ImmutableMap.of(EMBED_PATH, EMBED_FIELD),
            ImmutableSet.of(FILTER_PATH));

    assertThat(mapping.fieldMap()).hasSize(2);
    assertThat(mapping.fieldMap()).containsEntry(EMBED_PATH, EMBED_FIELD);
    assertThat(mapping.fieldMap())
        .containsEntry(FILTER_PATH, new AutoEmbedField.PassthroughField(FILTER_PATH));
  }

  @Test
  public void fieldMap_overlappingEmbedAndPassthrough_embedWins() {
    FieldPath shared = FieldPath.parse("content");
    var sharedEmbed = new AutoEmbedField.EmbedField(shared, SPEC);
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED, ImmutableMap.of(shared, sharedEmbed), ImmutableSet.of(shared));

    assertThat(mapping.fieldMap()).hasSize(1);
    assertThat(mapping.fieldMap()).containsEntry(shared, sharedEmbed);
  }

  @Test
  public void fieldMap_dynamicMode_onlyEmbeds() {
    // Dynamic mode's "everything not embed is passthrough" is open-ended, so we don't synthesize
    // entries here - only explicitly-declared paths show up.
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(EMBED_PATH, EMBED_FIELD), ImmutableSet.of());

    assertThat(mapping.fieldMap()).hasSize(1);
    assertThat(mapping.fieldMap()).containsKey(EMBED_PATH);
  }

  @Test
  public void dynamic_isPassthrough_descendantOfEmbed_returnsFalse() {
    FieldPath embedRoot = FieldPath.parse("content");
    FieldPath embedChild = FieldPath.parse("content.metadata");
    var embed = new AutoEmbedField.EmbedField(embedRoot, SPEC);
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(embedRoot, embed), ImmutableSet.of());

    assertThat(mapping.isEmbed(embedRoot)).isTrue();
    assertThat(mapping.isPassthrough(embedRoot)).isFalse();
    assertThat(mapping.isPassthrough(embedChild)).isFalse();
    assertThat(mapping.isPassthrough(FieldPath.parse("other"))).isTrue();
  }

  @Test
  public void dynamic_descendPredicate_skipsEmbedSubtree() {
    FieldPath embedRoot = FieldPath.parse("content");
    var embed = new AutoEmbedField.EmbedField(embedRoot, SPEC);
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_ENABLED, ImmutableMap.of(embedRoot, embed), ImmutableSet.of());

    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("other"))).isTrue();
    assertThat(mapping.passthroughDescendPredicate().test(FieldPath.parse("content.metadata")))
        .isFalse();
  }

  @Test
  public void empty_dynamic_isPassthroughStillTrueForAnyPath() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(DYNAMIC_ENABLED, ImmutableMap.of(), ImmutableSet.of());

    assertThat(mapping.isPassthrough(UNDECLARED_PATH)).isTrue();
    assertThat(mapping.embedFields()).isEmpty();
    // No declared embed or passthrough paths to synthesize from.
    assertThat(mapping.fieldMap()).isEmpty();
  }

  @Test
  public void empty_static_nothingIsPassthrough() {
    var mapping =
        new SearchIndexAutoEmbedFieldMapping(
            DYNAMIC_DISABLED, ImmutableMap.of(), ImmutableSet.of());

    assertThat(mapping.isPassthrough(UNDECLARED_PATH)).isFalse();
    assertThat(mapping.passthroughAncestorPaths()).isEmpty();
  }
}
