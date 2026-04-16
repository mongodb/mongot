package com.xgen.mongot.embedding;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import org.junit.Test;

public class VectorAutoEmbedFieldMappingTest {

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
  private static final FieldPath NESTED_EMBED_PATH = FieldPath.parse("a.b.c");
  private static final FieldPath FILTER_PATH = FieldPath.parse("genre");

  private static final AutoEmbedField.EmbedField EMBED_FIELD =
      new AutoEmbedField.EmbedField(EMBED_PATH, SPEC);
  private static final AutoEmbedField.EmbedField NESTED_EMBED_FIELD =
      new AutoEmbedField.EmbedField(NESTED_EMBED_PATH, SPEC);
  private static final AutoEmbedField.PassthroughField PASSTHROUGH_FIELD =
      new AutoEmbedField.PassthroughField(FILTER_PATH);

  private static final VectorAutoEmbedFieldMapping MAPPING =
      new VectorAutoEmbedFieldMapping(
          ImmutableMap.of(
              EMBED_PATH, EMBED_FIELD,
              NESTED_EMBED_PATH, NESTED_EMBED_FIELD,
              FILTER_PATH, PASSTHROUGH_FIELD));

  @Test
  public void fieldMap_returnsAllFields() {
    assertThat(MAPPING.fieldMap()).hasSize(3);
    assertThat(MAPPING.fieldMap()).containsKey(EMBED_PATH);
    assertThat(MAPPING.fieldMap()).containsKey(NESTED_EMBED_PATH);
    assertThat(MAPPING.fieldMap()).containsKey(FILTER_PATH);
  }

  @Test
  public void childPathExists_directField_returnsTrue() {
    assertThat(MAPPING.childPathExists(EMBED_PATH)).isTrue();
    assertThat(MAPPING.childPathExists(FILTER_PATH)).isTrue();
    assertThat(MAPPING.childPathExists(NESTED_EMBED_PATH)).isTrue();
  }

  @Test
  public void childPathExists_ancestorOfField_returnsTrue() {
    assertThat(MAPPING.childPathExists(FieldPath.parse("a"))).isTrue();
    assertThat(MAPPING.childPathExists(FieldPath.parse("a.b"))).isTrue();
  }

  @Test
  public void childPathExists_unknownPath_returnsFalse() {
    assertThat(MAPPING.childPathExists(FieldPath.parse("unknown"))).isFalse();
  }

  @Test
  public void subDocumentExists_ancestor_returnsTrue() {
    assertThat(MAPPING.subDocumentExists(FieldPath.parse("a"))).isTrue();
    assertThat(MAPPING.subDocumentExists(FieldPath.parse("a.b"))).isTrue();
  }

  @Test
  public void subDocumentExists_leafField_returnsFalse() {
    assertThat(MAPPING.subDocumentExists(EMBED_PATH)).isFalse();
    assertThat(MAPPING.subDocumentExists(NESTED_EMBED_PATH)).isFalse();
  }

  @Test
  public void subDocumentExists_unknownPath_returnsFalse() {
    assertThat(MAPPING.subDocumentExists(FieldPath.parse("unknown"))).isFalse();
  }

  @Test
  public void getField_knownPath_returnsField() {
    assertThat(MAPPING.getField(EMBED_PATH)).hasValue(EMBED_FIELD);
    assertThat(MAPPING.getField(FILTER_PATH)).hasValue(PASSTHROUGH_FIELD);
  }

  @Test
  public void getField_unknownPath_returnsEmpty() {
    assertThat(MAPPING.getField(FieldPath.parse("unknown"))).isEmpty();
  }

  @Test
  public void isEmbed_embedField_returnsTrue() {
    assertThat(MAPPING.isEmbed(EMBED_PATH)).isTrue();
    assertThat(MAPPING.isEmbed(NESTED_EMBED_PATH)).isTrue();
  }

  @Test
  public void isEmbed_passthroughField_returnsFalse() {
    assertThat(MAPPING.isEmbed(FILTER_PATH)).isFalse();
  }

  @Test
  public void isEmbed_unknownPath_returnsFalse() {
    assertThat(MAPPING.isEmbed(FieldPath.parse("unknown"))).isFalse();
  }

  @Test
  public void isPassthrough_passthroughField_returnsTrue() {
    assertThat(MAPPING.isPassthrough(FILTER_PATH)).isTrue();
  }

  @Test
  public void isPassthrough_embedField_returnsFalse() {
    assertThat(MAPPING.isPassthrough(EMBED_PATH)).isFalse();
  }

  @Test
  public void isPassthrough_unknownPath_returnsFalse() {
    assertThat(MAPPING.isPassthrough(FieldPath.parse("unknown"))).isFalse();
  }

  @Test
  public void embedFields_returnsOnlyEmbedFields() {
    Map<FieldPath, AutoEmbedField.EmbedField> embedFields = MAPPING.embedFields();
    assertThat(embedFields).hasSize(2);
    assertThat(embedFields).containsKey(EMBED_PATH);
    assertThat(embedFields).containsKey(NESTED_EMBED_PATH);
    assertThat(embedFields).doesNotContainKey(FILTER_PATH);
  }

  @Test
  public void ancestorPaths_computedFromAllFields() {
    // "a.b.c" contributes ancestors "a" and "a.b"
    assertThat(MAPPING.ancestorPaths()).contains(FieldPath.parse("a"));
    assertThat(MAPPING.ancestorPaths()).contains(FieldPath.parse("a.b"));
    // Leaf paths themselves are not ancestors
    assertThat(MAPPING.ancestorPaths()).doesNotContain(EMBED_PATH);
    assertThat(MAPPING.ancestorPaths()).doesNotContain(NESTED_EMBED_PATH);
    assertThat(MAPPING.ancestorPaths()).doesNotContain(FILTER_PATH);
  }

  @Test
  public void passthroughAncestorPaths_computedFromPassthroughFieldsOnly() {
    // "genre" is a root-level passthrough — it has no ancestors
    assertThat(MAPPING.passthroughAncestorPaths()).isEmpty();
  }

  @Test
  public void passthroughAncestorPaths_nestedPassthrough_containsAncestors() {
    var mapping =
        new VectorAutoEmbedFieldMapping(
            ImmutableMap.of(
                FieldPath.parse("x.y.z"),
                new AutoEmbedField.PassthroughField(FieldPath.parse("x.y.z")),
                EMBED_PATH,
                EMBED_FIELD));

    assertThat(mapping.passthroughAncestorPaths()).contains(FieldPath.parse("x"));
    assertThat(mapping.passthroughAncestorPaths()).contains(FieldPath.parse("x.y"));
  }

  @Test
  public void emptyMapping_allMethodsReturnEmpty() {
    var empty = new VectorAutoEmbedFieldMapping(ImmutableMap.of());
    assertThat(empty.fieldMap()).isEmpty();
    assertThat(empty.embedFields()).isEmpty();
    assertThat(empty.ancestorPaths()).isEmpty();
    assertThat(empty.passthroughAncestorPaths()).isEmpty();
    assertThat(empty.childPathExists(FieldPath.parse("a"))).isFalse();
    assertThat(empty.subDocumentExists(FieldPath.parse("a"))).isFalse();
    assertThat(empty.getField(FieldPath.parse("a"))).isEmpty();
  }
}
