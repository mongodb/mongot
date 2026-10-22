package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import org.junit.Before;
import org.junit.Test;

public class AutoEmbedFieldSetTest {

  private static final String MODEL = "voyage-3-large";

  @Before
  public void registerEmbeddingModel() {
    EmbeddingModelCatalogTestUtils.resetCatalogAndRegisterAutoEmbedModel(MODEL);
  }

  @Test
  public void create_nestedAutoEmbed_keysByMappingPath() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "description",
                                FieldDefinitionBuilder.builder()
                                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                    .build())
                            .build())
                    .build())
            .build();

    AutoEmbedFieldSet fieldSet = AutoEmbedFieldSet.create(mappings);

    assertThat(fieldSet.byPath().keySet()).containsExactly(FieldPath.parse("parent.description"));
    assertThat(fieldSet.get(FieldPath.parse("parent.description")).orElseThrow().modelName())
        .isEqualTo(MODEL);
  }

  @Test
  public void create_literalDottedKey_keepsSingleMappingKeySegment() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "description.text",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                    .build())
            .build();

    FieldPath sourcePath = AutoEmbedFieldSet.create(mappings).byPath().keySet().iterator().next();

    assertThat(sourcePath.isAtRoot()).isTrue();
    assertThat(sourcePath.getLeaf()).isEqualTo("description.text");
  }

  @Test
  public void create_renderedPathCollision_keepsOneDefinition() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "parent.content",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                    .build())
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "content",
                                FieldDefinitionBuilder.builder()
                                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                    .build())
                            .build())
                    .build())
            .build();

    AutoEmbedFieldSet fieldSet = AutoEmbedFieldSet.create(mappings);

    assertThat(fieldSet.byPath()).hasSize(1);
    assertThat(fieldSet.modelNamePerPath().values()).containsExactly(MODEL);
  }

  @Test
  public void create_noAutoEmbed_isEmpty() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder().dynamic(false).build();

    assertThat(AutoEmbedFieldSet.create(mappings).isEmpty()).isTrue();
  }
}
