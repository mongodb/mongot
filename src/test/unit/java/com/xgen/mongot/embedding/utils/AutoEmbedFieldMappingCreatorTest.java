package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;

import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorEmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutoEmbedFieldMappingCreatorTest {

  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      MAT_VIEW_SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of());

  @BeforeClass
  public static void registerTestModels() {
    EmbeddingModelCatalog.clear();
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000),
                    Optional.of("text"),
                    Optional.of(VectorAutoEmbedQuantization.FLOAT)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));
  }

  @Test
  public void createAutoEmbedMapping_mixedFieldTypes() {
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("title"));
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("genre"));
    var vectorField =
        new VectorDataFieldDefinition(
            FieldPath.parse("embedding"),
            new VectorFieldSpecification(
                1024,
                VectorSimilarity.COSINE,
                VectorAutoEmbedQuantization.FLOAT.toLuceneQuantization(),
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS)));
    var embeddedDocsField =
        new VectorEmbeddedDocumentsFieldDefinition(FieldPath.parse("chapters"), Map.of());
    List<VectorIndexFieldDefinition> fields =
        List.of(autoEmbedField, filterField, vectorField, embeddedDocsField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    // Only AUTO_EMBED and FILTER fields should be in the mapping.
    Assert.assertEquals(2, mapping.fieldMap().size());
    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("title")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("genre")));
    // VECTOR and EMBEDDED_DOCUMENTS should be excluded.
    Assert.assertFalse(mapping.fieldMap().containsKey(FieldPath.parse("embedding")));
    Assert.assertFalse(mapping.fieldMap().containsKey(FieldPath.parse("chapters")));
  }

  @Test
  public void createMatViewAutoEmbedMapping_schemaVersion0_hashAsSuffix() {
    var defaultAutoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, MAT_VIEW_SCHEMA_METADATA);

    // 2 auto-embed fields + 1 filter field + 2 hash fields (as passthrough).
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewMapping.fieldMap().size());
    Assert.assertTrue(
        matViewMapping.fieldMap().containsKey(FieldPath.parse("a" + HASH_FIELD_SUFFIX)));
    Assert.assertTrue(
        matViewMapping.fieldMap().containsKey(FieldPath.parse("b" + HASH_FIELD_SUFFIX)));
  }

  @Test
  public void createMatViewAutoEmbedMapping_schemaVersion1_hashAsPrefix() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("b.a"),
                FieldPath.parse("_autoEmbed.b.a"),
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a")));
    var defaultAutoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, schemaMetadata);

    // 2 auto-embed fields + 1 filter field + 2 hash fields (as passthrough).
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewMapping.fieldMap().size());
    Assert.assertTrue(
        matViewMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".b.a")));
    Assert.assertTrue(
        matViewMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".a")));
  }

  @Test
  public void getHashFieldPath_rootLevel() {
    Assert.assertEquals(
        "a" + HASH_FIELD_SUFFIX,
        AutoEmbedFieldMappingCreator.getHashFieldPath(FieldPath.parse("a"), 0).toString());
  }

  @Test
  public void getHashFieldPath_nested() {
    Assert.assertEquals(
        "a.b.c" + HASH_FIELD_SUFFIX,
        AutoEmbedFieldMappingCreator.getHashFieldPath(FieldPath.parse("a.b.c"), 0).toString());
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesHnswOptions() {
    var hnswOptions = new VectorFieldSpecification.HnswOptions(32, 200);
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(hnswOptions));
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedField, filterField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            indexDefinition, MAT_VIEW_SCHEMA_METADATA);

    var embedSpec =
        (VectorAutoEmbedFieldSpecification)
            matViewMapping.embedFields().values().stream().findFirst().get().specification();
    Assert.assertTrue(
        embedSpec.indexingAlgorithm() instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm);
    var preservedHnsw =
        (VectorIndexingAlgorithm.HnswIndexingAlgorithm) embedSpec.indexingAlgorithm();
    Assert.assertEquals(32, preservedHnsw.options().maxEdges());
    Assert.assertEquals(200, preservedHnsw.options().numEdgeCandidates());
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesIndexingMethodFlat() {
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.FlatIndexingAlgorithm());
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedField, filterField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            indexDefinition, MAT_VIEW_SCHEMA_METADATA);

    var embedSpec =
        (VectorAutoEmbedFieldSpecification)
            matViewMapping.embedFields().values().stream().findFirst().get().specification();
    Assert.assertTrue(
        embedSpec.indexingAlgorithm() instanceof VectorIndexingAlgorithm.FlatIndexingAlgorithm);
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesNestedRootAndHashPaths() {
    var autoEmbedUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.embedding"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterUnderNested = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedUnderNested, filterUnderNested);
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(fields).nestedRoot("sections").build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, MAT_VIEW_SCHEMA_METADATA);

    FieldPath nestedAutoEmbedPath = FieldPath.parse("sections.embedding");
    FieldPath expectedHashPath = FieldPath.parse("sections.embedding" + HASH_FIELD_SUFFIX);
    Assert.assertTrue(
        "Should contain original auto-embed path",
        matViewMapping.fieldMap().containsKey(nestedAutoEmbedPath));
    Assert.assertTrue(
        "Should contain hash path for nested auto-embed field",
        matViewMapping.fieldMap().containsKey(expectedHashPath));
  }
}
