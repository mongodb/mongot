package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.KnnVectorFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutoEmbeddingIndexDefinitionUtilsTest {
  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      MAT_VIEW_SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of());

  @BeforeClass
  public static void registerTestModels() {
    // Ensure "voyage-3-large" is present in the EmbeddingModelCatalog for tests that parse
    // VectorAutoEmbedFieldDefinition from BSON or use VectorAutoEmbedFieldSpecification
    // resolution logic.
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
                    Optional.of(VectorAutoEmbedQuantization.FLOAT),
                    Optional.empty()),
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
  public void testGetDerivedVectorIndexDefinition_version0() {

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

    var collectionUuid = UUID.randomUUID();
    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, collectionUuid, MAT_VIEW_SCHEMA_METADATA);

    Assert.assertEquals(collectionUuid, derivedIndexDefinition.getCollectionUuid());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size(), derivedIndexDefinition.getFields().size());

    var defaultVectorField = getVectorFieldDefinition("a", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, defaultVectorField.getType());

    var vectorFieldWithSpecifications = getVectorFieldDefinition("b", derivedIndexDefinition);
    Assert.assertEquals(
        VectorIndexFieldDefinition.Type.VECTOR, vectorFieldWithSpecifications.getType());
    assertDerivedVectorMatchesAutoEmbedSpec(
        autoEmbedFieldWithSpecifications, vectorFieldWithSpecifications);

    var derivedFilterField = getVectorFieldDefinition("color", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilterField.getType());

    Assert.assertEquals(MV_DATABASE_NAME, derivedIndexDefinition.getDatabase());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getIndexId().toHexString(),
        derivedIndexDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedIndexDefinition.getView());
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_version1() {

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

    var collectionUuid = UUID.randomUUID();
    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    Assert.assertEquals(collectionUuid, derivedIndexDefinition.getCollectionUuid());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size(), derivedIndexDefinition.getFields().size());

    var defaultVectorField = getVectorFieldDefinition("_autoEmbed.a", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, defaultVectorField.getType());
    Assert.assertEquals(FieldPath.parse("_autoEmbed.a"), defaultVectorField.getPath());

    var vectorFieldWithSpecifications =
        getVectorFieldDefinition("_autoEmbed.b.a", derivedIndexDefinition);
    Assert.assertEquals(
        VectorIndexFieldDefinition.Type.VECTOR, vectorFieldWithSpecifications.getType());
    Assert.assertEquals(FieldPath.parse("_autoEmbed.b.a"), vectorFieldWithSpecifications.getPath());
    assertDerivedVectorMatchesAutoEmbedSpec(
        autoEmbedFieldWithSpecifications, vectorFieldWithSpecifications);

    var derivedFilterField = getVectorFieldDefinition("color", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilterField.getType());

    Assert.assertEquals(MV_DATABASE_NAME, derivedIndexDefinition.getDatabase());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getIndexId().toHexString(),
        derivedIndexDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedIndexDefinition.getView());
  }

  private VectorIndexFieldDefinition getVectorFieldDefinition(
      String path, VectorIndexDefinition indexDefinition) {
    return indexDefinition.getFields().stream()
        .filter(field -> field.getPath().equals(FieldPath.parse(path)))
        .findFirst()
        .get();
  }

  /**
   * Derived MV {@code vector} fields use plain {@link VectorFieldSpecification}; compare the vector
   * parameters that must match the auto-embed text spec's Lucene quantization.
   */
  private static void assertDerivedVectorMatchesAutoEmbedSpec(
      VectorAutoEmbedFieldDefinition autoEmbed, VectorIndexFieldDefinition derivedVectorField) {
    var src = autoEmbed.specification();
    var der = derivedVectorField.asVectorField().specification();
    Assert.assertEquals(src.numDimensions(), der.numDimensions());
    Assert.assertEquals(src.similarity(), der.similarity());
    Assert.assertEquals(src.quantization(), der.quantization());
    Assert.assertEquals(src.indexingAlgorithm(), der.indexingAlgorithm());
  }

  @Test
  public void testModalityValidation() throws Exception {
    // Test valid modality values (case-insensitive)
    List<String> validModalityValues = List.of("text", "TEXT", "Text", "tExT");
    for (String modalityValue : validModalityValues) {
      var bsonDoc =
          new BsonDocument()
              .append("path", new BsonString("field"))
              .append("model", new BsonString("voyage-3-large"))
              .append("modality", new BsonString(modalityValue))
              .append("similarity", new BsonString("cosine"))
              .append("quantization", new BsonString("float"));

      try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
        var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
        Assert.assertEquals("text", field.specification().modality());
      }
    }

    // Test invalid modality values - use allowUnknownFields because when modality validation
    // fails early, the remaining fields (similarity, quantization) won't be consumed
    List<String> invalidModalityValues = List.of("image", "audio", "video", "", "multimodal");
    for (String modalityValue : invalidModalityValues) {
      var bsonDoc =
          new BsonDocument()
              .append("path", new BsonString("field"))
              .append("model", new BsonString("voyage-3-large"))
              .append("modality", new BsonString(modalityValue))
              .append("similarity", new BsonString("cosine"))
              .append("quantization", new BsonString("float"));

      try (var parser = BsonDocumentParser.fromRoot(bsonDoc).allowUnknownFields(true).build()) {
        var exception =
            Assert.assertThrows(
                BsonParseException.class, () -> VectorAutoEmbedFieldDefinition.fromBson(parser));
        Assert.assertTrue(
            "Expected error message to contain \"must be 'text'\" for modality value: "
                + modalityValue,
            exception.getMessage().contains("must be 'text'"));
      }
    }
  }

  @Test
  public void testGetDerivedVectorIndexDefinitionPreservesNestedRoot() {
    var autoEmbedFieldUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.text"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterUnderNested = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedFieldUnderNested, filterUnderNested);
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(fields).nestedRoot("sections").build();

    Truth.assertThat(autoEmbedIndexDefinition.getNestedRoot()).isPresent();
    Assert.assertEquals(
        FieldPath.parse("sections"), autoEmbedIndexDefinition.getNestedRoot().get());

    var collectionUuid = UUID.randomUUID();
    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, collectionUuid, MAT_VIEW_SCHEMA_METADATA);

    Truth.assertThat(derivedIndexDefinition.getNestedRoot()).isPresent();
    Assert.assertEquals(
        "Derived nestedRoot should be 'sections'",
        FieldPath.parse("sections"),
        derivedIndexDefinition.getNestedRoot().get());
    Assert.assertEquals(
        "Field count should be unchanged",
        autoEmbedIndexDefinition.getFields().size(),
        derivedIndexDefinition.getFields().size());

    var derivedVectorField = getVectorFieldDefinition("sections.text", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, derivedVectorField.getType());
    var derivedFilterField = getVectorFieldDefinition("sections.name", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilterField.getType());
  }

  /**
   * Regression test for CLOUDP-398738: when the source index has a nestedRoot and auto-embed field
   * paths are remapped to the MV namespace (schema version 1), the derived nestedRoot must be
   * remapped to the same namespace so that query-time embedded-vector detection matches the
   * remapped field path.
   */
  @Test
  public void testGetDerivedVectorIndexDefinition_nestedRootRemappedForVersion1() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content")));
    var autoEmbedFieldUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.section_content"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterUnderNested = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedFieldUnderNested, filterUnderNested))
            .nestedRoot("sections")
            .build();

    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    Truth.assertThat(derivedIndexDefinition.getNestedRoot()).isPresent();
    Assert.assertEquals(
        "Derived nestedRoot should be remapped into the MV namespace",
        FieldPath.parse("_autoEmbed.sections"),
        derivedIndexDefinition.getNestedRoot().get());

    // Sanity check: the remapped nestedRoot is now an ancestor of the remapped vector field path,
    // which is the invariant VectorSearchQueryFactory#determineEmbeddedRoot relies on.
    var derivedVectorField =
        getVectorFieldDefinition("_autoEmbed.sections.section_content", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, derivedVectorField.getType());
    Assert.assertTrue(
        "Remapped vector field path must be a descendant of remapped nestedRoot",
        derivedVectorField.getPath().isChildOf(derivedIndexDefinition.getNestedRoot().get()));
  }

  /**
   * CLOUDP-402522: when the schema mapping includes a filter field under the source nestedRoot, the
   * derived index definition must reflect that filter at the remapped MV path. Without this, Lucene
   * indexes filters at the source path while the vector lives at the embedded child level, silently
   * breaking filtered nested $vectorSearch.
   */
  @Test
  public void
      testGetDerivedVectorIndexDefinition_filterUnderNestedRootRemapsToMvNamespace_version1() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));
    var autoEmbedFieldUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.section_content"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    var topLevelFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("name"));
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedFieldUnderNested, nestedFilter, topLevelFilter))
            .nestedRoot("sections")
            .build();

    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            indexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    var derivedFilter =
        getVectorFieldDefinition("_autoEmbed.sections.section_name", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilter.getType());

    var derivedTopLevelFilter = getVectorFieldDefinition("name", derivedIndexDefinition);
    Assert.assertEquals(
        "Top-level filters without a mapping entry must keep their source path.",
        VectorIndexFieldDefinition.Type.FILTER,
        derivedTopLevelFilter.getType());
  }

  @Test
  public void fromBson_withoutHnswOptions_usesDefaults() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertTrue(
          field.specification().indexingAlgorithm()
              instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm);
      var hnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm) field.specification().indexingAlgorithm();
      Assert.assertEquals(
          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_MAX_EDGES,
          hnsw.options().maxEdges());
      Assert.assertEquals(
          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_NUM_EDGE_CANDIDATES,
          hnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void fromBson_withHnswOptions_parsesValues() throws BsonParseException {
    var hnswOptionsDoc =
        new BsonDocument()
            .append("maxEdges", new BsonInt32(32))
            .append("numEdgeCandidates", new BsonInt32(200));
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"))
            .append("hnswOptions", hnswOptionsDoc);

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      var hnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm) field.specification().indexingAlgorithm();
      Assert.assertEquals(32, hnsw.options().maxEdges());
      Assert.assertEquals(200, hnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void fromBson_withPartialHnswOptions_onlyMaxEdges_usesDefaultNumEdgeCandidates()
      throws BsonParseException {
    var hnswOptionsDoc = new BsonDocument().append("maxEdges", new BsonInt32(32));
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"))
            .append("hnswOptions", hnswOptionsDoc);

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      var hnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm) field.specification().indexingAlgorithm();
      Assert.assertEquals(32, hnsw.options().maxEdges());
      Assert.assertEquals(
          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_NUM_EDGE_CANDIDATES,
          hnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void fromBson_withPartialHnswOptions_onlyNumEdgeCandidates_usesDefaultMaxEdges()
      throws BsonParseException {
    var hnswOptionsDoc = new BsonDocument().append("numEdgeCandidates", new BsonInt32(200));
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"))
            .append("hnswOptions", hnswOptionsDoc);

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      var hnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm) field.specification().indexingAlgorithm();
      Assert.assertEquals(
          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_MAX_EDGES,
          hnsw.options().maxEdges());
      Assert.assertEquals(200, hnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void toBson_roundTrip_withHnswOptions_preservesValues() throws BsonParseException {
    var options = new VectorFieldSpecification.HnswOptions(32, 200);
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(options));

    BsonDocument bson = field.toBson();
    Assert.assertTrue(bson.containsKey("hnswOptions"));
    Assert.assertEquals(32, bson.getDocument("hnswOptions").getInt32("maxEdges").getValue());
    Assert.assertEquals(
        200, bson.getDocument("hnswOptions").getInt32("numEdgeCandidates").getValue());

    try (var parser = BsonDocumentParser.fromRoot(bson).allowUnknownFields(true).build()) {
      var roundTripped = VectorAutoEmbedFieldDefinition.fromBson(parser);
      var hnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm)
              roundTripped.specification().indexingAlgorithm();
      Assert.assertEquals(32, hnsw.options().maxEdges());
      Assert.assertEquals(200, hnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void toBson_defaultHnswOptions_omitsHnswOptionsField() {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT);

    BsonDocument bson = field.toBson();
    Assert.assertFalse(bson.containsKey("hnswOptions"));
  }

  @Test
  public void fromBson_withIndexingMethodFlat_usesFlatAlgorithm() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"))
            .append("indexingMethod", new BsonString("flat"));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertTrue(
          field.specification().indexingAlgorithm()
              instanceof VectorIndexingAlgorithm.FlatIndexingAlgorithm);
    }
  }

  @Test
  public void fromBson_withIndexingMethodFlatAndHnswOptions_throwsSemanticError()
      throws BsonParseException {
    var hnswOptionsDoc =
        new BsonDocument()
            .append("maxEdges", new BsonInt32(32))
            .append("numEdgeCandidates", new BsonInt32(200));
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("float"))
            .append("indexingMethod", new BsonString("flat"))
            .append("hnswOptions", hnswOptionsDoc);

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.fail("Expected BsonParseException for indexingMethod flat with hnswOptions");
    } catch (BsonParseException e) {
      // Any semantic parse error is acceptable for this combination (either during parse or close).
    }
  }

  @Test
  public void toBson_roundTrip_withIndexingMethodFlat_preservesValue() throws BsonParseException {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.FlatIndexingAlgorithm());

    BsonDocument bson = field.toBson();
    Assert.assertTrue(bson.containsKey("indexingMethod"));
    Assert.assertEquals("flat", bson.getString("indexingMethod").getValue());

    try (var parser = BsonDocumentParser.fromRoot(bson).allowUnknownFields(true).build()) {
      var roundTripped = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertTrue(
          roundTripped.specification().indexingAlgorithm()
              instanceof VectorIndexingAlgorithm.FlatIndexingAlgorithm);
    }
  }

  @Test
  public void toBson_defaultHnsw_omitsIndexingMethodField() {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT);

    BsonDocument bson = field.toBson();
    Assert.assertFalse(bson.containsKey("indexingMethod"));
  }

  // ======================= Similarity/Quantization Serialization Tests =======================

  @Test
  public void toBson_withSimilarityAndQuantization_includesFields() {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.SCALAR);

    BsonDocument bson = field.toBson();

    Assert.assertTrue("toBson should include similarity field", bson.containsKey("similarity"));
    Assert.assertEquals("dotProduct", bson.getString("similarity").getValue());
    Assert.assertTrue("toBson should include quantization field", bson.containsKey("quantization"));
    Assert.assertEquals("scalar", bson.getString("quantization").getValue());
  }

  @Test
  public void toBson_withDefaultSimilarityAndQuantization_includesFields() {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);

    BsonDocument bson = field.toBson();

    // Even default values should be serialized
    Assert.assertTrue("toBson should include similarity field", bson.containsKey("similarity"));
    Assert.assertEquals("cosine", bson.getString("similarity").getValue());
    Assert.assertTrue("toBson should include quantization field", bson.containsKey("quantization"));
    Assert.assertEquals("float", bson.getString("quantization").getValue());
  }

  @Test
  public void fromBson_withSimilarityAndQuantization_parsesValues() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("dotProduct"))
            .append("quantization", new BsonString(VectorAutoEmbedQuantization.SCALAR.getName()));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(VectorSimilarity.DOT_PRODUCT, field.specification().similarity());
      Assert.assertEquals(VectorQuantization.NONE, field.specification().quantization());
      Assert.assertEquals(
          VectorAutoEmbedQuantization.SCALAR, field.specification().autoEmbedQuantization());
    }
  }

  @Test
  public void fromBson_withEuclideanSimilarity_parsesCorrectly() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("euclidean"))
            .append("quantization", new BsonString("float"));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(VectorSimilarity.EUCLIDEAN, field.specification().similarity());
      Assert.assertEquals(VectorQuantization.NONE, field.specification().quantization());
    }
  }

  @Test
  public void roundTrip_similarityAndQuantization_preservesValues() throws BsonParseException {
    var original =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            1024,
            VectorSimilarity.EUCLIDEAN,
            VectorAutoEmbedQuantization.SCALAR);

    BsonDocument bson = original.toBson();

    try (var parser = BsonDocumentParser.fromRoot(bson).allowUnknownFields(true).build()) {
      var roundTripped = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(
          original.specification().similarity(), roundTripped.specification().similarity());
      Assert.assertEquals(
          original.specification().quantization(), roundTripped.specification().quantization());
      Assert.assertEquals(
          original.specification().autoEmbedQuantization(),
          roundTripped.specification().autoEmbedQuantization());
    }
  }

  @Test
  public void roundTrip_allOptions_preservesAllValues() throws BsonParseException {
    var hnswOptions = new VectorFieldSpecification.HnswOptions(64, 300);
    var original =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("nested.field.path"),
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.SCALAR,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(hnswOptions));

    BsonDocument bson = original.toBson();

    // Verify all fields are present in BSON
    Assert.assertEquals("nested.field.path", bson.getString("path").getValue());
    Assert.assertEquals("voyage-3-large", bson.getString("model").getValue());
    Assert.assertEquals("text", bson.getString("modality").getValue());
    Assert.assertEquals("dotProduct", bson.getString("similarity").getValue());
    Assert.assertEquals("scalar", bson.getString("quantization").getValue());
    Assert.assertTrue(bson.containsKey("hnswOptions"));
    Assert.assertEquals(64, bson.getDocument("hnswOptions").getInt32("maxEdges").getValue());
    Assert.assertEquals(
        300, bson.getDocument("hnswOptions").getInt32("numEdgeCandidates").getValue());

    // Round-trip and verify all values are preserved
    try (var parser = BsonDocumentParser.fromRoot(bson).allowUnknownFields(true).build()) {
      var roundTripped = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(original.getPath(), roundTripped.getPath());
      Assert.assertEquals(
          original.specification().modelName(), roundTripped.specification().modelName());
      Assert.assertEquals(
          original.specification().modality(), roundTripped.specification().modality());
      Assert.assertEquals(
          original.specification().similarity(), roundTripped.specification().similarity());
      Assert.assertEquals(
          original.specification().quantization(), roundTripped.specification().quantization());
      Assert.assertEquals(
          original.specification().autoEmbedQuantization(),
          roundTripped.specification().autoEmbedQuantization());

      var roundTrippedHnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm)
              roundTripped.specification().indexingAlgorithm();
      Assert.assertEquals(64, roundTrippedHnsw.options().maxEdges());
      Assert.assertEquals(300, roundTrippedHnsw.options().numEdgeCandidates());
    }
  }

  @Test
  public void fromBson_withBinaryNoRescoreAndDotProductSimilarity_throwsBsonParse()
      throws Exception {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("dotProduct"))
            .append("quantization", new BsonString("binaryNoRescore"));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class, () -> VectorAutoEmbedFieldDefinition.fromBson(parser));

      Truth.assertThat(e).hasMessageThat().contains("binaryNoRescore");
      Truth.assertThat(e).hasMessageThat().contains("euclidean");
      // The offending similarity must be reported in the user-facing camelCase spelling
      // (e.g. "dotProduct"), not the Java enum constant name ("DOT_PRODUCT").
      Truth.assertThat(e).hasMessageThat().contains("dotProduct");
    }
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_allDefaults_derivesCorrectly() {
    var scalarAutoEmbed =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("emb"));
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(scalarAutoEmbed)).build();
    var derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            indexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), MAT_VIEW_SCHEMA_METADATA);
    var vectorField = getVectorFieldDefinition("emb", derived).asVectorField();
    Assert.assertEquals(VectorQuantization.NONE, vectorField.specification().quantization());
    Assert.assertEquals(1024, vectorField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.DOT_PRODUCT, vectorField.specification().similarity());
    Assert.assertEquals(
        VectorIndexingAlgorithm.AlgorithmType.HNSW,
        vectorField.specification().indexingAlgorithm().type());
    VectorIndexingAlgorithm.HnswIndexingAlgorithm hnsw =
        (VectorIndexingAlgorithm.HnswIndexingAlgorithm)
            vectorField.specification().indexingAlgorithm();
    Assert.assertEquals(
        VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_MAX_EDGES, hnsw.options().maxEdges());
    Assert.assertEquals(
        VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_NUM_EDGE_CANDIDATES,
        hnsw.options().numEdgeCandidates());
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_scalarQuantization_derivesNone() {
    var scalarAutoEmbed =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("emb"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.SCALAR,
            new VectorIndexingAlgorithm.FlatIndexingAlgorithm());
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(scalarAutoEmbed)).build();
    var derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            indexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), MAT_VIEW_SCHEMA_METADATA);
    var vectorField = getVectorFieldDefinition("emb", derived).asVectorField();
    Assert.assertEquals(VectorQuantization.NONE, vectorField.specification().quantization());
    Assert.assertEquals(1024, vectorField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.COSINE, vectorField.specification().similarity());
    Assert.assertEquals(
        VectorIndexingAlgorithm.AlgorithmType.FLAT,
        vectorField.specification().indexingAlgorithm().type());
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_binaryNoRescoreQuantization_unchanged() {
    var binaryAutoEmbed =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("emb"),
            512,
            VectorSimilarity.EUCLIDEAN,
            VectorAutoEmbedQuantization.BINARY_NO_RESCORE);
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(binaryAutoEmbed)).build();
    var derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            indexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), MAT_VIEW_SCHEMA_METADATA);
    var vectorField = getVectorFieldDefinition("emb", derived).asVectorField();
    Assert.assertEquals(VectorQuantization.NONE, vectorField.specification().quantization());
    Assert.assertEquals(512, vectorField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.EUCLIDEAN, vectorField.specification().similarity());
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_binaryQuantization_unchanged() {
    var binaryAutoEmbed =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("emb"),
            256,
            VectorSimilarity.EUCLIDEAN,
            VectorAutoEmbedQuantization.BINARY);
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(binaryAutoEmbed)).build();
    var derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            indexDefinition, MV_DATABASE_NAME, UUID.randomUUID(), MAT_VIEW_SCHEMA_METADATA);
    var vectorField = getVectorFieldDefinition("emb", derived).asVectorField();
    Assert.assertEquals(VectorQuantization.BINARY, vectorField.specification().quantization());
    Assert.assertEquals(256, vectorField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.EUCLIDEAN, vectorField.specification().similarity());
  }

  // ---- Search index derivation tests ----

  @Test
  public void testGetDerivedSearchIndexDefinition() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();

    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    UUID collectionUuid = UUID.randomUUID();
    // path is "content", so schema maps content → _autoEmbed.content
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    Assert.assertEquals(collectionUuid, derivedDefinition.getCollectionUuid());
    Assert.assertEquals(MV_DATABASE_NAME, derivedDefinition.getDatabase());
    Assert.assertEquals(
        rawDefinition.getIndexId().toHexString(),
        derivedDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedDefinition.getView());

    // Field should be remapped from "embedding" into the nested _autoEmbed.content path.
    Assert.assertNull(
        "Original key should not exist in derived definition",
        derivedDefinition.getMappings().fields().get("embedding"));
    Assert.assertNull(
        "Derived mappings must not use a flat dotted key",
        derivedDefinition.getMappings().fields().get("_autoEmbed.content"));
    FieldDefinition remappedField =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.content");
    Assert.assertNotNull("Remapped field should exist", remappedField);
    Assert.assertTrue(
        "Auto-embed field should be replaced with vector field",
        remappedField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(
        "Auto-embed field should be removed from derived definition",
        remappedField.searchAutoEmbedFieldDefinition().isEmpty());

    SearchIndexVectorFieldDefinition vectorField =
        remappedField.searchIndexVectorFieldDefinition().get();
    Assert.assertEquals(
        autoEmbedField.specification().numDimensions(),
        vectorField.specification().numDimensions());
    Assert.assertEquals(
        autoEmbedField.specification().similarity(), vectorField.specification().similarity());

    FieldDefinition activeField = derivedDefinition.getMappings().fields().get("active");
    Assert.assertTrue(
        "Non-auto-embed fields should be preserved",
        activeField.booleanFieldDefinition().isPresent());
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_preservesAutoEmbedVectorConfig() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition(
            FieldPath.parse("content"),
            new VectorAutoEmbedFieldSpecification(
                256,
                VectorSimilarity.COSINE,
                VectorAutoEmbedQuantization.SCALAR,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    new VectorFieldSpecification.HnswOptions(32, 200)),
                "voyage-3-large",
                "text"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    FieldDefinition remappedField =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.content");
    SearchIndexVectorFieldDefinition vectorField =
        remappedField.searchIndexVectorFieldDefinition().orElseThrow();
    Assert.assertEquals(256, vectorField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.COSINE, vectorField.specification().similarity());
    Assert.assertEquals(VectorQuantization.NONE, vectorField.specification().quantization());
    Assert.assertEquals(
        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
            new VectorFieldSpecification.HnswOptions(32, 200)),
        vectorField.specification().indexingAlgorithm());
  }

  @Test
  public void getDerivedSearchIndexDefinition_currentSchema_keepsGeneratedParentStatic() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("content")))
                    .build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    DocumentFieldDefinition autoEmbed =
        derivedDefinition
            .getMappings()
            .fields()
            .get("_autoEmbed")
            .documentFieldDefinition()
            .orElseThrow();
    Truth.assertThat(autoEmbed.dynamic()).isEqualTo(DynamicDefinition.DISABLED);
    Truth.assertThat(
            derivedDefinition
                .createFieldDefinitionResolver(IndexFormatVersion.CURRENT)
                .getFieldDefinition(FieldPath.parse("_autoEmbed.internal"), Optional.empty()))
        .isEmpty();
  }

  @Test
  public void withNestedVectorField_missingLeaf_storesVectorDefinitionAtLeaf() {
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();

    Map<String, FieldDefinition> result =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            Map.of(), FieldPath.parse("content"), DynamicDefinition.DISABLED, vectorField);

    Truth.assertThat(result.get("content").searchIndexVectorFieldDefinition())
        .hasValue(vectorField);
    Truth.assertThat(result.get("content").documentFieldDefinition()).isEmpty();
  }

  @Test
  public void withNestedVectorField_existingLeaf_preservesOtherAlternatives() {
    FieldDefinition existing =
        FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build();
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();

    Map<String, FieldDefinition> result =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            Map.of("content", existing),
            FieldPath.parse("content"),
            DynamicDefinition.DISABLED,
            vectorField);

    Truth.assertThat(result.get("content").booleanFieldDefinition()).isPresent();
    Truth.assertThat(result.get("content").searchIndexVectorFieldDefinition())
        .hasValue(vectorField);
  }

  @Test
  public void withNestedVectorField_existingDocumentLeaf_preservesDocumentAlternative() {
    DocumentFieldDefinition existingDocument =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();

    Map<String, FieldDefinition> result =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            Map.of("content", FieldDefinitionBuilder.builder().document(existingDocument).build()),
            FieldPath.parse("content"),
            DynamicDefinition.DISABLED,
            vectorField);

    FieldDefinition content = result.get("content");
    Truth.assertThat(content.documentFieldDefinition()).hasValue(existingDocument);
    Truth.assertThat(content.searchIndexVectorFieldDefinition()).hasValue(vectorField);
  }

  @Test
  public void withNestedVectorField_existingParent_preservesDynamicAndSibling() {
    DocumentFieldDefinition existingParent =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();

    Map<String, FieldDefinition> result =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            Map.of("parent", FieldDefinitionBuilder.builder().document(existingParent).build()),
            FieldPath.parse("parent.content"),
            DynamicDefinition.DISABLED,
            vectorField);

    DocumentFieldDefinition updatedParent =
        result.get("parent").documentFieldDefinition().orElseThrow();
    Truth.assertThat(updatedParent.dynamic().isEnabled()).isTrue();
    Truth.assertThat(updatedParent.fields().get("active").booleanFieldDefinition()).isPresent();
    Truth.assertThat(updatedParent.fields().get("content").searchIndexVectorFieldDefinition())
        .hasValue(vectorField);
  }

  @Test
  public void withNestedVectorField_sharedIntermediate_preservesBothLeaves() {
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();
    Map<String, FieldDefinition> withContent =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            Map.of(),
            FieldPath.parse("_autoEmbed.parent.content"),
            DynamicDefinition.DISABLED,
            vectorField);

    Map<String, FieldDefinition> result =
        AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
            withContent,
            FieldPath.parse("_autoEmbed.parent.summary"),
            DynamicDefinition.DISABLED,
            vectorField);

    DocumentFieldDefinition autoEmbed =
        result.get("_autoEmbed").documentFieldDefinition().orElseThrow();
    DocumentFieldDefinition parent =
        autoEmbed.fields().get("parent").documentFieldDefinition().orElseThrow();
    Truth.assertThat(parent.fields()).containsKey("content");
    Truth.assertThat(parent.fields()).containsKey("summary");
    Truth.assertThat(parent.fields().get("content").searchIndexVectorFieldDefinition())
        .hasValue(vectorField);
    Truth.assertThat(parent.fields().get("summary").searchIndexVectorFieldDefinition())
        .hasValue(vectorField);
  }

  @Test
  public void withNestedVectorField_existingKnnVector_throws() {
    FieldDefinition existing =
        FieldDefinitionBuilder.builder()
            .knnVector(new KnnVectorFieldDefinition(512, VectorSimilarity.COSINE))
            .build();
    SearchIndexVectorFieldDefinition vectorField = createSearchVectorFieldDefinition();

    IllegalStateException exception =
        Assert.assertThrows(
            IllegalStateException.class,
            () ->
                AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
                    Map.of("content", existing),
                    FieldPath.parse("content"),
                    DynamicDefinition.DISABLED,
                    vectorField));

    Truth.assertThat(exception).hasMessageThat().contains("content");
  }

  @Test
  public void withNestedVectorField_existingSearchVector_throws() {
    SearchIndexVectorFieldDefinition existingVector = createSearchVectorFieldDefinition();
    FieldDefinition existing =
        FieldDefinitionBuilder.builder().searchIndexVectorField(existingVector).build();

    IllegalStateException exception =
        Assert.assertThrows(
            IllegalStateException.class,
            () ->
                AutoEmbeddingIndexDefinitionUtils.withNestedVectorField(
                    Map.of("content", existing),
                    FieldPath.parse("content"),
                    DynamicDefinition.DISABLED,
                    createSearchVectorFieldDefinition()));

    Truth.assertThat(exception).hasMessageThat().contains("content");
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_multipleAutoEmbedFields() {
    SearchAutoEmbedFieldDefinition autoEmbedField1 =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    SearchAutoEmbedFieldDefinition autoEmbedField2 =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("summary"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "contentEmbedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField1).build())
            .field(
                "summaryEmbedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField2).build())
            .build();

    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    UUID collectionUuid = UUID.randomUUID();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L,
            Map.of(
                FieldPath.parse("content"),
                FieldPath.parse("_autoEmbed.content"),
                FieldPath.parse("summary"),
                FieldPath.parse("_autoEmbed.summary")));
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    FieldDefinition contentField =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.content");
    Assert.assertNotNull(contentField);
    Assert.assertTrue(contentField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(contentField.searchAutoEmbedFieldDefinition().isEmpty());

    FieldDefinition summaryField =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.summary");
    Assert.assertNotNull(summaryField);
    Assert.assertTrue(summaryField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(summaryField.searchAutoEmbedFieldDefinition().isEmpty());
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_nestedAutoEmbedHoistedToMatViewPath() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    // The nested auto-embed field is hoisted to its nested materialized-view path.
    FieldDefinition hoisted =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.content");
    Assert.assertNotNull("Nested auto-embed should hoist to _autoEmbed.content", hoisted);
    Assert.assertTrue(hoisted.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(hoisted.searchAutoEmbedFieldDefinition().isEmpty());

    // The containing document is preserved with its non-auto-embed child, minus the auto-embed
    // field.
    FieldDefinition parent = derivedDefinition.getMappings().fields().get("parent");
    Assert.assertNotNull("Parent document should be preserved", parent);
    DocumentFieldDefinition parentDoc = parent.documentFieldDefinition().get();
    Assert.assertNotNull(
        "Sibling non-auto-embed field should be preserved", parentDoc.fields().get("active"));
    Assert.assertNull(
        "Nested auto-embed field should be removed from its document",
        parentDoc.fields().get("embedding"));
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_dynamicDocumentKeepsDynamicMapping() {
    // Dynamic document containing an auto-embed: the derived document keeps dynamic mapping (its
    // wholesale-copied MV subtree is dynamically indexed), keeps declared children, drops the
    // embed declaration, and the embed hoists to its nested materialized-view path.
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("parent.content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L,
            Map.of(
                FieldPath.parse("parent.content"), FieldPath.parse("_autoEmbed.parent.content")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    FieldDefinition hoisted =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.parent.content");
    Assert.assertNotNull("Auto-embed should hoist to its materialized-view path", hoisted);
    Assert.assertTrue(hoisted.searchIndexVectorFieldDefinition().isPresent());

    DocumentFieldDefinition parentDoc =
        derivedDefinition.getMappings().fields().get("parent").documentFieldDefinition().get();
    Assert.assertTrue("Derived document keeps dynamic mapping", parentDoc.dynamic().isEnabled());
    Assert.assertNotNull("Declared sibling should be preserved", parentDoc.fields().get("active"));
    Assert.assertNull("Embed declaration should be removed", parentDoc.fields().get("embedding"));
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_emptyDocumentsUnderDynamicMappingPreserved() {
    // dynamic article > static meta > only an embed. Both documents become empty after hoisting
    // but must survive: `article` so its wholesale-copied MV subtree is dynamically indexed,
    // `meta` so its static mapping keeps suppressing dynamic indexing of its subtree.
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("article.meta.summary"));
    DocumentFieldDefinition meta =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "summary_embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    DocumentFieldDefinition article =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("meta", FieldDefinitionBuilder.builder().document(meta).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("article", FieldDefinitionBuilder.builder().document(article).build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L,
            Map.of(
                FieldPath.parse("article.meta.summary"),
                FieldPath.parse("_autoEmbed.article.meta.summary")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    Assert.assertNotNull(
        getStructurallyNestedField(
            derivedDefinition.getMappings(), "_autoEmbed.article.meta.summary"));
    DocumentFieldDefinition articleDoc =
        derivedDefinition.getMappings().fields().get("article").documentFieldDefinition().get();
    Assert.assertTrue("Dynamic document survives empty", articleDoc.dynamic().isEnabled());
    DocumentFieldDefinition metaDoc =
        articleDoc.fields().get("meta").documentFieldDefinition().get();
    Assert.assertFalse(metaDoc.dynamic().isEnabled());
    Assert.assertTrue(
        "Static document under dynamic mapping survives empty to suppress dynamic descent",
        metaDoc.fields().isEmpty());
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_scalarAndDocumentAlternativesPreserved() {
    // A field declared as both a scalar and a document containing an auto-embed keeps the scalar
    // alternative and the rebuilt document (embed removed) at its original path.
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("text")))
                    .build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(nested)
                    .bool(new BooleanFieldDefinition())
                    .build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("text"), FieldPath.parse("_autoEmbed.text")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    FieldDefinition parent = derivedDefinition.getMappings().fields().get("parent");
    Assert.assertNotNull(parent);
    Assert.assertTrue(
        "Scalar alternative should be preserved", parent.booleanFieldDefinition().isPresent());
    DocumentFieldDefinition parentDoc = parent.documentFieldDefinition().get();
    Assert.assertNotNull(parentDoc.fields().get("active"));
    Assert.assertNull(parentDoc.fields().get("embedding"));
    Assert.assertNotNull(
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.text"));
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_autoEmbedAndDocumentAlternatives() {
    // A field declared as both an auto-embed and a document with its own nested auto-embed: both
    // embeds hoist to pure vector fields (no unprocessed document subtree on the hoisted field),
    // and the field keeps its rebuilt document alternative.
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "nested_embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("deep")))
                    .build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "combo",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("top")))
                    .document(nested)
                    .build())
            .build();
    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L,
            Map.of(
                FieldPath.parse("top"), FieldPath.parse("_autoEmbed.top"),
                FieldPath.parse("deep"), FieldPath.parse("_autoEmbed.deep")));

    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);

    FieldDefinition hoistedTop =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.top");
    Assert.assertNotNull(hoistedTop);
    Assert.assertTrue(hoistedTop.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(
        "Hoisted vector field must not carry the document alternative",
        hoistedTop.documentFieldDefinition().isEmpty());
    FieldDefinition hoistedDeep =
        getStructurallyNestedField(derivedDefinition.getMappings(), "_autoEmbed.deep");
    Assert.assertNotNull("Nested embed inside the document alternative must hoist", hoistedDeep);
    Assert.assertTrue(hoistedDeep.searchIndexVectorFieldDefinition().isPresent());

    FieldDefinition combo = derivedDefinition.getMappings().fields().get("combo");
    Assert.assertNotNull(combo);
    Assert.assertTrue(combo.searchAutoEmbedFieldDefinition().isEmpty());
    DocumentFieldDefinition comboDoc = combo.documentFieldDefinition().get();
    Assert.assertNotNull(comboDoc.fields().get("active"));
    Assert.assertNull(comboDoc.fields().get("nested_embedding"));
  }

  @Test
  public void testGetDerivedSearchIndexDefinition_preservesIndexMetadata() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();

    SearchIndexDefinition rawDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .analyzerName("lucene.standard")
            .build();

    UUID collectionUuid = UUID.randomUUID();
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    Assert.assertEquals(rawDefinition.getIndexId(), derivedDefinition.getIndexId());
    Assert.assertEquals(rawDefinition.getName(), derivedDefinition.getName());
    Assert.assertEquals(rawDefinition.getNumPartitions(), derivedDefinition.getNumPartitions());
    Assert.assertEquals(rawDefinition.getAnalyzerName(), derivedDefinition.getAnalyzerName());
    Assert.assertEquals(
        rawDefinition.getParsedIndexFeatureVersion(),
        derivedDefinition.getParsedIndexFeatureVersion());
  }

  private static FieldDefinition getStructurallyNestedField(
      DocumentFieldDefinition mappings, String dottedPath) {
    List<String> segments = ImmutableList.copyOf(FieldPath.parse(dottedPath).getSegments());
    @Var DocumentFieldDefinition currentDocument = mappings;
    for (int i = 0; i < segments.size() - 1; i++) {
      String segment = segments.get(i);
      FieldDefinition parent = currentDocument.fields().get(segment);
      Assert.assertNotNull("Missing derived document segment " + segment, parent);
      currentDocument = parent.documentFieldDefinition().orElseThrow();
    }
    String leaf = segments.getLast();
    FieldDefinition field = currentDocument.fields().get(leaf);
    Assert.assertNotNull("Missing derived mapping path " + dottedPath, field);
    return field;
  }

  private static SearchIndexVectorFieldDefinition createSearchVectorFieldDefinition() {
    return new SearchIndexVectorFieldDefinition(
        new VectorFieldSpecification(
            512,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
  }
}
