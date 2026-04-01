package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;
import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;

import com.google.common.truth.Truth;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
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
import org.junit.Test;

public class AutoEmbeddingIndexDefinitionUtilsTest {
  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      MAT_VIEW_SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of());

  @Test
  public void testGetDerivedVectorIndexDefinition_version0() {

    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
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
    Assert.assertEquals(
        autoEmbedFieldWithSpecifications.specification(),
        vectorFieldWithSpecifications.asVectorField().specification());

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
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
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
    Assert.assertEquals(
        autoEmbedFieldWithSpecifications.specification(),
        vectorFieldWithSpecifications.asVectorField().specification());

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
              .append("quantization", new BsonString("none"));

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
              .append("quantization", new BsonString("none"));

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
  public void testGetMatViewIndexFields_version0() {
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            autoEmbedIndexDefinition.getMappings(), MAT_VIEW_SCHEMA_METADATA);

    // 2 auto-embed fields, so 2 additional hash field entries.
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewFieldMapping.fieldMap().size());
    Assert.assertTrue(
        matViewFieldMapping.fieldMap().containsKey(FieldPath.parse("a" + HASH_FIELD_SUFFIX)));
    Assert.assertTrue(
        matViewFieldMapping.fieldMap().containsKey(FieldPath.parse("b" + HASH_FIELD_SUFFIX)));
  }

  @Test
  public void testGetMatViewIndexFields_version1() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("b.a"),
                FieldPath.parse("_autoEmbed.b.a"),
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a")));
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            autoEmbedIndexDefinition.getMappings(), schemaMetadata);

    // 2 auto-embed fields, so 2 additional hash field entries.
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewFieldMapping.fieldMap().size());
    Assert.assertTrue(
        matViewFieldMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".b.a")));
    Assert.assertTrue(
        matViewFieldMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".a")));
  }

  @Test
  public void testGetHashFieldPathRootLevel() {
    Assert.assertEquals(
        "a" + HASH_FIELD_SUFFIX,
        AutoEmbeddingIndexDefinitionUtils.getHashFieldPath(FieldPath.parse("a"), 0).toString());
  }

  @Test
  public void testGetHashFieldPathNested() {
    Assert.assertEquals(
        "a.b.c" + HASH_FIELD_SUFFIX,
        AutoEmbeddingIndexDefinitionUtils.getHashFieldPath(FieldPath.parse("a.b.c"), 0).toString());
  }

  @Test
  public void testGetDerivedVectorIndexDefinitionPreservesNestedRoot() {
    var autoEmbedFieldUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.text"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
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

  @Test
  public void fromBson_withoutHnswOptions_usesDefaults() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("cosine"))
            .append("quantization", new BsonString("none"));

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
            .append("quantization", new BsonString("none"))
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
            .append("quantization", new BsonString("none"))
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
            .append("quantization", new BsonString("none"))
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
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(options));

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
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());

    BsonDocument bson = field.toBson();
    Assert.assertFalse(bson.containsKey("hnswOptions"));
  }

  @Test
  public void getMatViewIndexFields_preservesHnswOptions() {
    var hnswOptions = new VectorFieldSpecification.HnswOptions(32, 200);
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("a"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.of(hnswOptions));
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedField, filterField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            indexDefinition.getMappings(), MAT_VIEW_SCHEMA_METADATA);

    var matViewAutoEmbedField =
        matViewFieldMapping.fieldMap().values().stream()
            .filter(f -> f.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .findFirst()
            .get()
            .asVectorAutoEmbedField();
    Assert.assertTrue(
        matViewAutoEmbedField.specification().indexingAlgorithm()
            instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm);
    var preservedHnsw =
        (VectorIndexingAlgorithm.HnswIndexingAlgorithm)
            matViewAutoEmbedField.specification().indexingAlgorithm();
    Assert.assertEquals(32, preservedHnsw.options().maxEdges());
    Assert.assertEquals(200, preservedHnsw.options().numEdgeCandidates());
  }

  @Test
  public void getMatViewIndexFieldsPreservesNestedRootAndHashPaths() {
    var autoEmbedUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.embedding"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());
    var filterUnderNested = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedUnderNested, filterUnderNested);
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(fields).nestedRoot("sections").build();

    var rawMapping = autoEmbedIndexDefinition.getMappings();
    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            rawMapping, MAT_VIEW_SCHEMA_METADATA);

    Assert.assertTrue(
        "Mat view mapping should preserve nestedRoot", matViewFieldMapping.hasNestedRoot());
    Assert.assertEquals(FieldPath.parse("sections"), matViewFieldMapping.nestedRoot().get());

    FieldPath nestedAutoEmbedPath = FieldPath.parse("sections.embedding");
    FieldPath expectedHashPath = FieldPath.parse("sections.embedding" + HASH_FIELD_SUFFIX);
    Assert.assertTrue(
        "Should contain original auto-embed path",
        matViewFieldMapping.fieldMap().containsKey(nestedAutoEmbedPath));
    Assert.assertTrue(
        "Should contain hash path for nested auto-embed field",
        matViewFieldMapping.fieldMap().containsKey(expectedHashPath));
  }

  // ======================= Similarity/Quantization Serialization Tests =======================

  @Test
  public void toBson_withSimilarityAndQuantization_includesFields() {
    var field =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.SCALAR,
            Optional.empty());

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
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            Optional.empty());

    BsonDocument bson = field.toBson();

    // Even default values should be serialized
    Assert.assertTrue("toBson should include similarity field", bson.containsKey("similarity"));
    Assert.assertEquals("cosine", bson.getString("similarity").getValue());
    Assert.assertTrue("toBson should include quantization field", bson.containsKey("quantization"));
    Assert.assertEquals("none", bson.getString("quantization").getValue());
  }

  @Test
  public void fromBson_withSimilarityAndQuantization_parsesValues() throws BsonParseException {
    var bsonDoc =
        new BsonDocument()
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-3-large"))
            .append("modality", new BsonString("text"))
            .append("similarity", new BsonString("dotProduct"))
            .append("quantization", new BsonString("scalar"));

    try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
      var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(VectorSimilarity.DOT_PRODUCT, field.specification().similarity());
      Assert.assertEquals(VectorQuantization.SCALAR, field.specification().quantization());
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
            .append("quantization", new BsonString("none"));

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
            VectorSimilarity.EUCLIDEAN,
            VectorQuantization.SCALAR,
            Optional.empty());

    BsonDocument bson = original.toBson();

    try (var parser = BsonDocumentParser.fromRoot(bson).allowUnknownFields(true).build()) {
      var roundTripped = VectorAutoEmbedFieldDefinition.fromBson(parser);
      Assert.assertEquals(
          original.specification().similarity(), roundTripped.specification().similarity());
      Assert.assertEquals(
          original.specification().quantization(), roundTripped.specification().quantization());
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
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.SCALAR,
            Optional.of(hnswOptions));

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

      var roundTrippedHnsw =
          (VectorIndexingAlgorithm.HnswIndexingAlgorithm)
              roundTripped.specification().indexingAlgorithm();
      Assert.assertEquals(64, roundTrippedHnsw.options().maxEdges());
      Assert.assertEquals(300, roundTrippedHnsw.options().numEdgeCandidates());
    }
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
    // sourceField is "content", so schema maps content → _autoEmbed.content
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1L,
            Map.of(
                FieldPath.parse("content"),
                FieldPath.parse("_autoEmbed.content")));
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    Assert.assertEquals(collectionUuid, derivedDefinition.getCollectionUuid());
    Assert.assertEquals(MV_DATABASE_NAME, derivedDefinition.getDatabase());
    Assert.assertEquals(
        rawDefinition.getIndexId().toHexString(),
        derivedDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedDefinition.getView());

    // Field should be remapped from "embedding" key to "_autoEmbed.content" key
    Assert.assertNull(
        "Original key should not exist in derived definition",
        derivedDefinition.getMappings().fields().get("embedding"));
    FieldDefinition remappedField =
        derivedDefinition.getMappings().fields().get("_autoEmbed.content");
    Assert.assertNotNull("Remapped field should exist", remappedField);
    Assert.assertTrue(
        "Auto-embed field should be replaced with vector field",
        remappedField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(
        "Auto-embed field should be removed from derived definition",
        remappedField.searchAutoEmbedFieldDefinition().isEmpty());

    SearchIndexVectorFieldDefinition vectorField =
        remappedField.searchIndexVectorFieldDefinition().get();
    Assert.assertEquals(autoEmbedField.specification(), vectorField.specification());

    FieldDefinition activeField = derivedDefinition.getMappings().fields().get("active");
    Assert.assertTrue(
        "Non-auto-embed fields should be preserved",
        activeField.booleanFieldDefinition().isPresent());
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
        derivedDefinition.getMappings().fields().get("_autoEmbed.content");
    Assert.assertNotNull(contentField);
    Assert.assertTrue(contentField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(contentField.searchAutoEmbedFieldDefinition().isEmpty());

    FieldDefinition summaryField =
        derivedDefinition.getMappings().fields().get("_autoEmbed.summary");
    Assert.assertNotNull(summaryField);
    Assert.assertTrue(summaryField.searchIndexVectorFieldDefinition().isPresent());
    Assert.assertTrue(summaryField.searchAutoEmbedFieldDefinition().isEmpty());
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
            1L,
            Map.of(
                FieldPath.parse("content"),
                FieldPath.parse("_autoEmbed.content")));
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
}
