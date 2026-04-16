package com.xgen.mongot.embedding.utils;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.compareDocuments;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.computeTextHash;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.getVectorTextPathMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class AutoEmbeddingDocumentUtilsTest {

  private static final MaterializedViewSchemaMetadata MAT_VIEW_SCHEMA_METADATA =
      new MaterializedViewSchemaMetadata(0, Map.of());

  @Test
  public void testGetTextValues() throws IOException {
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("b")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    Map<FieldPath, Set<String>> result =
        getVectorTextPathMap(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("a"), Set.of("aString"), FieldPath.parse("b"), Set.of("bString")),
        result);
  }

  @Test
  public void testGetEmbeddedTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.newRoot("dot.field")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc = createEmbeddedBson();

    // also test that "." in field name works
    bsonDoc.append("dot.field", new BsonString("dotString"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result =
        getVectorTextPathMap(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("root.a"),
            Set.of("aString"),
            FieldPath.parse("root.b"),
            Set.of("bString"),
            FieldPath.newRoot("dot.field"),
            Set.of("dotString")),
        result);
  }

  @Test
  public void testGetArrayTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.a")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result =
        getVectorTextPathMap(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("root"),
            Set.of("arrayString1", "arrayString2"),
            FieldPath.parse("root.a"),
            Set.of("aString", "aString2")),
        result);
  }

  @Test
  public void testGetIgnoredTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.parse("d")),
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("num")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc = createArrayBson();
    bsonDoc.append("d", new BsonString("dString"));
    bsonDoc.append("num", new BsonDouble(1.23));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result =
        getVectorTextPathMap(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));
    assertEquals(
        Map.of(
            FieldPath.parse("d"),
            Set.of("dString"),
            FieldPath.parse("root.b"),
            Set.of("bString"),
            FieldPath.parse("root"),
            Set.of("arrayString1", "arrayString2")),
        result);
  }

  @Test
  public void testBuildDocumentEvent() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("b")),
            new VectorTextFieldDefinition(FieldPath.parse("c")),
            new VectorTextFieldDefinition(FieldPath.parse("extra")),
            new VectorTextFieldDefinition(FieldPath.parse("num")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("extra", new BsonString("no-embedding"));
    bsonDoc.append("num", new BsonDouble(1.23));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef),
            vectorDef.getMappings().fieldMap().keySet().stream()
                .map(fieldPath -> new AbstractMap.SimpleEntry<>(fieldPath, embeddings))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            ImmutableMap.of(
                FieldPath.parse("a"),
                ImmutableMap.of("aString", embeddings.get("aString")),
                FieldPath.parse("b"),
                ImmutableMap.of("bString", embeddings.get("bString")),
                FieldPath.parse("c"),
                ImmutableMap.of(), // no matched vector in embeddings
                FieldPath.parse("extra"),
                ImmutableMap.of() // no matched vector in embeddings
                )),
        result);
  }

  @Test
  public void testBuildDocumentEvent_noop() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorTextFieldDefinition(FieldPath.parse("a")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    DocumentEvent rawDocumentEvent = DocumentEvent.createDelete(new BsonInt32(1));
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef),
            ImmutableMap.of(FieldPath.parse("a"), embeddings));
    assertEquals(rawDocumentEvent, result);
  }

  @Test
  public void testBuildDocumentEvent_embeddedDoc() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.newRoot("dot.field")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createEmbeddedBson();

    // also test that "." in field name works
    bsonDoc.append("dot.field", new BsonString("fString"));

    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef),
            ImmutableMap.of(
                FieldPath.parse("root.a"),
                embeddings,
                FieldPath.parse("root.b"),
                embeddings,
                FieldPath.newRoot("dot.field"),
                embeddings));

    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> perDocEmbeddings =
        ImmutableMap.of(
            FieldPath.parse("root.a"),
            ImmutableMap.of("aString", embeddings.get("aString")),
            FieldPath.parse("root.b"),
            ImmutableMap.of("bString", embeddings.get("bString")),
            FieldPath.newRoot("dot.field"),
            ImmutableMap.of("fString", embeddings.get("fString")));
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(rawDocumentEvent, perDocEmbeddings),
        result);
  }

  @Test
  public void testBuildDocumentEvent_array() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorTextFieldDefinition(FieldPath.parse("root")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef),
            ImmutableMap.of(FieldPath.parse("root"), embeddings));
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            ImmutableMap.of(
                FieldPath.parse("root"),
                ImmutableMap.of(
                    "arrayString1",
                    embeddings.get("arrayString1"),
                    "arrayString2",
                    embeddings.get("arrayString2")))),
        result);
  }

  @Test
  public void testBuildDocumentEvent_arrayEmbeddedDoc() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> allEmbeddingsFromBatchResponse =
        ImmutableMap.of(
            FieldPath.parse("root.b"), embeddings, FieldPath.parse("root.a"), embeddings);
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef),
            allEmbeddingsFromBatchResponse);
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            // Expects no entry for FieldPath.parse("root"), since allEmbeddingsFromBatchResponse
            // has no vector for it.
            ImmutableMap.of(
                // Expects no aString entry in FieldPath.parse("root.a"), since no results in
                // allEmbeddingsFromBatchResponse
                FieldPath.parse("root.a"), ImmutableMap.of("aString", embeddings.get("aString")),
                FieldPath.parse("root.b"), ImmutableMap.of("bString", embeddings.get("bString")))),
        result);
  }

  @Test
  public void testBuildMaterializedViewDocumentEvent_version0() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a.b")
            .withAutoEmbedField("b.d")
            .withAutoEmbedField("extra")
            .withAutoEmbedField("num")
            .withFilterPath("a")
            .withFilterPath("b.c")
            .withFilterPath("color")
            .build();

    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc =
        BsonDocument.parse(
            "{"
                + "\"a\": [\"arrayString1\", \"arrayString2\", {\"b\": \"bString\"}], "
                + "\"b\": {\"c\":\"cString\", \"d\": [\"arrayString1\", \"arrayString2\"]}, "
                + "\"color\": [\"red\", \"blue\"], "
                + "\"extra\": \"no-embedding\", "
                + "\"num\": 1.23, "
                + "\"_id\": \"anId\""
                + "}");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    assertEquals(
        new BsonDocument()
            .append(
                "a",
                new BsonArray(
                    List.of(
                        new BsonString("arrayString1"),
                        new BsonString("arrayString2"),
                        new BsonDocument("b", BsonVectorParser.encode(embeddings.get("bString"))),
                        new BsonDocument("b_hash", new BsonString(computeTextHash("bString"))))))
            .append(
                "b",
                new BsonDocument("c", new BsonString("cString"))
                    .append("d", BsonVectorParser.encode(embeddings.get("arrayString1")))
                    .append("d_hash", new BsonString(computeTextHash("arrayString1"))))
            .append("color", new BsonArray(List.of(new BsonString("red"), new BsonString("blue")))),
        result.getDocument().get());

    assertEquals(rawDocumentEvent.getDocumentId(), result.getDocumentId());
  }

  @Test
  public void testBuildMaterializedViewDocumentEvent_version1() throws IOException {
    // Setup: a document with auto-embed field that has multiple text values (array scenario)
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b.c")
            .withFilterPath("b")
            .build();
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("a"), FieldPath.parse("_autoEmbed.a"),
                FieldPath.parse("b.c"), FieldPath.parse("_autoEmbed.b.c")));

    // Create vectors
    Vector vector1 = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector2Old =
        Vector.fromFloats(new float[] {3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector2New =
        Vector.fromFloats(new float[] {9.0f, 10.0f}, FloatVector.OriginalType.NATIVE);

    // Create document with text field
    BsonDocument bsonDoc =
        BsonDocument.parse(
            "{"
                + "\"a\": \"text2\", "
                + "\"b\": [\"bString\", {\"c\":\"text1\"}], "
                + "\"_id\": \"anId\""
                + "}");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Create a DocumentEvent with existing embeddings for "text1" and "text2"
    DocumentEvent rawEventWithOldEmbeddings =
        DocumentEvent.createFromDocumentEventAndVectors(
            DocumentEvent.createInsert(
                DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc),
            ImmutableMap.of(
                FieldPath.parse("a"), ImmutableMap.of("text1", vector1, "text2", vector2Old)));

    // Provide new embeddings only for "text2" (updated), not "text1" (reuse old)
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> newEmbeddings =
        ImmutableMap.of(
            FieldPath.parse("a"),
            ImmutableMap.of("text2", vector2New),
            FieldPath.parse("b.c"),
            ImmutableMap.of("text1", vector1));

    // Build materialized view document event - the consolidated map should have:
    // "text2" -> vector2New (from new), "text1" -> vector1 (from old, since not in new)
    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEventWithOldEmbeddings,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            newEmbeddings,
            schemaMetadata);

    // The result document should use vector1 for "text1" since that's what's in the document
    BsonDocument resultDoc = result.getDocument().get().clone();
    assertEquals(
        new BsonDocument("b", new BsonArray(List.of(new BsonString("bString"))))
            .append(
                "_autoEmbed",
                new BsonDocument("a", BsonVectorParser.encode(vector2New))
                    .append(
                        "_hash",
                        new BsonDocument("a", new BsonString(computeTextHash("text2")))
                            .append(
                                "b",
                                new BsonDocument("c", new BsonString(computeTextHash("text1")))))
                    .append("b", new BsonDocument("c", BsonVectorParser.encode(vector1)))),
        resultDoc);
  }

  @Test
  public void testBuildMaterializedViewDocumentEvent_withExistingEmbeddings_prefersNewEmbeddings()
      throws IOException {
    // Setup: a document with auto-embed field "a"
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("a").build();

    // Create vectors for old and new embeddings
    Vector oldVector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    Vector newVector =
        Vector.fromFloats(new float[] {9.0f, 10.0f}, FloatVector.OriginalType.NATIVE);

    // Create document with text field
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("text"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Create a DocumentEvent with existing embeddings for "text"
    DocumentEvent rawEventWithOldEmbeddings =
        DocumentEvent.createFromDocumentEventAndVectors(
            DocumentEvent.createInsert(
                DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc),
            ImmutableMap.of(FieldPath.parse("a"), ImmutableMap.of("text", oldVector)));

    // Provide new embeddings for the same "text"
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> newEmbeddings =
        ImmutableMap.of(FieldPath.parse("a"), ImmutableMap.of("text", newVector));

    // Build materialized view document event
    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEventWithOldEmbeddings,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            newEmbeddings,
            MAT_VIEW_SCHEMA_METADATA);

    // Verify the new vector is used (not the old one)
    BsonDocument resultDoc = result.getDocument().get().clone();
    Vector resultVector = BsonVectorParser.parse(resultDoc.getBinary("a"));
    assertThat(resultVector).isEqualTo(newVector);
  }

  @Test
  public void testBuildMaterializedViewDocumentEvent_withExistingEmbeddings_usesOldWhenNoNew()
      throws IOException {
    // Setup: a document with auto-embed fields "a" and "b"
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .build();

    // Create vectors
    Vector vectorA = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    Vector vectorB = Vector.fromFloats(new float[] {5.0f, 6.0f}, FloatVector.OriginalType.NATIVE);

    // Create document with two text fields
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("textA"))
            .append("b", new BsonString("textB"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Create a DocumentEvent with existing embeddings for both fields
    DocumentEvent rawEventWithOldEmbeddings =
        DocumentEvent.createFromDocumentEventAndVectors(
            DocumentEvent.createInsert(
                DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc),
            ImmutableMap.of(
                FieldPath.parse("a"), ImmutableMap.of("textA", vectorA),
                FieldPath.parse("b"), ImmutableMap.of("textB", vectorB)));

    // Provide new embeddings only for field "a", not "b"
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> newEmbeddings =
        ImmutableMap.of(FieldPath.parse("a"), ImmutableMap.of("textA", vectorA));

    // Build materialized view document event
    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEventWithOldEmbeddings,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            newEmbeddings,
            MAT_VIEW_SCHEMA_METADATA);

    // Verify both fields have embeddings - "a" from new, "b" from old
    BsonDocument resultDoc = result.getDocument().get().clone();
    assertThat(resultDoc.containsKey("a")).isTrue();
    assertThat(resultDoc.containsKey("b")).isTrue();
    assertThat(BsonVectorParser.parse(resultDoc.getBinary("a"))).isEqualTo(vectorA);
    assertThat(BsonVectorParser.parse(resultDoc.getBinary("b"))).isEqualTo(vectorB);
  }

  @Test
  public void testNeedsReIndexing_DocumentsMatch_version0() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_DocumentsMatch_version1() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b.c")
            .withAutoEmbedField("c.d")
            .withAutoEmbedField("c.f")
            .withFilterPath("color")
            .build();
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a"),
                FieldPath.parse("b.c"),
                FieldPath.parse("_autoEmbed.b.c"),
                FieldPath.parse("c.d"),
                FieldPath.parse("_autoEmbed.c.d"),
                FieldPath.parse("c.f"),
                FieldPath.parse("_autoEmbed.c.f")));

    AutoEmbedFieldMapping matViewMappingsWithHash =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            vectorIndexDefinition, schemaMetadata);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayEmbededBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            schemaMetadata);
    var comparisonResult =
        compareDocuments(
            rawBsonDoc,
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            matViewMappingsWithHash,
            schemaMetadata);

    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(3, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertFalse(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b.c")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("c.d")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("c.f")));
    assertEquals(
        Set.of("arrayString1"),
        comparisonResult.reusableEmbeddings().get(FieldPath.parse("c.d")).keySet());
  }

  @Test
  public void testCompareDocumentsEmptyStringField() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("c").build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBsonWithGivenString("");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(0, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsNonEmptyToEmptyStringField() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("c").build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    @Var BsonDocument bsonDoc = createBsonWithGivenString("aString");
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    @Var
    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());

    // Update the source collection doc to have an empty string.
    bsonDoc = createBsonWithGivenString("");
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(0, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsEmptyStringFieldInArray() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("root.a").build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBsonWithGivenString("");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsNonEmptyToEmptyStringFieldInArray() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("root.a").build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    @Var BsonDocument bsonDoc = createArrayBsonWithGivenString("aString");
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    // Update one of the array fields to an empty string.
    bsonDoc = createArrayBsonWithGivenString("");
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect false as the first vector string in "root.a" is unchanged, we only changed the second
    // vector text, but should be ignored by Lucene.
    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testNeedsReIndexing_EmbeddingsMismatch() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withAutoEmbedField("c")
            .withAutoEmbedField("extra")
            .withAutoEmbedField("num")
            .withFilterPath("color")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("extra", new BsonString("no-embedding"));
    bsonDoc.append("num", new BsonDouble(1.23));
    bsonDoc.append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since only 2 of the 3 auto-embedding fields have embeddings in the mat
    // view.
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_FilterAddition() throws IOException {
    VectorIndexDefinition vectorIndexDefinition1 =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("color", new BsonString("red"));
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition1),
            createEmbeddingsPerField(vectorIndexDefinition1.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    // add a new filter field and update the source collection doc.
    VectorIndexDefinition vectorIndexDefinition2 =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .withFilterPath("size")
            .build();
    bsonDoc.append("size", new BsonString("large"));
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition2),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition2, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since filter fields dont match
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_FilterRemoval() throws IOException {
    VectorIndexDefinition vectorIndexDefinition1 =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .withFilterPath("size")
            .build();

    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("color", new BsonString("red"));
    bsonDoc.append("size", new BsonString("large"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition1),
            createEmbeddingsPerField(vectorIndexDefinition1.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    // remove a filter field and update the source collection doc.
    VectorIndexDefinition vectorIndexDefinition2 =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition2),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition2, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since filter fields dont match
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_LeaseVersionInMatView_version0() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    // Simulate MaterializedViewWriter fencing: inject _autoEmbed._leaseVersion into the MV doc.
    // Decode to a mutable BsonDocument since RawBsonDocument is immutable.
    BsonDocument matViewDoc = new BsonDocument();
    matViewDoc.putAll(result.getDocument().get());
    BsonDocument autoEmbedDoc =
        matViewDoc.containsKey("_autoEmbed")
            ? matViewDoc.getDocument("_autoEmbed")
            : new BsonDocument();
    autoEmbedDoc.put("_leaseVersion", new BsonInt64(42));
    matViewDoc.put("_autoEmbed", autoEmbedDoc);
    RawBsonDocument matViewRawDoc = new RawBsonDocument(matViewDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            matViewRawDoc,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // _leaseVersion is a metadata field and should NOT trigger re-indexing.
    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testNeedsReIndexing_LeaseVersionInMatView_version1() throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a"),
                FieldPath.parse("b"),
                FieldPath.parse("_autoEmbed.b")));

    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            schemaMetadata);

    // Simulate MaterializedViewWriter fencing: inject _autoEmbed._leaseVersion into the MV doc.
    // Decode to a mutable BsonDocument since RawBsonDocument is immutable.
    BsonDocument matViewDoc = new BsonDocument();
    matViewDoc.putAll(result.getDocument().get());
    BsonDocument autoEmbedDoc = matViewDoc.getDocument("_autoEmbed");
    autoEmbedDoc.put("_leaseVersion", new BsonInt64(42));

    RawBsonDocument matViewRawDoc = new RawBsonDocument(matViewDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            matViewRawDoc,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, schemaMetadata),
            schemaMetadata);

    // _leaseVersion is a metadata field and should NOT trigger re-indexing.
    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testNeedsReIndexing_UnknownMetadataFieldInMatView_triggersReIndexing()
      throws IOException {
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            createEmbeddingsPerField(vectorIndexDefinition.getMappings(), embeddings),
            MAT_VIEW_SCHEMA_METADATA);

    // Inject an unknown field that is NOT in MV_METADATA_FIELDS.
    BsonDocument matViewDoc = new BsonDocument();
    matViewDoc.putAll(result.getDocument().get());
    matViewDoc.put("_unknownFutureField", new BsonString("someValue"));
    RawBsonDocument matViewRawDoc = new RawBsonDocument(matViewDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            matViewRawDoc,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition),
            AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
                vectorIndexDefinition, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Unknown fields SHOULD trigger re-indexing — we must not over-exclude.
    assertTrue(comparisonResult.needsReIndexing());
  }

  @Test
  public void testMvMetadataFieldsContainsLeaseVersionField() {
    // Ensure MV_METADATA_FIELDS tracks _autoEmbed._leaseVersion (written by
    // MaterializedViewWriter). The enforcer test in MaterializedViewWriterTest verifies the full
    // writer → compareDocuments path. This test catches removal of the field from the set.
    assertThat(AutoEmbeddingDocumentUtils.MV_METADATA_FIELDS).contains("_autoEmbed._leaseVersion");
  }

  private BsonDocument createBasicBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc
        .append("_id", new BsonString("anId"))
        .append("a", new BsonString("aString"))
        .append("b", new BsonString("bString"))
        .append("c", new BsonString("cString"));
    return bsonDoc;
  }

  private BsonDocument createArrayEmbededBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc
        .append("_id", new BsonString("anId"))
        .append("a", new BsonString("aString"))
        .append("b", new BsonDocument("c", new BsonString("cString")))
        .append(
            "c",
            new BsonArray(
                List.of(
                    new BsonDocument("d", new BsonString("arrayString1")),
                    new BsonDocument("d", new BsonString("arrayString2")),
                    new BsonDocument("f", new BsonString("fString")))))
        .append("color", new BsonArray(List.of(new BsonString("red"), new BsonString("blue"))));
    return bsonDoc;
  }

  private BsonDocument createBsonWithGivenString(String value) {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("_id", new BsonString("anId")).append("c", new BsonString(value));
    return bsonDoc;
  }

  private ImmutableMap<String, Vector> createEmbeddings() {
    Vector vector1 = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector2 = Vector.fromFloats(new float[] {5.0f, 6.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector3 = Vector.fromFloats(new float[] {9.0f, 10.0f}, FloatVector.OriginalType.NATIVE);
    var embeddings = new ImmutableMap.Builder<String, Vector>();
    embeddings.put("aString", vector1);
    embeddings.put("bString", vector2);
    embeddings.put("fString", vector3);
    embeddings.put("arrayString1", vector1);
    embeddings.put("arrayString2", vector2);
    return embeddings.build();
  }

  /**
   * Creates per-field embeddings map from flat embeddings. Each field gets the same embeddings map.
   */
  private ImmutableMap<FieldPath, ImmutableMap<String, Vector>> createEmbeddingsPerField(
      VectorIndexFieldMapping mappings, ImmutableMap<String, Vector> embeddings) {
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> builder = ImmutableMap.builder();
    for (FieldPath fieldPath : mappings.fieldMap().keySet()) {
      builder.put(fieldPath, embeddings);
    }
    return builder.build();
  }

  private BsonDocument createEmbeddedBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("_id", new BsonString("anId"));
    bsonDoc.append("root", createBasicBson());
    return bsonDoc;
  }

  private BsonDocument createArrayBson() {
    BsonArray bsonArray = new BsonArray();
    bsonArray.add(createBasicBson());
    bsonArray.add(createBasicBson().append("a", new BsonString("aString2")));
    bsonArray.add(new BsonString("arrayString1"));
    bsonArray.add(new BsonString("arrayString2"));
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("root", bsonArray);
    bsonDoc.append("_id", new BsonString("anId"));
    return bsonDoc;
  }

  private BsonDocument createArrayBsonWithGivenString(String value) {
    BsonArray bsonArray = new BsonArray();
    bsonArray.add(createBasicBson());
    bsonArray.add(createBasicBson().append("a", new BsonString(value)));
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("root", bsonArray);
    bsonDoc.append("_id", new BsonString("anId"));
    return bsonDoc;
  }

  // Tests for requiresEmbeddingGeneration and extractFilterFieldValues

  @Test
  public void testRequiresEmbeddingGeneration_OnlyFilterFieldsUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with only filter field updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("color", new BsonString("blue")));

    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testRequiresEmbeddingGeneration_AutoEmbedFieldUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with auto-embed field updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("text", new BsonString("new text")));

    assertTrue(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testRequiresEmbeddingGeneration_BothFieldsUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with both fields updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null,
            new BsonDocument("text", new BsonString("new text"))
                .append("color", new BsonString("blue")));

    assertTrue(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testRequiresEmbeddingGeneration_AutoEmbedFieldRemoved() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with auto-embed field removed
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(List.of("text"), null);

    assertTrue(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testRequiresEmbeddingGeneration_FilterFieldRemoved() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with filter field removed
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(List.of("color"), null);

    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testRequiresEmbeddingGeneration_UnrelatedFieldChanged() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // UpdateDescription with unrelated field updated (not in index at all)
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("unrelated", new BsonString("value")));

    // No AUTO_EMBED/TEXT fields were changed, so embedding is not required.
    // The unrelated field is not in the index, so we don't need to regenerate embeddings.
    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
            updateDescription, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef)));
  }

  @Test
  public void testExtractFilterFieldValues_SingleFilterField() throws IOException {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // Document with filter field
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    assertEquals(new BsonDocument("color", new BsonString("red")), result);
  }

  @Test
  public void testExtractFilterFieldValues_MultipleFilterFields() throws IOException {
    // Setup: index with auto-embed field "text" and multiple filter fields
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("size")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // Document with multiple filter fields
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"))
            .append("size", new BsonString("large"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    assertEquals(
        new BsonDocument("color", new BsonString("red")).append("size", new BsonString("large")),
        result);
  }

  @Test
  public void testExtractFilterFieldValues_MissingFilterField() throws IOException {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    // Document without filter field
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // Should return empty document when filter field is missing
    assertEquals(new BsonDocument(), result);
  }

  @Test
  public void extractFilterFieldValues_arrayFilterField_preservesArrayStructure()
      throws IOException {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append(
                "tags",
                new BsonArray(
                    List.of(
                        new BsonString("red"), new BsonString("blue"), new BsonString("green"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // Assert
    assertThat(result)
        .isEqualTo(
            new BsonDocument(
                "tags",
                new BsonArray(
                    List.of(
                        new BsonString("red"), new BsonString("blue"), new BsonString("green")))));
  }

  @Test
  public void extractFilterFieldValues_singleElementArray_preservesArrayNotScalar() {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("tags", new BsonArray(List.of(new BsonString("red"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // Assert
    assertThat(result)
        .isEqualTo(new BsonDocument("tags", new BsonArray(List.of(new BsonString("red")))));
  }

  @Test
  public void extractFilterFieldValues_mixedScalarAndArray_handlesBothCorrectly()
      throws IOException {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"))
            .append(
                "tags", new BsonArray(List.of(new BsonString("large"), new BsonString("sale"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // Assert
    assertThat(result)
        .isEqualTo(
            new BsonDocument("color", new BsonString("red"))
                .append(
                    "tags",
                    new BsonArray(List.of(new BsonString("large"), new BsonString("sale")))));
  }

  // Tests for addBsonValueToBsonDocument

  @Test
  public void addBsonValueToBsonDocument_createsNewPaths() {
    // Case 1: Simple field path (no parent) - adds value directly at leaf
    BsonDocument doc1 = new BsonDocument();
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc1, FieldPath.parse("fieldName"), new BsonString("value"));
    assertThat(doc1).isEqualTo(new BsonDocument("fieldName", new BsonString("value")));

    // Case 2: Nested path - creates intermediate BsonDocuments
    BsonDocument doc2 = new BsonDocument();
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc2, FieldPath.parse("level1.level2.leaf"), new BsonInt32(42));
    assertThat(doc2)
        .isEqualTo(
            new BsonDocument(
                "level1", new BsonDocument("level2", new BsonDocument("leaf", new BsonInt32(42)))));

    // Case 3: Multiple fields under same parent - navigates into existing document
    BsonDocument doc3 = new BsonDocument();
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc3, FieldPath.parse("parent.field1"), new BsonString("value1"));
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc3, FieldPath.parse("parent.field2"), new BsonString("value2"));
    assertThat(doc3)
        .isEqualTo(
            new BsonDocument(
                "parent",
                new BsonDocument("field1", new BsonString("value1"))
                    .append("field2", new BsonString("value2"))));
  }

  @Test
  public void addBsonValueToBsonDocument_handlesExistingParentTypes() {
    // Case 1: Parent exists as BsonDocument - navigates into it
    BsonDocument doc1 = new BsonDocument("parent", new BsonDocument("existing", new BsonInt32(1)));
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc1, FieldPath.parse("parent.newField"), new BsonString("newValue"));
    assertThat(doc1)
        .isEqualTo(
            new BsonDocument(
                "parent",
                new BsonDocument("existing", new BsonInt32(1))
                    .append("newField", new BsonString("newValue"))));

    // Case 2: Parent exists as BsonArray - adds new BsonDocument to the array
    BsonArray existingArray = new BsonArray(List.of(new BsonString("existingItem")));
    BsonDocument doc2 = new BsonDocument("parent", existingArray);
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc2, FieldPath.parse("parent.newField"), new BsonString("arrayNestedValue"));
    assertThat(existingArray.size()).isEqualTo(2);
    assertThat(existingArray.get(0)).isEqualTo(new BsonString("existingItem"));
    assertThat(existingArray.get(1).asDocument().getString("newField").getValue())
        .isEqualTo("arrayNestedValue");

    // Case 3: Parent exists as primitive - converts to BsonArray with old value + new doc
    BsonDocument doc3 = new BsonDocument("a", new BsonString("primitiveValue"));
    AutoEmbeddingDocumentUtils.addBsonValueToBsonDocument(
        doc3, FieldPath.parse("a.b"), new BsonString("nestedValue"));
    assertThat(doc3.get("a").isArray()).isTrue();
    BsonArray array = doc3.getArray("a");
    assertThat(array.size()).isEqualTo(2);
    assertThat(array.get(0)).isEqualTo(new BsonString("primitiveValue"));
    assertThat(array.get(1).asDocument().getString("b").getValue()).isEqualTo("nestedValue");
  }
}
