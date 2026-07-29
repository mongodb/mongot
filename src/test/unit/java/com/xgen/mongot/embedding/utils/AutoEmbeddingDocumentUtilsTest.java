package com.xgen.mongot.embedding.utils;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.compareDocuments;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.computeTextHash;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.getVectorTextPathMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
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
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
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

  /** Static root; `parent` is a dynamic document containing an auto-embed declaration. */
  private static SearchIndexDefinition dynamicParentAutoEmbedSearchDef() {
    DocumentFieldDefinition parent =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("parent.content")))
                    .build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static final MaterializedViewSchemaMetadata DYNAMIC_PARENT_SCHEMA_METADATA =
      new MaterializedViewSchemaMetadata(
          1,
          Map.of(FieldPath.parse("parent.content"), FieldPath.parse("_autoEmbed.parent.content")));

  @Test
  public void buildMaterializedViewDocumentEvent_dynamicDocumentWithAutoEmbed_copiesWholesale()
      throws IOException {
    // The MV must carry the dynamic document's declared AND undeclared children (the derived
    // index maps them dynamically), while the embed source is owned by the embed pipeline
    // (vector + hash, no raw copy).
    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(dynamicParentAutoEmbedSearchDef());
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument bsonDoc =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"red\"}}");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            mapping,
            ImmutableMap.of(FieldPath.parse("parent.content"), ImmutableMap.of("embed me", vector)),
            DYNAMIC_PARENT_SCHEMA_METADATA);

    BsonDocument mvDoc = result.getDocument().get().clone();
    assertEquals(
        new BsonDocument(
                "parent",
                new BsonDocument("active", BsonBoolean.TRUE).append("color", new BsonString("red")))
            .append(
                "_autoEmbed",
                new BsonDocument(
                        "parent", new BsonDocument("content", BsonVectorParser.encode(vector)))
                    .append(
                        "_hash",
                        new BsonDocument(
                            "parent",
                            new BsonDocument(
                                "content", new BsonString(computeTextHash("embed me")))))),
        mvDoc);
  }

  @Test
  public void compareDocuments_dynamicDocumentWithAutoEmbed_freshMvComparesClean()
      throws IOException {
    // Round-trip: an MV document produced by the wholesale dynamic-document walk compares clean
    // against its source (embedding reusable); a changed embed source text compares stale.
    SearchIndexDefinition searchDef = dynamicParentAutoEmbedSearchDef();
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            searchDef, DYNAMIC_PARENT_SCHEMA_METADATA);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument bsonDoc =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"red\"}}");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                rawEvent,
                mapping,
                ImmutableMap.of(
                    FieldPath.parse("parent.content"), ImmutableMap.of("embed me", vector)),
                DYNAMIC_PARENT_SCHEMA_METADATA)
            .getDocument()
            .get();

    var fresh =
        compareDocuments(
            rawBsonDoc, mvDoc, mapping, matViewMapping, DYNAMIC_PARENT_SCHEMA_METADATA);
    assertFalse("Fresh MV doc must not be stale", fresh.needsReIndexing());
    assertTrue(
        "Embedding must be reusable",
        fresh.reusableEmbeddings().containsKey(FieldPath.parse("parent.content")));

    BsonDocument changedDoc =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"different\", \"color\": \"red\"}}");
    RawBsonDocument changedRaw = new RawBsonDocument(changedDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    var stale =
        compareDocuments(
            changedRaw, mvDoc, mapping, matViewMapping, DYNAMIC_PARENT_SCHEMA_METADATA);
    assertTrue("Changed embed source text must be stale", stale.needsReIndexing());
  }

  @Test
  public void compareDocuments_dynamicDocument_changedDynamicFieldIsStale() throws IOException {
    // A non-embed field changed inside the wholesale dynamic subtree while the embed text stayed
    // the same. The per-path loop cannot see it (no enumerable entry); projection equality must
    // flag it — while the unchanged embedding stays reusable, so the rebuild costs no embedding
    // call.
    SearchIndexDefinition searchDef = dynamicParentAutoEmbedSearchDef();
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            searchDef, DYNAMIC_PARENT_SCHEMA_METADATA);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"red\"}}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(
                    FieldPath.parse("parent.content"), ImmutableMap.of("embed me", vector)),
                DYNAMIC_PARENT_SCHEMA_METADATA)
            .getDocument()
            .get();

    BsonDocument changed =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"blue\"}}");
    RawBsonDocument changedRaw = new RawBsonDocument(changed, BsonUtils.BSON_DOCUMENT_CODEC);

    var result =
        compareDocuments(
            changedRaw, mvDoc, mapping, matViewMapping, DYNAMIC_PARENT_SCHEMA_METADATA);
    assertTrue("Changed field in the dynamic subtree must be stale", result.needsReIndexing());
    assertTrue(
        "Unchanged embedding must still be reusable",
        result.reusableEmbeddings().containsKey(FieldPath.parse("parent.content")));
  }

  @Test
  public void compareDocuments_rootDynamic_changedUndeclaredFieldIsStale() throws IOException {
    // Root-dynamic mode has the same open-ended passthroughs; projection equality must detect a
    // changed undeclared field there too (previously invisible: fieldMap() is embed-only in
    // dynamic mode).
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
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"content\": \"embed me\", \"category\": \"books\"}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(FieldPath.parse("content"), ImmutableMap.of("embed me", vector)),
                schemaMetadata)
            .getDocument()
            .get();

    var fresh = compareDocuments(originalRaw, mvDoc, mapping, matViewMapping, schemaMetadata);
    assertFalse("Fresh MV doc must not be stale", fresh.needsReIndexing());

    BsonDocument changed =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"content\": \"embed me\", \"category\": \"films\"}");
    RawBsonDocument changedRaw = new RawBsonDocument(changed, BsonUtils.BSON_DOCUMENT_CODEC);
    var stale = compareDocuments(changedRaw, mvDoc, mapping, matViewMapping, schemaMetadata);
    assertTrue(
        "Changed undeclared field under dynamic root must be stale", stale.needsReIndexing());
  }

  @Test
  public void compareDocuments_search_fencedLeaseVersionComparesClean() throws IOException {
    // Fencing injects _autoEmbed._leaseVersion into real MV docs; the projection comparison must
    // strip MV metadata so it never causes spurious re-indexing (search-mapping counterpart of
    // the vector enforcer test in MaterializedViewWriterTest).
    SearchIndexDefinition searchDef = dynamicParentAutoEmbedSearchDef();
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            searchDef, DYNAMIC_PARENT_SCHEMA_METADATA);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"red\"}}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    BsonDocument mvMutable =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(
                    FieldPath.parse("parent.content"), ImmutableMap.of("embed me", vector)),
                DYNAMIC_PARENT_SCHEMA_METADATA)
            .getDocument()
            .get()
            .decode(BsonUtils.BSON_DOCUMENT_CODEC);
    mvMutable.getDocument("_autoEmbed").append("_leaseVersion", new BsonInt64(42));
    RawBsonDocument fencedMvDoc = new RawBsonDocument(mvMutable, BsonUtils.BSON_DOCUMENT_CODEC);

    var result =
        compareDocuments(
            originalRaw, fencedMvDoc, mapping, matViewMapping, DYNAMIC_PARENT_SCHEMA_METADATA);
    assertFalse("MV metadata must not cause spurious re-indexing", result.needsReIndexing());
  }

  @Test
  public void compareDocuments_search_staticDeclaredFilterChangeIsStale() throws IOException {
    // Static-mode declared passthroughs were previously compared per-path; projection equality
    // must preserve that coverage.
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("content")))
                    .build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse("{\"_id\": \"anId\", \"content\": \"embed me\", \"active\": true}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(FieldPath.parse("content"), ImmutableMap.of("embed me", vector)),
                schemaMetadata)
            .getDocument()
            .get();

    BsonDocument changed =
        BsonDocument.parse("{\"_id\": \"anId\", \"content\": \"embed me\", \"active\": false}");
    RawBsonDocument changedRaw = new RawBsonDocument(changed, BsonUtils.BSON_DOCUMENT_CODEC);
    var result = compareDocuments(changedRaw, mvDoc, mapping, matViewMapping, schemaMetadata);
    assertTrue("Changed declared filter value must be stale", result.needsReIndexing());
  }

  @Test
  public void compareDocuments_dynamicDocument_embedSourceUnderArray_comparesClean()
      throws IOException {
    // Embed source under an array inside the wholesale dynamic document: the MV writer places
    // vector and hash document-nested under _autoEmbed regardless of arrays on the source side,
    // so the projection comparison strips them and a fresh MV compares clean (no re-index loop).
    DocumentFieldDefinition parent =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "items_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("parent.items.text")))
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("parent.items.text"),
                FieldPath.parse("_autoEmbed.parent.items.text")));
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": {\"color\": \"red\", \"items\":"
                + " [{\"text\": \"embed me\", \"tag\": 1}, {\"text\": \"\", \"tag\": 2}]}}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(
                    FieldPath.parse("parent.items.text"), ImmutableMap.of("embed me", vector)),
                schemaMetadata)
            .getDocument()
            .get();

    var fresh = compareDocuments(originalRaw, mvDoc, mapping, matViewMapping, schemaMetadata);
    assertFalse("Fresh MV doc must not be stale", fresh.needsReIndexing());
    assertTrue(
        "Embedding must be reusable",
        fresh.reusableEmbeddings().containsKey(FieldPath.parse("parent.items.text")));
  }

  @Test
  public void compareDocuments_search_schemaVersion0_freshMvComparesClean() throws IOException {
    // Schema v0 places vectors and hashes at source-relative paths, exercising the
    // decode-and-strip branch of the projection comparison.
    SearchIndexDefinition searchDef = dynamicParentAutoEmbedSearchDef();
    MaterializedViewSchemaMetadata schemaMetadata = new MaterializedViewSchemaMetadata(0, Map.of());
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);
    Vector vector = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    BsonDocument original =
        BsonDocument.parse(
            "{\"_id\": \"anId\", \"parent\": "
                + "{\"active\": true, \"content\": \"embed me\", \"color\": \"red\"}}");
    RawBsonDocument originalRaw = new RawBsonDocument(original, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mvDoc =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(originalRaw)), originalRaw),
                mapping,
                ImmutableMap.of(
                    FieldPath.parse("parent.content"), ImmutableMap.of("embed me", vector)),
                schemaMetadata)
            .getDocument()
            .get();

    var fresh = compareDocuments(originalRaw, mvDoc, mapping, matViewMapping, schemaMetadata);
    assertFalse("Fresh v0 MV doc must not be stale", fresh.needsReIndexing());
  }

  @Test
  public void buildMaterializedViewDocumentEvent_idSubdocumentFilter_omitsIdFromMatViewDoc()
      throws IOException {
    // Regression for CLOUDP-412237. When the index declares a filter on a subfield of _id (e.g.
    // _id.ProfileId) and the source document's _id is a subdocument with additional sibling
    // fields, the MaterializedViewDocumentHandler used to descend into _id and copy only the
    // declared subfield, producing an MV doc with a truncated _id. The MV writer's
    // ReplaceOneModel filter is {_id: <full source _id>}, so the replacement _id no longer
    // matched and MongoDB rejected the write as a non-retryable alteration of the immutable _id
    // field, FAILing the index during initial sync.
    //
    // The MV doc must therefore never carry _id: it is owned by the upsert filter, which already
    // carries the full source _id (subdocument and all).
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("statement.Text")
            .withFilterPath("_id.ProfileId")
            .build();
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append(
                "_id",
                new BsonDocument("ProfileId", new BsonString("P1"))
                    .append("Tenant", new BsonString("T1")))
            .append("statement", new BsonDocument("Text", new BsonString("aString")));
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

    BsonDocument mvDoc = result.getDocument().get();
    assertFalse(
        "MV doc must not include _id; the upsert filter owns it. Found: " + mvDoc.get("_id"),
        mvDoc.containsKey("_id"));
    // The documentId on the event must still carry the full source _id so the upsert filter can
    // address the MV row correctly.
    assertEquals(
        new BsonDocument("ProfileId", new BsonString("P1")).append("Tenant", new BsonString("T1")),
        result.getDocumentId());
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

  // ---- CLOUDP-428208: removing a filter field nested under a subdocument ----
  // The removed-field scan walks the MV document through the current mapping, so it cannot reach a
  // stale value under a subdocument that no longer has any mapped field. These cover each shape a
  // removed filter can have; testNeedsReIndexing_FilterRemoval above covers the top-level shape.

  /**
   * Removing a filter nested under a subdocument must re-index so the stale value is pruned. Here
   * the surviving auto-embed field {@code awards.text} is remapped to {@code
   * _autoEmbed.awards.text}, so once {@code awards.wins} is dropped nothing maps under the
   * source-shaped {@code awards} subdocument.
   */
  @Test
  public void compareDocuments_removedNestedFilter_triggersReindex() throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("awards.text"), FieldPath.parse("_autoEmbed.awards.text")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("awards.text"));
    var nestedFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("awards.wins"));
    VectorIndexDefinition withFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .build();
    VectorIndexDefinition withoutFilter =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(autoEmbedField)).build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append(
                "awards",
                new BsonDocument()
                    .append("text", new BsonString("winText"))
                    .append("wins", new BsonInt32(1)));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        buildMatViewDoc(
            rawSource,
            withFilter,
            "awards.text",
            List.of("winText"),
            schemaMetadata,
            Optional.empty());

    assertTrue(
        "MV row must carry the nested filter value before removal",
        matViewDoc.getDocument("awards").containsKey("wins"));
    assertFalse(
        "Control: filter still declared must not re-index",
        compare(rawSource, matViewDoc, withFilter, schemaMetadata, Optional.empty())
            .needsReIndexing());
    assertTrue(
        "Removing a nested filter must re-index to prune the stale value",
        compare(rawSource, matViewDoc, withoutFilter, schemaMetadata, Optional.empty())
            .needsReIndexing());
  }

  /** Same as above but two levels deep, to show the fix is not specific to one nesting level. */
  @Test
  public void compareDocuments_removedDeeplyNestedFilter_triggersReindex() throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("a.b.text"), FieldPath.parse("_autoEmbed.a.b.text")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("a.b.text"));
    var deepFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("a.b.wins"));
    VectorIndexDefinition withFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, deepFilter))
            .build();
    VectorIndexDefinition withoutFilter =
        VectorIndexDefinitionBuilder.builder().setFields(List.of(autoEmbedField)).build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append(
                "a",
                new BsonDocument()
                    .append(
                        "b",
                        new BsonDocument()
                            .append("text", new BsonString("t1"))
                            .append("wins", new BsonInt32(7))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        buildMatViewDoc(
            rawSource, withFilter, "a.b.text", List.of("t1"), schemaMetadata, Optional.empty());

    assertTrue(
        "MV row must carry the deeply nested filter value before removal",
        matViewDoc.getDocument("a").getDocument("b").containsKey("wins"));
    assertFalse(
        "Control: filter still declared must not re-index",
        compare(rawSource, matViewDoc, withFilter, schemaMetadata, Optional.empty())
            .needsReIndexing());
    assertTrue(
        "Removing a filter two levels deep must re-index",
        compare(rawSource, matViewDoc, withoutFilter, schemaMetadata, Optional.empty())
            .needsReIndexing());
  }

  /**
   * A filter OUTSIDE a declared {@code nestedRoot} is stored source-shaped, exactly like the
   * non-nested case, so removing it must re-index. Guards against suppressing the projection check
   * for every index that merely declares a nestedRoot.
   */
  @Test
  public void compareDocuments_nestedRoot_removedFilterOutsideNestedRoot_triggersReindex()
      throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(FieldPath.parse("sections.text"), FieldPath.parse("_autoEmbed.sections.text")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("sections.text"));
    var outsideFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("metadata.category"));
    VectorIndexDefinition withFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, outsideFilter))
            .nestedRoot("sections")
            .build();
    VectorIndexDefinition withoutFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField))
            .nestedRoot("sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(List.of(new BsonDocument().append("text", new BsonString("c1")))))
            .append("metadata", new BsonDocument().append("category", new BsonString("sci-fi")));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        buildMatViewDoc(
            rawSource,
            withFilter,
            "sections.text",
            List.of("c1"),
            schemaMetadata,
            withFilter.getNestedRoot());

    assertTrue(
        "MV row must carry the source-shaped filter outside nestedRoot",
        matViewDoc.containsKey("metadata"));
    assertFalse(
        "Control: filter still declared must not re-index",
        compare(rawSource, matViewDoc, withFilter, schemaMetadata, withFilter.getNestedRoot())
            .needsReIndexing());
    assertTrue(
        "Removing a filter outside nestedRoot must re-index to prune the stale value",
        compare(rawSource, matViewDoc, withoutFilter, schemaMetadata, withoutFilter.getNestedRoot())
            .needsReIndexing());
  }

  /**
   * A filter INSIDE {@code nestedRoot} is co-located under the remapped {@code _autoEmbed} root,
   * where the per-path scan already detects its removal. Locks that in so the projection check --
   * which deliberately ignores the {@code _autoEmbed} namespace -- is not relied on for this shape.
   */
  @Test
  public void compareDocuments_nestedRoot_removedFilterInsideNestedRoot_triggersReindex()
      throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.text"),
                FieldPath.parse("_autoEmbed.sections.text"),
                FieldPath.parse("sections.name"),
                FieldPath.parse("_autoEmbed.sections.name")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("sections.text"));
    var insideFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    VectorIndexDefinition withFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, insideFilter))
            .nestedRoot("sections")
            .build();
    VectorIndexDefinition withoutFilter =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField))
            .nestedRoot("sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("text", new BsonString("c1"))
                            .append("name", new BsonString("n1")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        buildMatViewDoc(
            rawSource,
            withFilter,
            "sections.text",
            List.of("c1"),
            schemaMetadata,
            withFilter.getNestedRoot());

    assertTrue(
        "MV row must carry the filter co-located under the remapped nestedRoot before removal",
        matViewDoc
            .getDocument("_autoEmbed")
            .getArray("sections")
            .get(0)
            .asDocument()
            .containsKey("name"));
    assertFalse(
        "Control: filter still declared must not re-index",
        compare(rawSource, matViewDoc, withFilter, schemaMetadata, withFilter.getNestedRoot())
            .needsReIndexing());
    assertTrue(
        "Removing a filter inside nestedRoot must re-index to prune the stale value",
        compare(rawSource, matViewDoc, withoutFilter, schemaMetadata, withoutFilter.getNestedRoot())
            .needsReIndexing());
  }

  /**
   * No-spurious-reindex guard for the projection comparison on a nestedRoot index that has filters
   * both inside and outside the nestedRoot. An unchanged document must not be re-indexed: the
   * inside filter is stored both source-shaped and remapped under {@code _autoEmbed}, and the
   * comparison strips only the {@code _autoEmbed} copy, so the source-shaped copy is present on
   * both sides and compares equal. A regression here would re-index every document of every nested
   * index on each resync.
   */
  @Test
  public void compareDocuments_nestedRoot_unchangedDocWithInsideAndOutsideFilters_noReindex()
      throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.text"),
                FieldPath.parse("_autoEmbed.sections.text"),
                FieldPath.parse("sections.name"),
                FieldPath.parse("_autoEmbed.sections.name")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("sections.text"));
    var insideFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    var outsideFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("metadata.category"));
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, insideFilter, outsideFilter))
            .nestedRoot("sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("text", new BsonString("c1"))
                            .append("name", new BsonString("n1")),
                        // Second element intentionally omits the filter: a sparse nested array must
                        // not make the projection comparison disagree either.
                        new BsonDocument().append("text", new BsonString("c2")))))
            .append("metadata", new BsonDocument().append("category", new BsonString("sci-fi")));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        buildMatViewDoc(
            rawSource,
            definition,
            "sections.text",
            List.of("c1", "c2"),
            schemaMetadata,
            definition.getNestedRoot());

    assertFalse(
        "An unchanged nested document must not be re-indexed",
        compare(rawSource, matViewDoc, definition, schemaMetadata, definition.getNestedRoot())
            .needsReIndexing());
  }

  /**
   * Builds the MV document for {@code definition}, embedding every text in {@code embedTexts} under
   * {@code embedPath}. Every embeddable text in the source must be supplied, otherwise the MV is
   * genuinely missing an embedding and comparison correctly reports it as stale.
   */
  private static RawBsonDocument buildMatViewDoc(
      RawBsonDocument source,
      VectorIndexDefinition definition,
      String embedPath,
      List<String> embedTexts,
      MaterializedViewSchemaMetadata schemaMetadata,
      Optional<FieldPath> nestedRoot)
      throws IOException {
    ImmutableMap.Builder<String, Vector> vectors = ImmutableMap.builder();
    for (int i = 0; i < embedTexts.size(); i++) {
      vectors.put(
          embedTexts.get(i),
          Vector.fromFloats(
              new float[] {0.1f * (i + 1), 0.2f * (i + 1)}, FloatVector.OriginalType.NATIVE));
    }
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(source)), source);
    return new RawBsonDocument(
        buildMaterializedViewDocumentEvent(
                event,
                AutoEmbedFieldMappingCreator.createAutoEmbedMapping(definition),
                ImmutableMap.of(FieldPath.parse(embedPath), vectors.build()),
                schemaMetadata,
                nestedRoot)
            .getDocument()
            .get(),
        BsonUtils.BSON_DOCUMENT_CODEC);
  }

  /** Runs compareDocuments for {@code definition} against an existing MV document. */
  private static AutoEmbeddingDocumentUtils.DocumentComparisonResult compare(
      RawBsonDocument source,
      RawBsonDocument matViewDoc,
      VectorIndexDefinition definition,
      MaterializedViewSchemaMetadata schemaMetadata,
      Optional<FieldPath> nestedRoot) {
    return compareDocuments(
        source,
        matViewDoc,
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(definition),
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(definition, schemaMetadata),
        schemaMetadata,
        nestedRoot);
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
        extractFilterValues(
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
        extractFilterValues(
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
        extractFilterValues(
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
        extractFilterValues(
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
        extractFilterValues(
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
        extractFilterValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // Assert
    assertThat(result)
        .isEqualTo(
            new BsonDocument("color", new BsonString("red"))
                .append(
                    "tags",
                    new BsonArray(List.of(new BsonString("large"), new BsonString("sale")))));
  }

  @Test
  public void extractFilterFieldValues_filterPathTraversesArrayOfSubdocs_buildsNestedArray()
      throws IOException {
    // Regression for CLOUDP-406702: a filter field path like "annotations.id" that traverses an
    // array of subdocuments must produce a nested $set ({annotations: [{id: ...}, ...]}), not a
    // flat dotted key (which MongoDB rejects with "Cannot create field 'id' in element
    // {annotations: [...]}").
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("annotations.id")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append(
                "annotations",
                new BsonArray(
                    List.of(
                        new BsonDocument("id", new BsonString("uuid-1")),
                        new BsonDocument("id", new BsonString("uuid-2")),
                        new BsonDocument("id", new BsonString("uuid-3")))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        extractFilterValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    assertThat(result)
        .isEqualTo(
            new BsonDocument(
                "annotations",
                new BsonArray(
                    List.of(
                        new BsonDocument("id", new BsonString("uuid-1")),
                        new BsonDocument("id", new BsonString("uuid-2")),
                        new BsonDocument("id", new BsonString("uuid-3"))))));
  }

  @Test
  public void extractFilterFieldValues_multipleFilterFieldsUnderSameArray_pairsByArrayIndex()
      throws IOException {
    // Two filter fields under the same array (annotations.id + annotations.type) must remain
    // paired by array index in the reconstructed $set document.
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("annotations.id")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("annotations.type")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append(
                "annotations",
                new BsonArray(
                    List.of(
                        new BsonDocument("id", new BsonString("uuid-1"))
                            .append("type", new BsonString("topic"))
                            .append("score", new BsonDouble(0.9)),
                        new BsonDocument("id", new BsonString("uuid-2"))
                            .append("type", new BsonString("person"))
                            .append("score", new BsonDouble(0.7)))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        extractFilterValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    // "score" is not in the index definition so it must be dropped, but id/type stay paired.
    assertThat(result)
        .isEqualTo(
            new BsonDocument(
                "annotations",
                new BsonArray(
                    List.of(
                        new BsonDocument("id", new BsonString("uuid-1"))
                            .append("type", new BsonString("topic")),
                        new BsonDocument("id", new BsonString("uuid-2"))
                            .append("type", new BsonString("person"))))));
  }

  @Test
  public void extractFilterFieldValues_nestedSubdocFilter_buildsNestedDocument()
      throws IOException {
    // Filter path through a nested subdocument (not an array) must produce a nested $set, not a
    // flat dotted key.
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("meta.locale")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append(
                "meta",
                new BsonDocument("locale", new BsonString("en-US"))
                    .append("region", new BsonString("NA")));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        extractFilterValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    assertThat(result)
        .isEqualTo(new BsonDocument("meta", new BsonDocument("locale", new BsonString("en-US"))));
  }

  @Test
  public void extractFilterFieldValues_resultNeverContainsId() throws IOException {
    // Guard the contract: the $set document must never include _id (MongoDB rejects updates to
    // _id). The handler does not normally copy _id since it is not a passthrough field, but the
    // helper strips it defensively in case a future index definition declares _id as a filter.
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result =
        extractFilterValues(
            rawBsonDoc, AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef));

    assertThat(result.containsKey("_id")).isFalse();
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

  @Test
  public void compareDocuments_dynamicSearchMapping_dynamicPassthroughFieldDoesNotTriggerReindex() {
    // For a dynamic search auto-embed index, fields that dynamic mode copies implicitly (not listed
    // in fieldMap()) must not cause compareDocuments to flag the doc as stale.
    DynamicDefinition dynamic = new DynamicDefinition.Boolean(true);
    AutoEmbedFieldMapping sourceMapping =
        new SearchIndexAutoEmbedFieldMapping(dynamic, ImmutableMap.of(), ImmutableSet.of());
    AutoEmbedFieldMapping matViewMapping =
        new SearchIndexAutoEmbedFieldMapping(dynamic, ImmutableMap.of(), ImmutableSet.of());

    // MV contains a dynamic-passthrough field not in fieldMap() — must not look stale.
    RawBsonDocument sourceDoc =
        new RawBsonDocument(
            new BsonDocument("body", new BsonString("world")), BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument matViewDoc =
        new RawBsonDocument(
            new BsonDocument("body", new BsonString("world")), BsonUtils.BSON_DOCUMENT_CODEC);

    var result =
        compareDocuments(
            sourceDoc, matViewDoc, sourceMapping, matViewMapping, MAT_VIEW_SCHEMA_METADATA);

    assertFalse(
        "dynamic passthrough field in MV must not trigger spurious reindex",
        result.needsReIndexing());
  }

  @Test
  public void requiresEmbeddingGeneration_truncatedArrayOnEmbedAncestor_returnsTrue() {
    // $pop / $slice on an array that contains an embed field surfaces only via
    // truncatedArrays — neither updatedFields nor removedFields names the touched field.
    // Without consulting truncatedArrays the gate would misclassify this as filter-only.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title");
    com.mongodb.client.model.changestream.TruncatedArray truncated =
        new com.mongodb.client.model.changestream.TruncatedArray("items", 2);
    UpdateDescription ud = new UpdateDescription(List.of(), new BsonDocument(), List.of(truncated));

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(ud, mapping));
  }

  @Test
  public void requiresEmbeddingGeneration_truncatedArrayUnrelatedToEmbed_returnsFalse() {
    // truncatedArrays on a filter-only array shouldn't force embedding work.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title");
    com.mongodb.client.model.changestream.TruncatedArray truncated =
        new com.mongodb.client.model.changestream.TruncatedArray("unrelatedArray", 5);
    UpdateDescription ud = new UpdateDescription(List.of(), new BsonDocument(), List.of(truncated));

    assertFalse(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(ud, mapping));
  }

  @Test
  public void computePartialEmbedChangedTexts_truncatedArrayOnEmbedAncestor_fallsBack() {
    // Even when the update also writes a clean embed field, a truncated array touching an embed
    // path is destructive — partial $set can't represent it. Must fall back to full replace so
    // the MV vector for items.title is correctly regenerated against the truncated array.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "items.title");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString("new title"))
                .append("items", new BsonArray()),
            BsonUtils.BSON_DOCUMENT_CODEC);
    com.mongodb.client.model.changestream.TruncatedArray truncated =
        new com.mongodb.client.model.changestream.TruncatedArray("items", 0);
    UpdateDescription ud =
        new UpdateDescription(
            List.of(), new BsonDocument("title", new BsonString("new title")), List.of(truncated));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void requiresEmbeddingGeneration_arrayIndexedEmbedUpdate_returnsTrue() {
    // Change-stream `updatedFields` addresses array elements with numeric segments
    // (e.g., "items.0.title"). requiresEmbeddingGeneration must recognize that touch as relevant
    // to the declared embed path "items.title" — otherwise the update would be misrouted into
    // the filter-only path and the MV embedding for items.title would go stale.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title");
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("items.0.title", new BsonString("changed")));

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(ud, mapping));
  }

  @Test
  public void requiresEmbeddingGeneration_arrayIndexedEmbedRemoval_returnsTrue() {
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title");
    UpdateDescription ud = new UpdateDescription(List.of("items.0.title"), new BsonDocument());

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(ud, mapping));
  }

  @Test
  public void computePartialEmbedChangedTexts_nullUpdateDescription_fallsBack() {
    // requiresEmbeddingGeneration returns true on a null updateDescription (its fail-safe
    // contract), so the partial-embed classifier must defend against that case rather than NPE
    // on the first dereference. Expected behavior is "fall back to full replace".
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("title", new BsonString("kept")),
            BsonUtils.BSON_DOCUMENT_CODEC);

    Optional<ImmutableMap<FieldPath, String>> result = classify(null, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_subsetOfEmbedFieldsUpdated_returnsThem() {
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "description", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString("new title"))
                .append("description", new BsonString("kept"))
                .append("category", new BsonString("books")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("new title")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertTrue(result.isPresent());
    assertEquals(Set.of(FieldPath.parse("title")), result.get().keySet());
  }

  @Test
  public void computePartialEmbedChangedTexts_embedFieldRemoved_fallsBack() {
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "description", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("description", new BsonString("kept")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud = new UpdateDescription(List.of("title"), new BsonDocument());

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_arrayTraversal_fallsBack() {
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title");
    BsonArray items = new BsonArray();
    items.add(new BsonDocument("title", new BsonString("a")));
    items.add(new BsonDocument("title", new BsonString("b")));
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("items", items), BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("items.title", new BsonString("b")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_onlyFilterChanged_throws() {
    // Calling computePartialEmbedChangedTexts when no embed field changed is a contract violation
    // — callers must gate on requiresEmbeddingGeneration (which decides between filter-only and
    // re-embed routing). Reaching the classifier with only filter changes means the caller skipped
    // that gate, so we fail loudly rather than ambiguously returning Optional.empty (which would
    // collide with the "fall back to full replace" signal).
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString("t"))
                .append("category", new BsonString("books")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("books")));

    assertThrows(
        com.google.common.base.VerifyException.class, () -> classify(ud, fullDoc, mapping));
  }

  @Test
  public void computePartialEmbedChangedTexts_emptyNewEmbedText_fallsBack() {
    // When the new value for a changed embed field is empty, the partial-update path can't $unset
    // the existing MV vector for that field — fall back to a full replace which handles unset
    // correctly via the full ReplaceOne.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString(""))
                .append("category", new BsonString("books")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_unrelatedFieldsAreIgnored() {
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString("new"))
                .append("unrelated", new BsonString("noise"))
                .append("category", new BsonString("books")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(
            null,
            new BsonDocument()
                .append("title", new BsonString("new"))
                .append("unrelated", new BsonString("noise")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertTrue(result.isPresent());
    assertEquals(Set.of(FieldPath.parse("title")), result.get().keySet());
  }

  @Test
  public void computePartialEmbedChangedTexts_arrayIndexedEmbedUpdate_fallsBack() {
    // Declared embed path "items.title" is matched by an update written via an array-index
    // segment ("items.0.title"). FieldPath.isDirectRelation is literal-segment based and would
    // not flag this as related, so the classifier must collapse the numeric segment and detect
    // the relation — otherwise a partial-embed update that touches another embed field
    // (e.g. "description") alongside it would silently leave the "items.title" embedding stale.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title", "description");
    BsonArray items = new BsonArray();
    items.add(new BsonDocument("title", new BsonString("Element zero")));
    items.add(new BsonDocument("title", new BsonString("Element one")));
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("items", items).append("description", new BsonString("desc")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(
            null,
            new BsonDocument()
                .append("items.0.title", new BsonString("Element zero updated"))
                .append("description", new BsonString("desc updated")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_arrayIndexedEmbedRemoval_fallsBack() {
    // Same shape as above but the array-indexed touch is a $unset (e.g. removing items.0.title).
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("items.title");
    BsonArray items = new BsonArray();
    items.add(new BsonDocument());
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("items", items), BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud = new UpdateDescription(List.of("items.0.title"), new BsonDocument());

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  // Schema v1+ metadata, matching what production MV collections are stamped with: vectors are
  // rewritten under _autoEmbed.* and hashes under _autoEmbed._hash.*. The v0
  // MAT_VIEW_SCHEMA_METADATA constant above is kept for the older v0-era functions only; the
  // partial-embed builder is exercised against the schema it will actually see in production.
  private static final MaterializedViewSchemaMetadata V1_SCHEMA_METADATA =
      new MaterializedViewSchemaMetadata(
          1, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title")));

  @Test
  public void buildPartialEmbedUpdateSetBody_writesDottedVectorAndHashEntries() {
    BsonDocument filterBody = new BsonDocument().append("category", new BsonString("books"));
    Vector titleVector =
        Vector.fromFloats(new float[] {1.0f, 2.0f, 3.0f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> vectors =
        ImmutableMap.of(FieldPath.parse("title"), ImmutableMap.of("hello", titleVector));

    BsonDocument result =
        AutoEmbeddingDocumentUtils.buildPartialEmbedUpdateSetBody(
            filterBody,
            ImmutableMap.of(FieldPath.parse("title"), "hello"),
            vectors,
            V1_SCHEMA_METADATA);

    // Filter entry preserved.
    assertEquals(new BsonString("books"), result.get("category"));
    // Vector entry written at the schema-v1 MV path and the hash under the v1 hash namespace,
    // both as dotted keys so the $set touches only these fields.
    assertEquals(
        BsonVectorParser.encode(titleVector).asBinary(), result.get("_autoEmbed.title").asBinary());
    assertEquals(new BsonString(computeTextHash("hello")), result.get("_autoEmbed._hash.title"));
    // No entries at the v0-era source-relative paths.
    assertFalse(result.containsKey("title"));
    assertFalse(result.containsKey("title_hash"));
    // Untouched embed field has no entry — its MV vector + hash are preserved by the partial $set.
    assertFalse(result.containsKey("description"));
    assertFalse(result.containsKey("_autoEmbed.description"));
    // Original filter body must not be mutated.
    assertFalse(filterBody.containsKey("_autoEmbed.title"));
  }

  @Test
  public void buildPartialEmbedUpdateSetBody_missingVectorThrows() {
    // The embedding-service contract is one vector per non-empty input; if the response is
    // missing a vector for a non-empty changed-path text, the build must throw so the batch
    // fails rather than writing an inconsistent MV state.
    BsonDocument filterBody = new BsonDocument();
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> noVectors = ImmutableMap.of();

    IllegalStateException ex =
        org.junit.Assert.assertThrows(
            IllegalStateException.class,
            () ->
                AutoEmbeddingDocumentUtils.buildPartialEmbedUpdateSetBody(
                    filterBody,
                    ImmutableMap.of(FieldPath.parse("title"), "hello"),
                    noVectors,
                    V1_SCHEMA_METADATA));
    assertTrue(ex.getMessage().contains("title"));
  }

  @Test
  public void computePartialEmbedChangedTexts_updatedAncestorOfFilter_fallsBack() {
    // $set {meta: 5} deletes filter meta.category from the post-image even though nothing
    // appears in removedFields — a scalar overwrite of an ancestor is a deletion expressed
    // through updatedFields. The $set body would carry nothing for meta, leaving the stale
    // filter value on the MV; full replace drops it.
    AutoEmbedFieldMapping mapping = mappingOf(List.of("title"), List.of("meta.category"));
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument()
                .append("title", new BsonString("new"))
                .append("meta", new BsonInt32(5)),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(
            null,
            new BsonDocument()
                .append("meta", new BsonInt32(5))
                .append("title", new BsonString("new")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_embedIsAncestorOfFilter_fallsBack() {
    // Embed "a" with filter "a.b": $set {a: "y"} re-embeds a, but the post-image no longer
    // carries a.b, so its stale filter value would survive a partial $set on the MV. The
    // strict-ancestor check runs before the exact-embed branch precisely so embed paths that
    // are also ancestors of filters fall back to a full replace.
    AutoEmbedFieldMapping mapping = mappingOf(List.of("a"), List.of("a.b"));
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("a", new BsonString("y")), BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud = new UpdateDescription(null, new BsonDocument("a", new BsonString("y")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_searchIndexMapping_fallsBack() {
    // The destructive-touch safety check requires fieldMap() to enumerate every indexed path,
    // which SearchIndexAutoEmbedFieldMapping's dynamic mode intentionally does not do. Any
    // non-vector mapping must degrade to a full replace rather than risk classifying a
    // destructive update as partial-safe and leaving stale values on the MV.
    AutoEmbedFieldMapping mapping =
        new SearchIndexAutoEmbedFieldMapping(
            new DynamicDefinition.Boolean(true), ImmutableMap.of(), ImmutableSet.of());
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("title", new BsonString("new")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(List.of("body"), new BsonDocument("title", new BsonString("new")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_overlappingEmbeds_fallsBack() {
    // Embeds "a" and "a.b" are both declared (definition validation only rejects exact
    // duplicates). A $set of "a" reshapes what "a.b" extracts — e.g. {a: {b: "x"}} -> {a: "y"}
    // re-embeds "a" but a partial $set can't drop "a.b"'s now-stale vector + hash. Must fall
    // back to a full replace, which rebuilds the MV document wholesale.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("a", "a.b");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("a", new BsonString("y")), BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud = new UpdateDescription(null, new BsonDocument("a", new BsonString("y")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_removedFilterField_fallsBack() {
    // {$set: {title: ...}, $unset: {category: ""}} with category a filter field: the $set body
    // is built from the post-image, which no longer carries category, so a partial update would
    // leave the stale category value on the MV. Full replace drops it; the classifier must fall
    // back on destructive touches of filter fields, not just embeds.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title", "category");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("title", new BsonString("new")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(
            List.of("category"), new BsonDocument("title", new BsonString("new")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_nullFullDocument_fallsBack() {
    // Defensive: in production fullDocument is always present (mongot configures fullDocument
    // lookup on change streams), but a null defaults to a safe fall-back rather than NPE'ing.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title");
    UpdateDescription ud =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("hi")));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, null, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_noChannelsReported_fallsBack() {
    // requiresEmbeddingGeneration returns true (be safe) when all three UpdateDescription channels
    // are empty. The classifier must match that conservatism: fall back to full replace rather
    // than reach the post-loop precondition with no changedEmbedPaths.
    AutoEmbedFieldMapping mapping = autoEmbedMappingWithFields("title");
    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("title", new BsonString("hi")),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud = new UpdateDescription(null, new BsonDocument());

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_filterIsAncestorOfEmbed_fallsBack() {
    // Filter "b" is declared as the ancestor of embed "b.c". An update touching "b" reaches
    // requiresEmbeddingGeneration (which sees b as an embed-ancestor via mayAffectEmbedField) and
    // would dispatch here. The classifier must treat "b" the same way the gate did — as an
    // ancestor/descendant touch that we can't precisely scope — and fall back to a full replace,
    // not let "b" slip through as a benign passthrough and produce an empty changedEmbedPaths set.
    VectorIndexDefinition def =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("b")),
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("b.c"))))
            .build();
    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(def);

    RawBsonDocument fullDoc =
        new RawBsonDocument(
            new BsonDocument().append("b", new BsonDocument("c", new BsonString("hello"))),
            BsonUtils.BSON_DOCUMENT_CODEC);
    UpdateDescription ud =
        new UpdateDescription(
            null, new BsonDocument("b", new BsonDocument("c", new BsonString("hello"))));

    Optional<ImmutableMap<FieldPath, String>> result = classify(ud, fullDoc, mapping);

    assertFalse(result.isPresent());
  }

  @Test
  public void computePartialEmbedChangedTexts_pinsFallbackReason() {
    // Each input shape the classifier can reject maps to exactly one PartialEmbedFallbackReason.
    // (TEXT_EXTRACTION_FAILED needs an IOException mid-walk, not reachable from a well-formed doc;
    // REPLACE_EVENT is produced at the change-stream routing layer, not by the classifier.)
    BsonArray items = new BsonArray();
    items.add(new BsonDocument("title", new BsonString("a")));
    items.add(new BsonDocument("title", new BsonString("b")));

    record ReasonCase(
        String name,
        AutoEmbedFieldMapping mapping,
        UpdateDescription ud,
        RawBsonDocument fullDoc,
        PartialEmbedFallbackReason expected) {}

    List<ReasonCase> cases =
        List.of(
            new ReasonCase(
                "null update description",
                autoEmbedMappingWithFields("title"),
                null,
                rawDoc(new BsonDocument("title", new BsonString("hi"))),
                PartialEmbedFallbackReason.NULL_UPDATE_DESCRIPTION),
            new ReasonCase(
                "null full document",
                autoEmbedMappingWithFields("title"),
                new UpdateDescription(null, new BsonDocument("title", new BsonString("hi"))),
                null,
                PartialEmbedFallbackReason.NULL_FULL_DOCUMENT),
            new ReasonCase(
                "no channels reported",
                autoEmbedMappingWithFields("title"),
                new UpdateDescription(null, new BsonDocument()),
                rawDoc(new BsonDocument("title", new BsonString("hi"))),
                PartialEmbedFallbackReason.NO_CHANNELS_REPORTED),
            new ReasonCase(
                "unsupported field mapping",
                new SearchIndexAutoEmbedFieldMapping(
                    new DynamicDefinition.Boolean(true), ImmutableMap.of(), ImmutableSet.of()),
                new UpdateDescription(
                    List.of("body"), new BsonDocument("title", new BsonString("new"))),
                rawDoc(new BsonDocument("title", new BsonString("new"))),
                PartialEmbedFallbackReason.UNSUPPORTED_FIELD_MAPPING),
            new ReasonCase(
                "destructive indexed touch",
                autoEmbedMappingWithFields("title", "category"),
                new UpdateDescription(
                    List.of("category"), new BsonDocument("title", new BsonString("new"))),
                rawDoc(new BsonDocument("title", new BsonString("new"))),
                PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH),
            new ReasonCase(
                "ancestor overwrite",
                mappingOf(List.of("title"), List.of("meta.category")),
                new UpdateDescription(
                    null,
                    new BsonDocument()
                        .append("meta", new BsonInt32(5))
                        .append("title", new BsonString("new"))),
                rawDoc(
                    new BsonDocument()
                        .append("title", new BsonString("new"))
                        .append("meta", new BsonInt32(5))),
                PartialEmbedFallbackReason.ANCESTOR_OVERWRITE),
            new ReasonCase(
                "overlapping embeds",
                autoEmbedMappingWithFields("a", "a.b"),
                new UpdateDescription(null, new BsonDocument("a.b", new BsonString("y"))),
                rawDoc(new BsonDocument("a", new BsonDocument("b", new BsonString("y")))),
                PartialEmbedFallbackReason.OVERLAPPING_EMBEDS),
            new ReasonCase(
                "indirect embed touch",
                autoEmbedMappingWithFields("items.title"),
                new UpdateDescription(
                    null, new BsonDocument("items.0.title", new BsonString("changed"))),
                rawDoc(new BsonDocument("items", items.clone())),
                PartialEmbedFallbackReason.INDIRECT_EMBED_TOUCH),
            new ReasonCase(
                "array traversal",
                autoEmbedMappingWithFields("items.title"),
                new UpdateDescription(null, new BsonDocument("items.title", new BsonString("b"))),
                rawDoc(new BsonDocument("items", items.clone())),
                PartialEmbedFallbackReason.ARRAY_TRAVERSAL),
            new ReasonCase(
                "empty new value",
                autoEmbedMappingWithFields("title", "category"),
                new UpdateDescription(null, new BsonDocument("title", new BsonString(""))),
                rawDoc(new BsonDocument("title", new BsonString(""))),
                PartialEmbedFallbackReason.EMPTY_NEW_VALUE));

    for (ReasonCase c : cases) {
      PartialEmbedClassification result =
          AutoEmbeddingDocumentUtils.computePartialEmbedChangedTexts(c.ud, c.fullDoc, c.mapping);
      assertEquals(
          c.name + ": expected fallback reason", Optional.of(c.expected), result.fallbackReason());
      assertFalse(c.name + ": should not be applied", result.changedTexts().isPresent());
    }
  }

  @Test
  public void testFilterOnlyPartialUpdateFallbackReason_TopLevelFilters() {
    AutoEmbedFieldMapping mapping = mappingOf(List.of("body"), List.of("meta.tag", "category"));
    Optional<FieldPath> noNestedRoot = Optional.empty();

    // $set overwriting an ancestor drops the co-located filter sub-field → ANCESTOR_OVERWRITE.
    assertEquals(
        Optional.of(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(
                null, new BsonDocument("meta", new BsonDocument("color", new BsonString("blue")))),
            mapping,
            noNestedRoot));

    // $unset of a filter can't be cleared by a partial $set → DESTRUCTIVE_INDEXED_TOUCH.
    assertEquals(
        Optional.of(PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(List.of("category"), new BsonDocument()), mapping, noNestedRoot));

    // Setting a filter directly, or touching unrelated fields, stays on the partial path.
    assertEquals(
        Optional.empty(),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(null, new BsonDocument("category", new BsonString("x"))),
            mapping,
            noNestedRoot));
    assertEquals(
        Optional.empty(),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(null, new BsonDocument("unrelated", new BsonString("x"))),
            mapping,
            noNestedRoot));
    assertEquals(
        Optional.empty(),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(List.of("unrelated"), new BsonDocument()),
            mapping,
            noNestedRoot));
  }

  @Test
  public void testFilterOnlyPartialUpdateFallbackReason_NestedFilters() {
    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            VectorIndexDefinitionBuilder.builder()
                .setFields(
                    List.of(
                        new VectorAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("sections.content")),
                        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.meta.tag"))))
                .nestedRoot("sections")
                .build());
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));

    // Reviewer's case: $set overwrites sections.0.meta, dropping the co-located sections.meta.tag.
    // The additive positional $set can't clear it → NESTED_FILTER_REMAPPING.
    assertEquals(
        Optional.of(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(
                null,
                new BsonDocument(
                    "sections.0.meta", new BsonDocument("color", new BsonString("blue")))),
            mapping,
            nestedRoot));

    // $unset of a nested filter is also unclearable by a partial $set.
    assertEquals(
        Optional.of(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(List.of("sections.0.meta.tag"), new BsonDocument()),
            mapping,
            nestedRoot));

    // A $pop/$slice on the nestedRoot array shortens the co-located element list.
    assertEquals(
        Optional.of(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(
                List.of(), new BsonDocument(), List.of(new TruncatedArray("sections", 2))),
            mapping,
            nestedRoot));

    // Destructive/overwrite on an unrelated field stays on the partial path.
    assertEquals(
        Optional.empty(),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            new UpdateDescription(List.of("unrelatedField"), new BsonDocument()),
            mapping,
            nestedRoot));
    assertEquals(
        Optional.empty(),
        AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
            null, mapping, nestedRoot));
  }

  private static RawBsonDocument rawDoc(BsonDocument doc) {
    return new RawBsonDocument(doc, BsonUtils.BSON_DOCUMENT_CODEC);
  }

  // Shared fixtures for the CLOUDP-402522 nested-filter remap tests below.
  private static final FieldPath SECTION_CONTENT = FieldPath.parse("sections.section_content");
  private static final FieldPath SECTION_NAME = FieldPath.parse("sections.section_name");
  private static final Vector SAMPLE_VECTOR =
      Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);

  private static MaterializedViewSchemaMetadata nestedSchemaMetadata() {
    return new MaterializedViewSchemaMetadata(
        1,
        Map.of(
            SECTION_CONTENT, FieldPath.parse("_autoEmbed.sections.section_content"),
            SECTION_NAME, FieldPath.parse("_autoEmbed.sections.section_name")));
  }

  private static VectorIndexDefinition nestedIndexDefinition() {
    return VectorIndexDefinitionBuilder.builder()
        .setFields(
            List.of(
                new VectorAutoEmbedFieldDefinition("voyage-3-large", SECTION_CONTENT),
                new VectorIndexFilterFieldDefinition(SECTION_NAME)))
        .nestedRoot("sections")
        .build();
  }

  private static BsonDocument section(String name, String content) {
    return new BsonDocument()
        .append("section_name", new BsonString(name))
        .append("section_content", new BsonString(content));
  }

  private static DocumentEvent insertEventOf(RawBsonDocument rawSource) {
    return DocumentEvent.createInsert(
        DocumentMetadata.fromOriginalDocument(Optional.of(rawSource)), rawSource);
  }

  private static ImmutableMap<FieldPath, ImmutableMap<String, Vector>> sectionEmbeddings(
      String text, Vector vector) {
    return ImmutableMap.of(SECTION_CONTENT, ImmutableMap.of(text, vector));
  }

  /** Routing verdict produced by the production gate -> classifier sequence. */
  private enum Verdict {
    FILTER_ONLY,
    PARTIAL,
    FULL_REPLACE
  }

  /**
   * Mirrors the production dispatch in ChangeStreamDocumentUtils: gate on
   * requiresEmbeddingGeneration first, then classify. Keeping the sequence identical to production
   * is what lets this matrix catch gate/classifier inconsistencies (e.g. a shape the gate passes
   * through but the classifier's precondition rejects).
   */
  private static Verdict verdictFor(
      UpdateDescription ud, RawBsonDocument fullDoc, AutoEmbedFieldMapping mapping) {
    if (!AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(ud, mapping)) {
      return Verdict.FILTER_ONLY;
    }
    return classify(ud, fullDoc, mapping).isPresent() ? Verdict.PARTIAL : Verdict.FULL_REPLACE;
  }

  private record MatrixCase(
      String name,
      AutoEmbedFieldMapping mapping,
      UpdateDescription ud,
      BsonDocument postImage,
      Verdict expected) {}

  /**
   * Exhaustive verdict matrix over update channel (updated / removed / truncated) x path relation
   * (exact / ancestor / descendant / array-indexed) x field kind (embed / filter / unindexed) x
   * declaration overlap. Each cell pins the routing verdict of the production gate -> classifier
   * sequence. The safety invariant behind the expectations: a partial $set must produce the same MV
   * document a full replace would; anything that removes indexed content or can't be precisely
   * scoped must not classify as PARTIAL.
   */
  @Test
  public void classifierVerdictMatrix() {
    AutoEmbedFieldMapping std =
        mappingOf(List.of("title", "items.title"), List.of("category", "meta.category"));
    AutoEmbedFieldMapping arrayEmbed = mappingOf(List.of("title", "tags"), List.of("category"));
    AutoEmbedFieldMapping arrayFilter = mappingOf(List.of("title"), List.of("filterArr"));
    AutoEmbedFieldMapping overlapEmbeds = mappingOf(List.of("a", "a.b"), List.of());
    AutoEmbedFieldMapping filterOverEmbed = mappingOf(List.of("b.c"), List.of("b"));

    BsonDocument withTitle =
        new BsonDocument()
            .append("title", new BsonString("new"))
            .append("category", new BsonString("x"));
    BsonDocument empty = new BsonDocument();

    List<MatrixCase> cases =
        List.of(
            // ---- updatedFields channel ----
            new MatrixCase(
                "updated exact embed",
                std,
                ud(List.of(), doc("title", "new"), List.of()),
                withTitle,
                Verdict.PARTIAL),
            new MatrixCase(
                "updated ancestor of embed",
                std,
                ud(List.of(), new BsonDocument("items", new BsonDocument()), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "updated descendant of embed",
                std,
                ud(List.of(), doc("title.sub", "x"), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "updated array-indexed embed",
                std,
                ud(List.of(), doc("items.0.title", "x"), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "updated exact filter only",
                std,
                ud(List.of(), doc("category", "y"), List.of()),
                withTitle,
                Verdict.FILTER_ONLY),
            new MatrixCase(
                "updated filter + exact embed",
                std,
                ud(
                    List.of(),
                    doc("category", "y").append("title", new BsonString("new")),
                    List.of()),
                withTitle,
                Verdict.PARTIAL),
            new MatrixCase(
                "updated unindexed only",
                std,
                ud(List.of(), doc("noise", "z"), List.of()),
                withTitle,
                Verdict.FILTER_ONLY),
            new MatrixCase(
                "updated unindexed + exact embed",
                std,
                ud(List.of(), doc("noise", "z").append("title", new BsonString("new")), List.of()),
                withTitle,
                Verdict.PARTIAL),
            // ---- removedFields channel ----
            new MatrixCase(
                "removed exact embed",
                std,
                ud(List.of("title"), new BsonDocument(), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "removed ancestor of embed",
                std,
                ud(List.of("items"), new BsonDocument(), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "removed array-indexed embed",
                std,
                ud(List.of("items.0.title"), new BsonDocument(), List.of()),
                empty,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "removed exact filter + updated embed",
                std,
                ud(List.of("category"), doc("title", "new"), List.of()),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "removed ancestor of filter + updated embed",
                std,
                ud(List.of("meta"), doc("title", "new"), List.of()),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "removed unindexed + updated embed",
                std,
                ud(List.of("noise"), doc("title", "new"), List.of()),
                withTitle,
                Verdict.PARTIAL),
            // Known gate-level gap, pre-existing on the filter-only path (it exists on master and
            // is tracked for a separate fix): a pure $unset of one filter field never reaches the
            // classifier and routes filter-only. The post-image below models the post-$unset doc —
            // category is gone, the other filter (meta.category) survives, so extraction yields a
            // non-empty $set body that cannot remove category, leaving its stale value on the MV.
            // FILTER_ONLY here pins the bug deliberately; the fix shows up as a verdict change.
            new MatrixCase(
                "removed exact filter only (pre-existing gate gap)",
                std,
                ud(List.of("category"), new BsonDocument(), List.of()),
                new BsonDocument()
                    .append("title", new BsonString("unchanged"))
                    .append("meta", new BsonDocument("category", new BsonString("kept"))),
                Verdict.FILTER_ONLY),
            // ---- truncatedArrays channel ----
            new MatrixCase(
                "truncated ancestor of embed + updated embed",
                std,
                ud(List.of(), doc("title", "new"), List.of(new TruncatedArray("items", 1))),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "truncated exact-embed array",
                arrayEmbed,
                ud(List.of(), doc("title", "new"), List.of(new TruncatedArray("tags", 1))),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "truncated filter array + updated embed",
                arrayFilter,
                ud(List.of(), doc("title", "new"), List.of(new TruncatedArray("filterArr", 1))),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "truncated unindexed array + updated embed",
                std,
                ud(List.of(), doc("title", "new"), List.of(new TruncatedArray("noiseArr", 1))),
                withTitle,
                Verdict.PARTIAL),
            // The one shape where this PR's gate is NARROWER than master's: a truncation-only
            // update unrelated to any embed. Master returned "require embedding" (full replace)
            // because it never consulted truncatedArrays and treated empty updated+removed as
            // unknown; the gate now sees the truncated path, finds no embed relation, and routes
            // filter-only. Safe because extractFilterFieldValues carries the whole post-image
            // array value and $set replaces arrays wholesale — pinned here deliberately.
            new MatrixCase(
                "truncated filter array only (gate narrowed vs master)",
                arrayFilter,
                ud(List.of(), new BsonDocument(), List.of(new TruncatedArray("filterArr", 1))),
                withTitle,
                Verdict.FILTER_ONLY),
            new MatrixCase(
                "truncated unindexed array only (gate narrowed vs master)",
                std,
                ud(List.of(), new BsonDocument(), List.of(new TruncatedArray("noiseArr", 1))),
                withTitle,
                Verdict.FILTER_ONLY),
            // ---- overlapping declarations ----
            new MatrixCase(
                "overlapping embeds, updated ancestor embed",
                overlapEmbeds,
                ud(List.of(), doc("a", "y"), List.of()),
                doc("a", "y"),
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "overlapping embeds, updated descendant embed",
                overlapEmbeds,
                ud(List.of(), doc("a.b", "y"), List.of()),
                new BsonDocument("a", new BsonDocument("b", new BsonString("y"))),
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "filter ancestor of embed, updated filter",
                filterOverEmbed,
                ud(
                    List.of(),
                    new BsonDocument("b", new BsonDocument("c", new BsonString("x"))),
                    List.of()),
                new BsonDocument("b", new BsonDocument("c", new BsonString("x"))),
                Verdict.FULL_REPLACE),
            // ---- ancestor-overwrite deletions via updatedFields ----
            new MatrixCase(
                "updated scalar ancestor of filter + updated embed",
                std,
                ud(List.of(), doc("title", "new").append("meta", new BsonInt32(5)), List.of()),
                withTitle,
                Verdict.FULL_REPLACE),
            // Conservative: a subdoc overwrite that PRESERVES the filter value would be safe to
            // partial-update (the post-image still carries meta.category), but the classifier
            // refuses all strict-ancestor touches rather than reasoning about which survive.
            // Revisit if CLOUDP-412617 metrics show this fallback is hot.
            new MatrixCase(
                "updated subdoc ancestor of filter (value preserved) + updated embed",
                std,
                ud(
                    List.of(),
                    doc("title", "new")
                        .append("meta", new BsonDocument("category", new BsonString("x"))),
                    List.of()),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase(
                "updated embed that is ancestor of filter",
                mappingOf(List.of("a"), List.of("a.b")),
                ud(List.of(), doc("a", "y"), List.of()),
                doc("a", "y"),
                Verdict.FULL_REPLACE),
            // ---- degenerate shapes ----
            new MatrixCase(
                "all channels empty",
                std,
                ud(List.of(), new BsonDocument(), List.of()),
                withTitle,
                Verdict.FULL_REPLACE),
            new MatrixCase("null update description", std, null, withTitle, Verdict.FULL_REPLACE));

    List<String> failures = new ArrayList<>();
    for (MatrixCase c : cases) {
      RawBsonDocument fullDoc = new RawBsonDocument(c.postImage(), BsonUtils.BSON_DOCUMENT_CODEC);
      try {
        Verdict actual = verdictFor(c.ud(), fullDoc, c.mapping());
        if (actual != c.expected()) {
          failures.add(c.name() + ": expected " + c.expected() + " but got " + actual);
        }
      } catch (RuntimeException e) {
        failures.add(c.name() + ": threw " + e.getClass().getSimpleName() + ": " + e.getMessage());
      }
    }
    assertTrue("Verdict matrix mismatches:\n" + String.join("\n", failures), failures.isEmpty());
  }

  private static BsonDocument doc(String key, String value) {
    return new BsonDocument(key, new BsonString(value));
  }

  private static UpdateDescription ud(
      List<String> removed, BsonDocument updated, List<TruncatedArray> truncated) {
    return new UpdateDescription(removed, updated, truncated);
  }

  /** Classifier result reduced to the changed-text Optional (empty = fell back). */
  private static Optional<ImmutableMap<FieldPath, String>> classify(
      UpdateDescription updateDescription,
      RawBsonDocument fullDocument,
      AutoEmbedFieldMapping fieldMapping) {
    return AutoEmbeddingDocumentUtils.computePartialEmbedChangedTexts(
            updateDescription, fullDocument, fieldMapping)
        .changedTexts();
  }

  /** Filter-field {@code $set} body, unwrapped — these tests all exercise the success path. */
  private static BsonDocument extractFilterValues(
      RawBsonDocument fullDocument, AutoEmbedFieldMapping fieldMapping) {
    return AutoEmbeddingDocumentUtils.extractFilterFieldValues(fullDocument, fieldMapping)
        .orElseThrow();
  }

  private static AutoEmbedFieldMapping mappingOf(List<String> embeds, List<String> filters) {
    List<VectorIndexFieldDefinition> fields = new ArrayList<>();
    for (String embed : embeds) {
      fields.add(new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse(embed)));
    }
    for (String filter : filters) {
      fields.add(new VectorIndexFilterFieldDefinition(FieldPath.parse(filter)));
    }
    return AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
        VectorIndexDefinitionBuilder.builder().setFields(fields).build());
  }

  private static AutoEmbedFieldMapping autoEmbedMappingWithFields(String... paths) {
    List<VectorIndexFieldDefinition> fields = new java.util.ArrayList<>();
    for (String path : paths) {
      // Treat "category" as the conventional filter path in these tests; everything else is an
      // auto-embed field.
      if (path.equals("category")) {
        fields.add(new VectorIndexFilterFieldDefinition(FieldPath.parse(path)));
      } else {
        fields.add(new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse(path)));
      }
    }
    VectorIndexDefinition def = VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    return AutoEmbedFieldMappingCreator.createAutoEmbedMapping(def);
  }

  /**
   * Multi-vector nested auto-embed: every source array element under the nestedRoot is embedded
   * into its own MV child under {@code _autoEmbed.<nestedRoot>}, co-located with that element's
   * filter value, with a parallel per-element hash array. This makes every element independently
   * searchable, matching how regular pre-computed nested vectors index each array element.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_nestedRoot_writesPerElementVectors()
      throws IOException {
    var schemaMetadata = nestedSchemaMetadata();
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", SECTION_CONTENT),
                    new VectorIndexFilterFieldDefinition(SECTION_NAME),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("name"))))
            .nestedRoot("sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_12"))
            .append("name", new BsonString("book_12"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("book_12_section_1"))
                            .append("section_content", new BsonString("content s1")),
                        new BsonDocument()
                            .append("section_name", new BsonString("book_12_section_2"))
                            .append("section_content", new BsonString("content s2")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawSource)), rawSource);

    Vector v1 = Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    Vector v2 = Vector.fromFloats(new float[] {0.3f, 0.4f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(
            FieldPath.parse("sections.section_content"),
            ImmutableMap.of("content s1", v1, "content s2", v2));

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            embeddings,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    BsonDocument matViewDoc = result.getDocument().get();
    BsonArray sections = matViewDoc.getDocument("_autoEmbed").getArray("sections");
    assertEquals(
        "Every source element must be embedded into its own index-aligned MV child.",
        2,
        sections.size());
    assertEquals(
        new BsonString("book_12_section_1"),
        sections.get(0).asDocument().getString("section_name"));
    assertThat(sections.get(0).asDocument().containsKey("section_content")).isTrue();
    assertEquals(
        new BsonString("book_12_section_2"),
        sections.get(1).asDocument().getString("section_name"));
    assertThat(sections.get(1).asDocument().containsKey("section_content")).isTrue();

    BsonArray hashes =
        matViewDoc
            .getDocument("_autoEmbed")
            .getDocument("_hash")
            .getDocument("sections")
            .getArray("section_content");
    assertEquals("Per-element hash array must align 1:1 with the element array.", 2, hashes.size());
    assertEquals(new BsonString(computeTextHash("content s1")), hashes.get(0));
    assertEquals(new BsonString(computeTextHash("content s2")), hashes.get(1));
    assertEquals(
        "Top-level filters must keep their source path so they continue to be indexed at the"
            + " root document level.",
        new BsonString("book_12"),
        matViewDoc.getString("name"));
  }

  /**
   * Multi-segment nestedRoot regression (writer side): a deep nestedRoot like {@code
   * metadata.sections} lives under an intermediate subdocument, so its element array must be
   * resolved by walking the path. Otherwise the writer builds an empty {@code
   * _autoEmbed.metadata.sections} array and drops every nested vector/filter for deep nested roots.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_multiSegmentNestedRoot_writesPerElementVectors()
      throws IOException {
    FieldPath embedSource = FieldPath.parse("metadata.sections.section_content");
    FieldPath filterSource = FieldPath.parse("metadata.sections.section_name");
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                embedSource, FieldPath.parse("_autoEmbed.metadata.sections.section_content"),
                filterSource, FieldPath.parse("_autoEmbed.metadata.sections.section_name")));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", embedSource),
                    new VectorIndexFilterFieldDefinition(filterSource)))
            .nestedRoot("metadata.sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_deep"))
            .append(
                "metadata",
                new BsonDocument(
                    "sections",
                    new BsonArray(
                        List.of(
                            new BsonDocument()
                                .append("section_name", new BsonString("name_a"))
                                .append("section_content", new BsonString("content a")),
                            new BsonDocument()
                                .append("section_name", new BsonString("name_b"))
                                .append("section_content", new BsonString("content b"))))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent = insertEventOf(rawSource);

    Vector v1 = Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    Vector v2 = Vector.fromFloats(new float[] {0.3f, 0.4f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(embedSource, ImmutableMap.of("content a", v1, "content b", v2));

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            embeddings,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    BsonDocument matViewDoc = result.getDocument().get();
    BsonArray sections =
        matViewDoc.getDocument("_autoEmbed").getDocument("metadata").getArray("sections");
    assertEquals(
        "A multi-segment nestedRoot must resolve through the intermediate subdocument and embed"
            + " every element into its own index-aligned MV child.",
        2,
        sections.size());
    assertEquals(new BsonString("name_a"), sections.get(0).asDocument().getString("section_name"));
    assertThat(sections.get(0).asDocument().containsKey("section_content")).isTrue();
    assertEquals(new BsonString("name_b"), sections.get(1).asDocument().getString("section_name"));
    assertThat(sections.get(1).asDocument().containsKey("section_content")).isTrue();

    BsonArray hashes =
        matViewDoc
            .getDocument("_autoEmbed")
            .getDocument("_hash")
            .getDocument("metadata")
            .getDocument("sections")
            .getArray("section_content");
    assertEquals("Per-element hash array must align 1:1 with the element array.", 2, hashes.size());
    assertEquals(new BsonString(computeTextHash("content a")), hashes.get(0));
    assertEquals(new BsonString(computeTextHash("content b")), hashes.get(1));
  }

  /**
   * Mixed nested + non-nested auto-embed index: an index may declare embed fields both under the
   * nestedRoot and at the root level. The nested fields are written per-element under {@code
   * _autoEmbed.<nestedRoot>}, while the root-level embed field must still get its own standard
   * single vector + hash so it stays searchable.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_mixedNestedAndNonNestedEmbed_writesBoth()
      throws IOException {
    FieldPath nestedEmbed = FieldPath.parse("sections.section_content");
    FieldPath nestedFilter = FieldPath.parse("sections.section_name");
    FieldPath rootEmbed = FieldPath.parse("title");
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                nestedEmbed, FieldPath.parse("_autoEmbed.sections.section_content"),
                nestedFilter, FieldPath.parse("_autoEmbed.sections.section_name"),
                rootEmbed, FieldPath.parse("_autoEmbed.title")));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", nestedEmbed),
                    new VectorIndexFilterFieldDefinition(nestedFilter),
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", rootEmbed)))
            .nestedRoot("sections")
            .build();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_mixed"))
            .append("title", new BsonString("the title text"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("n1"))
                            .append("section_content", new BsonString("content a")),
                        new BsonDocument()
                            .append("section_name", new BsonString("n2"))
                            .append("section_content", new BsonString("content b")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent = insertEventOf(rawSource);

    Vector sectionVector1 =
        Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    Vector sectionVector2 =
        Vector.fromFloats(new float[] {0.3f, 0.4f}, FloatVector.OriginalType.NATIVE);
    Vector titleVector =
        Vector.fromFloats(new float[] {0.5f, 0.6f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(
            nestedEmbed, ImmutableMap.of("content a", sectionVector1, "content b", sectionVector2),
            rootEmbed, ImmutableMap.of("the title text", titleVector));

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            embeddings,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    BsonDocument matViewDoc = result.getDocument().get();

    // Nested embeds: one index-aligned child per source element.
    BsonArray sections = matViewDoc.getDocument("_autoEmbed").getArray("sections");
    assertEquals(2, sections.size());

    // Root-level embed: standard single vector + hash, not dropped by the nested path.
    assertEquals(
        "A root-level embed field on a mixed index must still get its vector written.",
        BsonVectorParser.encode(titleVector),
        matViewDoc.getDocument("_autoEmbed").get("title"));
    assertEquals(
        "A root-level embed field on a mixed index must still get its hash written.",
        new BsonString(computeTextHash("the title text")),
        matViewDoc.getDocument("_autoEmbed").getDocument("_hash").getString("title"));
  }

  /**
   * CLOUDP-402522 (partial-update optimization): a filter-only {@code $set} on a nested index must
   * patch the co-located {@code _autoEmbed.*} filter in place via an array-positional key, so the
   * MV's indexed filter stays consistent with the embedded vector without forcing a full re-index.
   * The source-path copy produced by {@link AutoEmbeddingDocumentUtils#extractFilterFieldValues} is
   * preserved untouched.
   */
  @Test
  public void buildNestedFilterPartialSetBody_addsPositionalCoLocatedFilter() {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));

    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_12"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("updated_name"))
                            .append("section_content", new BsonString("content s1")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);

    // The filter-only $set body as produced by ChangeStreamDocumentUtils: source-path copy only.
    BsonDocument filterOnlySetBody =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawSource, mapping).get();

    BsonDocument augmented =
        AutoEmbeddingDocumentUtils.buildNestedFilterPartialSetBody(
            filterOnlySetBody, rawSource, mapping, indexDefinition.getNestedRoot(), schemaMetadata);

    assertEquals(
        "Co-located filter must be patched via an array-positional $set key so the MV's indexed"
            + " filter stays aligned with the existing vector.",
        new BsonString("updated_name"),
        augmented.getString("_autoEmbed.sections.0.section_name"));
    assertEquals(
        "The source-path copy must be preserved in the augmented body.",
        new BsonString("updated_name"),
        augmented.getArray("sections").get(0).asDocument().getString("section_name"));
  }

  /**
   * Guard against a MongoDB path conflict: when the MV nestedRoot resolves to the source nestedRoot
   * (no _autoEmbed.* remapping, e.g. an empty autoEmbeddingFieldsMapping), extractFilterFieldValues
   * already wrote the filter at the source path. The augmenter must NOT add array-positional keys
   * under the same root (e.g. "sections.0.section_name" alongside "sections"), which MongoDB
   * rejects.
   */
  @Test
  public void buildNestedFilterPartialSetBody_mvRootEqualsSourceRoot_noPositionalKeys() {
    // Empty fields mapping => getMatViewNestedRoot returns the source nestedRoot unchanged.
    var schemaMetadata = new MaterializedViewSchemaMetadata(1, Map.of());

    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_13"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("updated_name"))
                            .append("section_content", new BsonString("content s1")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument filterOnlySetBody =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawSource, mapping).get();

    BsonDocument augmented =
        AutoEmbeddingDocumentUtils.buildNestedFilterPartialSetBody(
            filterOnlySetBody, rawSource, mapping, indexDefinition.getNestedRoot(), schemaMetadata);

    assertFalse(
        "No array-positional key may be emitted when the MV root equals the source root, else the"
            + " $set conflicts with the source-path 'sections' entry.",
        augmented.containsKey("sections.0.section_name"));
    assertEquals(
        "The body must be unchanged from the source-path-only filter body.",
        filterOnlySetBody,
        augmented);
  }

  /**
   * Multi-vector partial-$set: a filter-only update emits one array-positional key per source
   * element straight from the post-image, each carrying that element's own filter value. Because
   * the MV array is index-aligned with the source array, MV child {@code i} is patched from source
   * element {@code i} — no "chosen element" selection — so the result matches what a full replace
   * would produce.
   */
  @Test
  public void buildNestedFilterPartialSetBody_emitsPositionalKeyPerElement() {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));

    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_skew"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("name_a"))
                            .append("section_content", new BsonString("")),
                        new BsonDocument()
                            .append("section_name", new BsonString("name_b"))
                            .append("section_content", new BsonString("real content")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument filterOnlySetBody =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawSource, mapping).get();

    BsonDocument augmented =
        AutoEmbeddingDocumentUtils.buildNestedFilterPartialSetBody(
            filterOnlySetBody, rawSource, mapping, indexDefinition.getNestedRoot(), schemaMetadata);

    assertEquals(
        "Element 0's filter must be patched at its own positional key.",
        new BsonString("name_a"),
        augmented.getString("_autoEmbed.sections.0.section_name"));
    assertEquals(
        "Element 1's filter must be patched at its own positional key.",
        new BsonString("name_b"),
        augmented.getString("_autoEmbed.sections.1.section_name"));
  }

  /**
   * Multi-segment nestedRoot regression: a deep nestedRoot like {@code metadata.sections} lives
   * under an intermediate subdocument, so its element array must be resolved by walking the path
   * rather than read as a single literal top-level key. Otherwise the post-image lookup returns
   * nothing and the co-located {@code _autoEmbed.*} filters are silently left stale, breaking
   * filtered nested $vectorSearch.
   */
  @Test
  public void buildNestedFilterPartialSetBody_multiSegmentNestedRoot_resolvesDeepPath() {
    FieldPath embedSource = FieldPath.parse("metadata.sections.section_content");
    FieldPath filterSource = FieldPath.parse("metadata.sections.section_name");
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                embedSource, FieldPath.parse("_autoEmbed.metadata.sections.section_content"),
                filterSource, FieldPath.parse("_autoEmbed.metadata.sections.section_name")));

    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", embedSource),
                    new VectorIndexFilterFieldDefinition(filterSource)))
            .nestedRoot("metadata.sections")
            .build();
    var mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_deep"))
            .append(
                "metadata",
                new BsonDocument(
                    "sections",
                    new BsonArray(
                        List.of(
                            new BsonDocument()
                                .append("section_name", new BsonString("name_a"))
                                .append("section_content", new BsonString("content a")),
                            new BsonDocument()
                                .append("section_name", new BsonString("name_b"))
                                .append("section_content", new BsonString("content b"))))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument filterOnlySetBody =
        AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawSource, mapping).get();

    BsonDocument augmented =
        AutoEmbeddingDocumentUtils.buildNestedFilterPartialSetBody(
            filterOnlySetBody, rawSource, mapping, indexDefinition.getNestedRoot(), schemaMetadata);

    assertEquals(
        "A multi-segment nestedRoot must resolve through the intermediate subdocument and patch"
            + " element 0's co-located filter.",
        new BsonString("name_a"),
        augmented.getString("_autoEmbed.metadata.sections.0.section_name"));
    assertEquals(
        "Element 1's co-located filter must also be patched at its positional key.",
        new BsonString("name_b"),
        augmented.getString("_autoEmbed.metadata.sections.1.section_name"));
  }

  /**
   * CLOUDP-402522: top-level filter fields must not be remapped, and the MV doc must continue to
   * carry the entire filter list at the source path. Without this guard, schema mapping changes
   * could regress simple non-nested auto-embed indexes.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_topLevelFilter_keepsSourcePath()
      throws IOException {
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("body"));
    var topLevelFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, topLevelFilter))
            .build();

    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("body"), FieldPath.parse("_autoEmbed.body")));

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("d1"))
            .append("color", new BsonString("red"))
            .append("body", new BsonString("hello"));
    DocumentEvent rawEvent = insertEventOf(rawDoc(source));

    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(FieldPath.parse("body"), ImmutableMap.of("hello", SAMPLE_VECTOR));

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            embeddings,
            schemaMetadata);

    BsonDocument matViewDoc = result.getDocument().get();
    assertEquals(new BsonString("red"), matViewDoc.getString("color"));
    assertFalse(
        "No nested-filter remap should happen when the source has no nestedRoot match.",
        matViewDoc.containsKey("_autoEmbed")
            && matViewDoc.getDocument("_autoEmbed").containsKey("color"));
  }

  /**
   * Multi-vector nested construction with an empty element: an element whose embed text is empty
   * still produces a child carrying its filter value (but no vector) and an empty hash slot, so the
   * MV array stays index-aligned with the source array — which keeps {@code compareDocuments} and
   * the partial-{@code $set} path positionally consistent.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_nestedEmptyElement_keepsAlignment()
      throws IOException {
    var schemaMetadata = nestedSchemaMetadata();
    VectorIndexDefinition indexDefinition = nestedIndexDefinition();

    // First element has empty embed text but a valid filter value; only the second is embedded.
    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_skew"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        section("name_for_empty_section", ""),
                        section("name_for_real_section", "real content"))));
    DocumentEvent rawEvent = insertEventOf(rawDoc(source));

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            sectionEmbeddings("real content", SAMPLE_VECTOR),
            schemaMetadata,
            indexDefinition.getNestedRoot());

    BsonDocument matViewDoc = result.getDocument().get();
    BsonArray sections = matViewDoc.getDocument("_autoEmbed").getArray("sections");
    assertEquals("Both elements must produce a child to keep alignment.", 2, sections.size());
    assertEquals(
        new BsonString("name_for_empty_section"),
        sections.get(0).asDocument().getString("section_name"));
    assertFalse(
        "Empty-text element must not carry a vector.",
        sections.get(0).asDocument().containsKey("section_content"));
    assertEquals(
        new BsonString("name_for_real_section"),
        sections.get(1).asDocument().getString("section_name"));
    assertThat(sections.get(1).asDocument().containsKey("section_content")).isTrue();

    BsonArray hashes =
        matViewDoc
            .getDocument("_autoEmbed")
            .getDocument("_hash")
            .getDocument("sections")
            .getArray("section_content");
    assertEquals(2, hashes.size());
    assertEquals(
        "Empty-text element must have an empty hash slot.", new BsonString(""), hashes.get(0));
    assertEquals(new BsonString(computeTextHash("real content")), hashes.get(1));
  }

  /**
   * Multi-vector nested construction when no element has embed text: each element still becomes a
   * child carrying its filter value (no vector), with an all-empty hash array. The children remain
   * so filter-only nested reads stay consistent and the array stays aligned with the source; they
   * simply won't match vector queries.
   */
  @Test
  public void buildMaterializedViewDocumentEvent_nestedAllEmpty_writesChildrenWithoutVectors()
      throws IOException {
    var schemaMetadata = nestedSchemaMetadata();
    VectorIndexDefinition indexDefinition = nestedIndexDefinition();

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_no_vec"))
            .append(
                "sections", new BsonArray(List.of(section("name_a", ""), section("name_b", ""))));
    DocumentEvent rawEvent = insertEventOf(rawDoc(source));

    // Embedding map is keyed by source path but empty for any candidate text — no vectors exist.
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(SECTION_CONTENT, ImmutableMap.of());

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawEvent,
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition),
            embeddings,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    BsonDocument matViewDoc = result.getDocument().get();
    BsonArray sections = matViewDoc.getDocument("_autoEmbed").getArray("sections");
    assertEquals(2, sections.size());
    assertEquals(new BsonString("name_a"), sections.get(0).asDocument().getString("section_name"));
    assertFalse(sections.get(0).asDocument().containsKey("section_content"));
    assertEquals(new BsonString("name_b"), sections.get(1).asDocument().getString("section_name"));
    assertFalse(sections.get(1).asDocument().containsKey("section_content"));

    BsonArray hashes =
        matViewDoc
            .getDocument("_autoEmbed")
            .getDocument("_hash")
            .getDocument("sections")
            .getArray("section_content");
    assertEquals(List.of(new BsonString(""), new BsonString("")), hashes.getValues());
  }

  /**
   * Element-wise resync comparison for multi-vector nested indexes: when the source matches the MV
   * produced by a full replace, no re-index is required and every element's vector is reused.
   */
  @Test
  public void compareDocuments_nestedMultiElement_noReindexAndReusesAllVectors()
      throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var sourceMapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(indexDefinition, schemaMetadata);

    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("n1"))
                            .append("section_content", new BsonString("c1")),
                        new BsonDocument()
                            .append("section_name", new BsonString("n2"))
                            .append("section_content", new BsonString("c2")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawSource)), rawSource);

    Vector v1 = Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    Vector v2 = Vector.fromFloats(new float[] {0.3f, 0.4f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(
            FieldPath.parse("sections.section_content"), ImmutableMap.of("c1", v1, "c2", v2));

    RawBsonDocument matViewDoc =
        new RawBsonDocument(
            buildMaterializedViewDocumentEvent(
                    rawEvent,
                    sourceMapping,
                    embeddings,
                    schemaMetadata,
                    indexDefinition.getNestedRoot())
                .getDocument()
                .get(),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var comparison =
        compareDocuments(
            rawSource,
            matViewDoc,
            sourceMapping,
            matViewMapping,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    assertFalse(
        "An unchanged multi-element nested doc must not be re-indexed.",
        comparison.needsReIndexing());
    var reusable = comparison.reusableEmbeddings().get(FieldPath.parse("sections.section_content"));
    assertEquals("Every element's vector must be reusable.", 2, reusable.size());
    assertEquals(v1, reusable.get("c1"));
    assertEquals(v2, reusable.get("c2"));
  }

  /**
   * Element-wise resync regression: multiple nested elements can carry identical embed text (e.g.
   * repeated content). The reuse map is keyed by text, so the duplicate must be deduped rather than
   * letting the builder throw on a duplicate key — which would otherwise force a spurious re-index.
   */
  @Test
  public void compareDocuments_nestedMultiElement_duplicateText_noReindex() throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var sourceMapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(indexDefinition, schemaMetadata);

    // Both elements carry identical embed text "dup" (different filter names).
    BsonDocument source =
        new BsonDocument()
            .append("_id", new BsonString("book_dup"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("n1"))
                            .append("section_content", new BsonString("dup")),
                        new BsonDocument()
                            .append("section_name", new BsonString("n2"))
                            .append("section_content", new BsonString("dup")))));
    RawBsonDocument rawSource = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawSource)), rawSource);

    Vector v1 = Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(FieldPath.parse("sections.section_content"), ImmutableMap.of("dup", v1));

    RawBsonDocument matViewDoc =
        new RawBsonDocument(
            buildMaterializedViewDocumentEvent(
                    rawEvent,
                    sourceMapping,
                    embeddings,
                    schemaMetadata,
                    indexDefinition.getNestedRoot())
                .getDocument()
                .get(),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var comparison =
        compareDocuments(
            rawSource,
            matViewDoc,
            sourceMapping,
            matViewMapping,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    assertFalse(
        "Duplicate embed text across elements must not force a re-index.",
        comparison.needsReIndexing());
    var reusable = comparison.reusableEmbeddings().get(FieldPath.parse("sections.section_content"));
    assertEquals(
        "The duplicate text must collapse to a single reusable entry.", 1, reusable.size());
    assertEquals(v1, reusable.get("dup"));
  }

  /**
   * Element-wise resync comparison for multi-vector nested indexes: a change to any element's embed
   * text triggers a re-index (the ordered per-element hashes diverge from the MV's).
   */
  @Test
  public void compareDocuments_nestedMultiElement_elementTextChange_triggersReindex()
      throws IOException {
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("sections.section_content"));
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedField, nestedFilter))
            .nestedRoot("sections")
            .build();
    var sourceMapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(indexDefinition, schemaMetadata);

    BsonDocument oldSource =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("n1"))
                            .append("section_content", new BsonString("c1")),
                        new BsonDocument()
                            .append("section_name", new BsonString("n2"))
                            .append("section_content", new BsonString("c2")))));
    RawBsonDocument rawOldSource = new RawBsonDocument(oldSource, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawOldEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawOldSource)), rawOldSource);
    Vector v1 = Vector.fromFloats(new float[] {0.1f, 0.2f}, FloatVector.OriginalType.NATIVE);
    Vector v2 = Vector.fromFloats(new float[] {0.3f, 0.4f}, FloatVector.OriginalType.NATIVE);
    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddings =
        ImmutableMap.of(
            FieldPath.parse("sections.section_content"), ImmutableMap.of("c1", v1, "c2", v2));
    RawBsonDocument matViewDoc =
        new RawBsonDocument(
            buildMaterializedViewDocumentEvent(
                    rawOldEvent,
                    sourceMapping,
                    embeddings,
                    schemaMetadata,
                    indexDefinition.getNestedRoot())
                .getDocument()
                .get(),
            BsonUtils.BSON_DOCUMENT_CODEC);

    // Second element's embed text changed; the MV is stale and must be re-indexed.
    BsonDocument newSource =
        new BsonDocument()
            .append("_id", new BsonString("book_1"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_name", new BsonString("n1"))
                            .append("section_content", new BsonString("c1")),
                        new BsonDocument()
                            .append("section_name", new BsonString("n2"))
                            .append("section_content", new BsonString("c2_changed")))));
    RawBsonDocument rawNewSource = new RawBsonDocument(newSource, BsonUtils.BSON_DOCUMENT_CODEC);

    var comparison =
        compareDocuments(
            rawNewSource,
            matViewDoc,
            sourceMapping,
            matViewMapping,
            schemaMetadata,
            indexDefinition.getNestedRoot());

    assertTrue(
        "Changing any element's embed text must trigger a re-index.", comparison.needsReIndexing());
  }
}
