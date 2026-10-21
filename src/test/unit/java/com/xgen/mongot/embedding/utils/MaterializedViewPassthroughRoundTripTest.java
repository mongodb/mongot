package com.xgen.mongot.embedding.utils;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Before;
import org.junit.Test;

/**
 * Round-trip regression test for CLOUDP-459586: with a dynamic mapping every source path is a
 * passthrough, so the materialized-view (MV) writer must copy the document verbatim for every BSON
 * type and container shape. The derived index ingests this copy through the same {@code
 * BsonDocumentProcessor} dispatch the direct index uses on the source document, so a dropped or
 * injected element would change what the derived index sees.
 *
 * <p>Before the CLOUDP-459586 fix this test fails: uuid array elements were duplicated and nested
 * numeric arrays carried injected vector binaries.
 */
public class MaterializedViewPassthroughRoundTripTest {

  private static final String AUTO_EMBED_MODEL = "voyage-3-large";

  private static final MaterializedViewSchemaMetadata SCHEMA_METADATA =
      new MaterializedViewSchemaMetadata(
          1, Map.of(FieldPath.parse("probeSource"), FieldPath.parse("_autoEmbed.probeSource")));

  @Before
  public void registerEmbeddingModels() {
    EmbeddingModelCatalogTestUtils.resetCatalogAndRegisterAutoEmbedModel(AUTO_EMBED_MODEL);
  }

  @Test
  public void dynamicPassthroughCopyIsVerbatimForEveryBsonTypeAndContainerShape()
      throws IOException {
    BsonDocument source =
        BsonDocument.parse(
            "{\"_id\": 0,\"s\": \"text\",\"i32\": 1,\"i64\": {\"$numberLong\":"
                + " \"4294967296\"},\"d\": 1.5,\"dec\": {\"$numberDecimal\": \"1.25\"},\"b\":"
                + " true,\"dt\": {\"$date\": {\"$numberLong\": \"1700000000000\"}},\"nul\":"
                + " null,\"ts\": {\"$timestamp\": {\"t\": 42, \"i\": 7}},\"rx\":"
                + " {\"$regularExpression\": {\"pattern\": \"^a\", \"options\": \"i\"}},\"code\":"
                + " {\"$code\": \"x = 1\"},\"minK\": {\"$minKey\": 1},\"maxK\": {\"$maxKey\":"
                + " 1},\"bin0\": {\"$binary\": {\"base64\": \"AAEC\", \"subType\":"
                + " \"00\"}},\"bin3\": {\"$binary\": {\"base64\": \"AAAAAAAAAAAAAAAAAAAAAA==\","
                + " \"subType\": \"03\"}},\"bin5\": {\"$binary\": {\"base64\": \"AAEC\","
                + " \"subType\": \"05\"}},\"bin128\": {\"$binary\": {\"base64\": \"AAEC\","
                + " \"subType\": \"80\"}},\"vec\": {\"$binary\": {\"base64\": \"JwAAACBBAAAgQQ==\","
                + " \"subType\": \"09\"}},\"uuidArr\": [  {\"$binary\": {\"base64\":"
                + " \"AAECAwQFBgcICQoLDA0ODw==\", \"subType\": \"04\"}},  {\"$binary\":"
                + " {\"base64\": \"Dw4NCwsJCAcGBQQDAwEAAA==\", \"subType\": \"04\"}}],\"binArr\":"
                + " [{\"$binary\": {\"base64\": \"AAEC\", \"subType\": \"00\"}}],\"nestedNumArr\":"
                + " [[1, 2], [3, 4]],\"emptyArr\": [],\"mixedArr\": [1, \"s\", null,"
                + " true],\"docArr\": [{\"k\": 1}, {\"k\": 2}],\"arrArrArr\": [[[1]],"
                + " [[2]]],\"deep\": {\"a\": {\"b\": {\"c\": [1, {\"d\": \"x\"}]}}},\"geo\":"
                + " {\"type\": \"MultiPoint\", \"coordinates\": [[10, 10], [20, 20]]}}");
    source.append(
        "oid", new org.bson.BsonObjectId(new org.bson.types.ObjectId("5f1f3a1b8d5e4a1b8c9d0e1f")));

    RawBsonDocument mv = materialize(source);

    assertThat(source.get("oid").getBsonType()).isEqualTo(org.bson.BsonType.OBJECT_ID);
    assertThat(source.get("dec").getBsonType()).isEqualTo(org.bson.BsonType.DECIMAL128);
    assertThat(source.get("ts").getBsonType()).isEqualTo(org.bson.BsonType.TIMESTAMP);
    assertThat(source.get("rx").getBsonType()).isEqualTo(org.bson.BsonType.REGULAR_EXPRESSION);
    assertThat(source.get("code").getBsonType()).isEqualTo(org.bson.BsonType.JAVASCRIPT);
    assertThat(source.get("minK").getBsonType()).isEqualTo(org.bson.BsonType.MIN_KEY);
    assertThat(source.get("maxK").getBsonType()).isEqualTo(org.bson.BsonType.MAX_KEY);
    BsonDocument actual = mv.decode(BsonUtils.BSON_DOCUMENT_CODEC);
    actual.remove("_id");
    assertThat(actual.get("oid").getBsonType()).isEqualTo(org.bson.BsonType.OBJECT_ID);
    BsonDocument expected = new BsonDocument();
    source.forEach(
        (k, v) -> {
          if (!k.equals("_id")) {
            expected.put(k, v);
          }
        });
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void vectorAutoEmbedFilterArraysAreCopiedVerbatim() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("plot")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tenantIds")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("coordinates")));
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder().setFields(fields).build();
    AutoEmbedFieldMapping fieldMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef);

    BsonDocument source =
        BsonDocument.parse(
            "{\"_id\": 0,\"plot\": \"hello world\",\"tenantIds\": [  {\"$binary\": {\"base64\":"
                + " \"AAECAwQFBgcICQoLDA0ODw==\", \"subType\": \"04\"}},  {\"$binary\":"
                + " {\"base64\": \"Dw4NCwsJCAcGBQQDAwEAAA==\", \"subType\":"
                + " \"04\"}}],\"coordinates\": [[10, 10], [20, 20]]}");

    RawBsonDocument sourceRaw = new RawBsonDocument(source, BsonUtils.BSON_DOCUMENT_CODEC);
    RawBsonDocument mv =
        buildMaterializedViewDocumentEvent(
                DocumentEvent.createInsert(
                    DocumentMetadata.fromOriginalDocument(Optional.of(sourceRaw)), sourceRaw),
                fieldMapping,
                ImmutableMap.of(),
                SCHEMA_METADATA)
            .getDocument()
            .get();

    BsonDocument actual = mv.decode(BsonUtils.BSON_DOCUMENT_CODEC);
    actual.remove("_id");

    BsonDocument expected =
        BsonDocument.parse(
            "{\"tenantIds\": [  {\"$binary\": {\"base64\": \"AAECAwQFBgcICQoLDA0ODw==\","
                + " \"subType\": \"04\"}},  {\"$binary\": {\"base64\":"
                + " \"Dw4NCwsJCAcGBQQDAwEAAA==\", \"subType\": \"04\"}}],\"coordinates\": [[10,"
                + " 10], [20, 20]]}");
    assertThat(actual).isEqualTo(expected);
  }

  private static RawBsonDocument materialize(BsonDocument sourceDoc) throws IOException {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("probe", autoEmbedProbe())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    AutoEmbedFieldMapping fieldMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);
    RawBsonDocument source = new RawBsonDocument(sourceDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    return buildMaterializedViewDocumentEvent(
            DocumentEvent.createInsert(
                DocumentMetadata.fromOriginalDocument(Optional.of(source)), source),
            fieldMapping,
            ImmutableMap.of(),
            SCHEMA_METADATA)
        .getDocument()
        .get();
  }

  private static com.xgen.mongot.index.definition.FieldDefinition autoEmbedProbe() {
    return FieldDefinitionBuilder.builder()
        .searchAutoEmbed(
            new SearchAutoEmbedFieldDefinition(AUTO_EMBED_MODEL, FieldPath.parse("probeSource")))
        .build();
  }
}
