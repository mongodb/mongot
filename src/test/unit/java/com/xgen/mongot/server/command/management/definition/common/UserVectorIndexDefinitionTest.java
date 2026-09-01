package com.xgen.mongot.server.command.management.definition.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.IndexNamespaceDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSearchEngine;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.server.command.management.definition.UserVectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UserVectorIndexDefinition} with nestedRoot (user-facing createIndex API). */
@RunWith(JUnit4.class)
public class UserVectorIndexDefinitionTest {

  private static UserVectorIndexDefinition definitionWithNestedRoot() {
    UserVectorIndexDefinition withoutNested =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(
                128, VectorSimilarity.COSINE, VectorQuantization.NONE, "sections.embedding")
            .addFilterField("sections.name")
            .build();
    return new UserVectorIndexDefinition(
        withoutNested.fields(),
        withoutNested.numPartitions(),
        withoutNested.storedSource(),
        Optional.of(FieldPath.parse("sections")),
        Optional.empty());
  }

  @Test
  public void testNestedRoot_roundTrip() throws BsonParseException {
    UserVectorIndexDefinition original = definitionWithNestedRoot();
    BsonDocument bson = original.toBson();

    Truth.assertThat(bson.containsKey("nestedRoot")).isTrue();
    assertEquals("sections", bson.getString("nestedRoot").getValue());

    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      UserVectorIndexDefinition parsed = UserVectorIndexDefinition.fromBson(parser);
      assertEquals(original, parsed);
    }
  }

  @Test
  public void testNestedRoot_absent_omittedFromBson() throws BsonParseException {
    UserVectorIndexDefinition definition =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(
                128, VectorSimilarity.COSINE, VectorQuantization.NONE, "my.vector.field")
            .build();

    BsonDocument bson = definition.toBson();
    Truth.assertThat(bson.containsKey("nestedRoot")).isFalse();

    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      UserVectorIndexDefinition parsed = UserVectorIndexDefinition.fromBson(parser);
      Truth.assertThat(parsed.nestedRoot()).isEmpty();
    }
  }

  private static UserVectorIndexDefinition definitionWithIndexNamespace() {
    return new UserVectorIndexDefinition(
        List.of(
            new VectorDataFieldDefinition(
                FieldPath.parse("embedding"),
                new VectorFieldSpecification(
                    128,
                    VectorSimilarity.COSINE,
                    VectorQuantization.NONE,
                    new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
                    VectorSearchEngine.CUSTOM))),
        1,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new IndexNamespaceDefinition(FieldPath.parse("tenantId"))));
  }

  @Test
  public void testIndexNamespace_roundTrip() throws BsonParseException {
    UserVectorIndexDefinition original = definitionWithIndexNamespace();
    BsonDocument bson = original.toBson();

    Truth.assertThat(bson.containsKey("indexNamespace")).isTrue();
    assertEquals("tenantId", bson.getDocument("indexNamespace").getString("path").getValue());

    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      UserVectorIndexDefinition parsed = UserVectorIndexDefinition.fromBson(parser);
      assertEquals(original, parsed);
    }
  }

  @Test
  public void testIndexNamespace_withoutCustomEngine_rejected() {
    BsonDocument bson =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build()
            .toBson()
            .append("indexNamespace", new BsonDocument("path", new BsonString("tenantId")));

    BsonParseException thrown =
        assertThrows(
            BsonParseException.class,
            () -> {
              try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
                UserVectorIndexDefinition.fromBson(parser);
              }
            });
    Truth.assertThat(thrown).hasMessageThat().contains("custom vector engine");
  }
}
