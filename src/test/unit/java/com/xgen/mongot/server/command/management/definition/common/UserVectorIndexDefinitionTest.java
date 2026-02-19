package com.xgen.mongot.server.command.management.definition.common;

import static org.junit.Assert.assertEquals;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.server.command.management.definition.UserVectorIndexDefinitionBuilder;
import java.util.Optional;
import org.bson.BsonDocument;
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
        Optional.of(FieldPath.parse("sections")));
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
}
