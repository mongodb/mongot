package com.xgen.mongot.index.query.operators;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

public class VectorEmbeddedOptionsTest {

  @Test
  public void testParseMaxScoreMode() throws BsonParseException {
    BsonDocument doc = new BsonDocument("scoreMode", new BsonString("max"));
    VectorEmbeddedOptions options =
        VectorEmbeddedOptions.fromBson(BsonDocumentParser.fromRoot(doc).build());

    assertThat(options.scoreMode()).isEqualTo(VectorEmbeddedOptions.ScoreMode.MAX);
  }

  @Test
  public void testParseAvgScoreMode() throws BsonParseException {
    BsonDocument doc = new BsonDocument("scoreMode", new BsonString("avg"));
    VectorEmbeddedOptions options =
        VectorEmbeddedOptions.fromBson(BsonDocumentParser.fromRoot(doc).build());

    assertThat(options.scoreMode()).isEqualTo(VectorEmbeddedOptions.ScoreMode.AVG);
  }

  @Test
  public void testParseInvalidScoreMode() {
    BsonDocument doc = new BsonDocument("scoreMode", new BsonString("sum"));
    BsonDocumentParser parser = BsonDocumentParser.fromRoot(doc).build();

    BsonParseException e =
        assertThrows(BsonParseException.class, () -> VectorEmbeddedOptions.fromBson(parser));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "scoreMode: \"sum\" is not supported. Accepted values are \"max\" or \"avg\"");
  }

  @Test
  public void testParseMissingScoreMode() throws BsonParseException {
    BsonDocument doc = new BsonDocument();
    BsonDocumentParser parser = BsonDocumentParser.fromRoot(doc).build();

    // Should use default value (MAX)
    VectorEmbeddedOptions options = VectorEmbeddedOptions.fromBson(parser);
    assertThat(options.scoreMode()).isEqualTo(VectorEmbeddedOptions.ScoreMode.MAX);
  }

  @Test
  public void testSerializeMaxScoreMode() {
    VectorEmbeddedOptions options = new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX);
    BsonDocument doc = options.toBson();

    assertThat(doc.getString("scoreMode").getValue()).isEqualTo("max");
  }

  @Test
  public void testSerializeAvgScoreMode() {
    VectorEmbeddedOptions options = new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.AVG);
    BsonDocument doc = options.toBson();

    assertThat(doc.getString("scoreMode").getValue()).isEqualTo("avg");
  }

  @Test
  public void testRoundTripMax() throws BsonParseException {
    VectorEmbeddedOptions original =
        new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX);
    BsonDocument doc = original.toBson();
    VectorEmbeddedOptions parsed =
        VectorEmbeddedOptions.fromBson(BsonDocumentParser.fromRoot(doc).build());

    assertThat(parsed).isEqualTo(original);
  }

  @Test
  public void testRoundTripAvg() throws BsonParseException {
    VectorEmbeddedOptions original =
        new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.AVG);
    BsonDocument doc = original.toBson();
    VectorEmbeddedOptions parsed =
        VectorEmbeddedOptions.fromBson(BsonDocumentParser.fromRoot(doc).build());

    assertThat(parsed).isEqualTo(original);
  }
}

