package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.config.provider.community.TracingConfig.SamplingStrategy;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.Test;

public class TracingConfigTest {

  private static TracingConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return TracingConfig.fromBson(parser);
    }
  }

  @Test
  public void parse_fullBlock_preservesSettingsAndRoundTrips() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("enabled", new BsonBoolean(true))
            .append("searchSamplingStrategy", new BsonString("head"))
            .append("vectorSamplingStrategy", new BsonString("tail"))
            .append("searchTracingSamplerRate", new BsonDouble(0.25))
            .append("vectorTracingSamplerRate", new BsonDouble(0.5));

    TracingConfig config = parse(doc);

    assertThat(config.enabled()).isTrue();
    assertThat(config.searchSamplingStrategy()).isEqualTo(SamplingStrategy.HEAD);
    assertThat(config.vectorSamplingStrategy()).isEqualTo(SamplingStrategy.TAIL);
    assertThat(config.searchTracingSamplerRate()).isEqualTo(0.25);
    assertThat(config.vectorTracingSamplerRate()).isEqualTo(0.5);
    assertThat(parse(config.toBson())).isEqualTo(config);
  }

  @Test
  public void parse_emptyBlock_usesDisabledDefaults() throws BsonParseException {
    TracingConfig config = parse(new BsonDocument());

    assertThat(config.enabled()).isFalse();
    assertThat(config.searchSamplingStrategy()).isEqualTo(SamplingStrategy.TAIL);
    assertThat(config.vectorSamplingStrategy()).isEqualTo(SamplingStrategy.HEAD);
    assertThat(config.searchTracingSamplerRate()).isEqualTo(0.01);
    assertThat(config.vectorTracingSamplerRate()).isEqualTo(0.01);
  }

  @Test
  public void parse_invalidValues_areRejected() {
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("searchTracingSamplerRate", new BsonDouble(1.5))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("vectorTracingSamplerRate", new BsonDouble(-0.1))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("searchSamplingStrategy", new BsonString("nonsense"))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("vectorSamplingStrategy", new BsonString("nonsense"))));
  }
}
