package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;

public class CommunityCursorConfigTest {

  private static CommunityCursorConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityCursorConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    CommunityCursorConfig original =
        new CommunityCursorConfig(Optional.of(1800000), Optional.of(3600000));
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    CommunityCursorConfig original = new CommunityCursorConfig(Optional.empty(), Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("idleCursorHandlingRateMs", new BsonInt32(1800000))
            .append("cursorIdleTimeMs", new BsonInt32(3600000));
    CommunityCursorConfig parsed = parse(doc);
    assertEquals(Optional.of(1800000), parsed.idleCursorHandlingRateMs());
    assertEquals(Optional.of(3600000), parsed.cursorIdleTimeMs());
  }

  @Test
  public void parse_negativeCursorIdleTimeMs_throws() {
    BsonDocument doc = new BsonDocument("cursorIdleTimeMs", new BsonInt32(-1));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_unknownField_isIgnored() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("cursorIdleTimeMs", new BsonInt32(3600000))
            .append("notARealField", new BsonInt32(1));
    assertEquals(Optional.of(3600000), parse(doc).cursorIdleTimeMs());
  }
}
