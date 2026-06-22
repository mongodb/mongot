package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.Test;

public class RegularBlockingRequestConfigTest {

  private static RegularBlockingRequestConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return RegularBlockingRequestConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    RegularBlockingRequestConfig original =
        new RegularBlockingRequestConfig(Optional.of(2.0), Optional.of(4.0), Optional.of(true));
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    RegularBlockingRequestConfig original =
        new RegularBlockingRequestConfig(Optional.empty(), Optional.empty(), Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("threadPoolSizeMultiplier", new BsonDouble(2.0))
            .append("queueCapacityMultiplier", new BsonDouble(4.0))
            .append("virtualQueueCapacity", BsonBoolean.TRUE);
    RegularBlockingRequestConfig parsed = parse(doc);
    assertEquals(Optional.of(2.0), parsed.threadPoolSizeMultiplier());
    assertEquals(Optional.of(4.0), parsed.queueCapacityMultiplier());
    assertEquals(Optional.of(true), parsed.virtualQueueCapacity());
  }

  @Test
  public void parse_wrongType_throws() {
    BsonDocument doc = new BsonDocument("threadPoolSizeMultiplier", new BsonString("not-a-double"));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeThreadPoolSizeMultiplier_throws() {
    BsonDocument doc = new BsonDocument("threadPoolSizeMultiplier", new BsonDouble(-1.0));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeQueueCapacityMultiplier_throws() {
    BsonDocument doc = new BsonDocument("queueCapacityMultiplier", new BsonDouble(-0.5));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_zeroMultiplier_isAccepted() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("threadPoolSizeMultiplier", new BsonDouble(0.0))
            .append("queueCapacityMultiplier", new BsonDouble(0.0));
    RegularBlockingRequestConfig parsed = parse(doc);
    assertEquals(Optional.of(0.0), parsed.threadPoolSizeMultiplier());
    assertEquals(Optional.of(0.0), parsed.queueCapacityMultiplier());
  }

  @Test
  public void parse_unknownField_isIgnored() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("queueCapacityMultiplier", new BsonDouble(4.0))
            .append("notARealField", new BsonDouble(1.0));
    assertEquals(Optional.of(4.0), parse(doc).queueCapacityMultiplier());
  }
}
