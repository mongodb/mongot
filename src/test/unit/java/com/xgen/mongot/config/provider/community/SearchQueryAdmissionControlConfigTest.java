package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.Test;

public class SearchQueryAdmissionControlConfigTest {

  private static SearchQueryAdmissionControlConfig parse(BsonDocument doc)
      throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return SearchQueryAdmissionControlConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    SearchQueryAdmissionControlConfig original =
        new SearchQueryAdmissionControlConfig(2.0, 4.0, true, false);
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    SearchQueryAdmissionControlConfig original = SearchQueryAdmissionControlConfig.defaults();
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("threadPoolSizeMultiplier", new BsonDouble(2.0))
            .append("queueCapacityMultiplier", new BsonDouble(4.0))
            .append("virtualQueueCapacity", BsonBoolean.TRUE);
    SearchQueryAdmissionControlConfig parsed = parse(doc);
    assertEquals(2.0, parsed.threadPoolSizeMultiplier(), 0.0);
    assertEquals(4.0, parsed.queueCapacityMultiplier(), 0.0);
    assertTrue(parsed.virtualQueueCapacity());
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
    SearchQueryAdmissionControlConfig parsed = parse(doc);
    assertEquals(0.0, parsed.threadPoolSizeMultiplier(), 0.0);
    assertEquals(0.0, parsed.queueCapacityMultiplier(), 0.0);
  }

  @Test
  public void parse_unknownField_isIgnored() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("queueCapacityMultiplier", new BsonDouble(4.0))
            .append("notARealField", new BsonDouble(1.0));
    assertEquals(4.0, parse(doc).queueCapacityMultiplier(), 0.0);
  }

  @Test
  public void parse_shouldRetryViaLoadBalancerTrue_isExposed() throws BsonParseException {
    BsonDocument doc = new BsonDocument().append("shouldRetryViaLoadBalancer", BsonBoolean.TRUE);
    SearchQueryAdmissionControlConfig parsed = parse(doc);
    assertTrue(parsed.shouldRetryViaLoadBalancer());
  }

  @Test
  public void parse_shouldRetryViaLoadBalancerFalse_isExposed() throws BsonParseException {
    BsonDocument doc = new BsonDocument().append("shouldRetryViaLoadBalancer", BsonBoolean.FALSE);
    SearchQueryAdmissionControlConfig parsed = parse(doc);
    assertFalse(parsed.shouldRetryViaLoadBalancer());
  }

  @Test
  public void parse_shouldRetryViaLoadBalancerAbsent_defaultsToFalse() throws BsonParseException {
    BsonDocument doc = new BsonDocument();
    SearchQueryAdmissionControlConfig parsed = parse(doc);
    assertFalse(parsed.shouldRetryViaLoadBalancer());
  }

  @Test
  public void roundTrip_withShouldRetryViaLoadBalancerTrue_preservesAllFields()
      throws BsonParseException {
    SearchQueryAdmissionControlConfig original =
        new SearchQueryAdmissionControlConfig(2.0, 4.0, true, true);
    assertEquals(original, parse(original.toBson()));
  }
}
