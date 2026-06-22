package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for FragmentBuffer. */
public class FragmentBufferTest {

  private FragmentBuffer buffer;
  private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

  @Before
  public void setUp() {
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    this.buffer = new FragmentBuffer(metricsFactory);
  }

  // Core Functionality Tests

  @Test
  public void testNonFragmentEventThrowsException() {
    // This  verifies the buffer correctly rejects non-fragments
    BsonDocument doc =
        new BsonDocument()
            .append("_id", createResumeToken("820000000000000000010000"))
            .append("operationType", new BsonString("insert"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    RawBsonDocument event = new RawBsonDocument(doc, CODEC);

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class, () -> this.buffer.processEvent(event));
    assertThat(e.getMessage()).contains("non-fragment event in FragmentBuffer");
    assertFalse(this.buffer.isBuffering());
  }

  // Fragment Reassembly Tests

  @Test
  public void testSingleEventReassembly() throws FragmentProcessingException {
    // Create 3 fragments of the same event
    String baseToken = "820000000000000000010000"; // Valid resume token hex

    // Fragment 1: contains operationType
    BsonDocument frag1 =
        new BsonDocument()
            .append("_id", createResumeToken(baseToken))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(3)))
            .append("operationType", new BsonString("insert"));

    // Fragment 2: contains ns
    BsonDocument frag2 =
        new BsonDocument()
            .append("_id", createResumeToken(baseToken))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(3)))
            .append(
                "ns",
                new BsonDocument()
                    .append("db", new BsonString("test"))
                    .append("coll", new BsonString("coll")));

    // Fragment 3: contains fullDocument
    BsonDocument frag3 =
        new BsonDocument()
            .append("_id", createResumeToken(baseToken))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(3))
                    .append("of", new BsonInt32(3)))
            .append(
                "fullDocument",
                new BsonDocument("_id", new BsonInt32(1))
                    .append("data", new BsonString("large data")));

    // Process fragments
    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();
    assertTrue(this.buffer.isBuffering());

    assertTrue(this.buffer.processEvent(new RawBsonDocument(frag2, CODEC)).isEmpty());
    assertTrue(this.buffer.isBuffering());

    Optional<RawBsonDocument> complete =
        this.buffer.processEvent(new RawBsonDocument(frag3, CODEC));
    assertTrue(complete.isPresent());
    assertFalse(this.buffer.isBuffering());

    // Verify reassembled event
    BsonDocument result = complete.get().decode(CODEC);
    assertFalse(result.containsKey("splitEvent"));
    assertEquals("insert", result.getString("operationType").getValue());
    assertEquals("test", result.getDocument("ns").getString("db").getValue());
    assertEquals("large data", result.getDocument("fullDocument").getString("data").getValue());
    // Should use fragment 3's resume token
    assertEquals(createResumeToken(baseToken), result.getDocument("_id"));
  }

  @Test
  public void testMultipleSequentialEvents() throws FragmentProcessingException {
    // First event (2 fragments)
    String tokenA = "820000000000000000010000"; // Valid resume token hex
    BsonDocument a1 = createFragment(tokenA, 1, 2, "operationType", "update");
    BsonDocument a2 =
        createFragment(tokenA, 2, 2, "fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    assertTrue(this.buffer.processEvent(new RawBsonDocument(a1, CODEC)).isEmpty());
    Optional<RawBsonDocument> completeA = this.buffer.processEvent(new RawBsonDocument(a2, CODEC));
    assertTrue(completeA.isPresent());
    assertFalse(this.buffer.isBuffering());

    // Second event (3 fragments)
    String tokenB = "820000000000000100000000"; // Valid resume token hex with different timestamp
    BsonDocument b1 = createFragment(tokenB, 1, 3, "operationType", "insert");
    BsonDocument b2 =
        createFragment(tokenB, 2, 3, "ns", new BsonDocument("db", new BsonString("test")));
    BsonDocument b3 =
        createFragment(tokenB, 3, 3, "fullDocument", new BsonDocument("_id", new BsonInt32(2)));

    assertTrue(this.buffer.processEvent(new RawBsonDocument(b1, CODEC)).isEmpty());
    assertTrue(this.buffer.processEvent(new RawBsonDocument(b2, CODEC)).isEmpty());
    Optional<RawBsonDocument> completeB = this.buffer.processEvent(new RawBsonDocument(b3, CODEC));
    assertTrue(completeB.isPresent());
    assertFalse(this.buffer.isBuffering());
  }

  // Error Condition Tests

  @Test
  public void testDifferentEventInterruption() throws FragmentProcessingException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FragmentBuffer buffer = new FragmentBuffer(new MetricsFactory("test", registry));

    // Start buffering event A
    String tokenA = "820000000000000000010000"; // Valid resume token hex
    BsonDocument a1 = createFragment(tokenA, 1, 3, "operationType", "insert");
    assertTrue(buffer.processEvent(new RawBsonDocument(a1, CODEC)).isEmpty());
    assertTrue(buffer.isBuffering());

    // Try to process fragment from different event B
    String tokenB = "820000000000000100000000"; // Valid resume token hex with different timestamp
    BsonDocument b1 = createFragment(tokenB, 1, 2, "operationType", "update");

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> buffer.processEvent(new RawBsonDocument(b1, CODEC)));
    assertThat(e.getMessage()).contains("different event");
    assertThat(registry.get("test.numFragmentOpTimeMismatches").counter().count()).isEqualTo(1.0);
  }

  @Test
  public void testNonFragmentWhileBuffering() throws FragmentProcessingException {
    // Start buffering
    String token = "820000000000000000010000"; // Valid resume token hex
    BsonDocument frag1 = createFragment(token, 1, 2, "operationType", "insert");
    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();

    // Try to process non-fragment event
    BsonDocument regular =
        new BsonDocument()
            .append("_id", createResumeToken("820000000000000200000000"))
            .append("operationType", new BsonString("update"));

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(regular, CODEC)));
    assertThat(e.getMessage()).contains("non-fragment event in FragmentBuffer");
  }

  @Test
  public void testInvalidFragmentMetadata() {
    // Fragment > of
    BsonDocument invalid1 =
        new BsonDocument()
            .append("_id", createResumeToken("820000000000000000010000"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(4))
                    .append("of", new BsonInt32(3)));

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(invalid1, CODEC)));
    assertThat(e.getMessage()).contains("Invalid fragment metadata");

    // Fragment < 1
    BsonDocument invalid2 =
        new BsonDocument()
            .append("_id", createResumeToken("820000000000000200000000"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(0))
                    .append("of", new BsonInt32(3)));

    FragmentProcessingException e2 =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(invalid2, CODEC)));
    assertThat(e2.getMessage()).contains("Invalid fragment metadata");
  }

  @Test
  public void testInconsistentFragmentCount() throws FragmentProcessingException {
    String token = "820000000000000000010000"; // Valid resume token hex
    BsonDocument frag1 = createFragment(token, 1, 3, "operationType", "insert");
    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();

    // Second fragment with different 'of' value
    BsonDocument frag2 =
        new BsonDocument()
            .append("_id", createResumeToken(token))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(4))) // Different 'of' value
            .append("ns", new BsonDocument("db", new BsonString("test")));

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(frag2, CODEC)));
    assertThat(e.getMessage()).contains("inconsistent fragment count");
  }

  // Diagnostic and Utility Tests

  @Test
  public void testClearBuffer() throws FragmentProcessingException {
    String token = "820000000000000000010000"; // Valid resume token hex
    BsonDocument frag1 = createFragment(token, 1, 2, "operationType", "insert");

    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();
    assertTrue(this.buffer.isBuffering());

    this.buffer.clear();
    assertFalse(this.buffer.isBuffering());

    // Should be able to start new event after clear
    String token2 = "820000000000000200000000"; // Valid resume token hex with different timestamp
    BsonDocument newFrag = createFragment(token2, 1, 2, "operationType", "update");
    assertTrue(this.buffer.processEvent(new RawBsonDocument(newFrag, CODEC)).isEmpty());
    assertTrue(this.buffer.isBuffering());
  }

  @Test
  public void testSingleFragmentEvent() throws FragmentProcessingException {
    // Edge case: event split into just 1 fragment (of=1)
    String token = "820000000000000000010000"; // Valid resume token hex
    BsonDocument frag =
        new BsonDocument()
            .append("_id", createResumeToken(token))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(1)))
            .append("operationType", new BsonString("insert"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    Optional<RawBsonDocument> complete = this.buffer.processEvent(new RawBsonDocument(frag, CODEC));
    assertTrue(complete.isPresent());
    assertFalse(this.buffer.isBuffering());

    // Should have removed splitEvent field
    BsonDocument result = complete.get().decode(CODEC);
    assertFalse(result.containsKey("splitEvent"));
    assertEquals("insert", result.getString("operationType").getValue());
  }

  @Test
  public void testDiagnosticApis() throws FragmentProcessingException {
    String token = "820000000000000000010000"; // Valid resume token hex

    // Initially no buffering
    assertThat(this.buffer.getCurrentEventOpTime()).isEmpty();
    assertEquals(0, this.buffer.getFragmentCount());
    assertEquals(0, this.buffer.getTotalFragments());

    // Start buffering
    BsonDocument frag1 = createFragment(token, 1, 3, "operationType", "insert");
    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();

    // Check diagnostic info
    assertTrue(this.buffer.getCurrentEventOpTime().isPresent());
    assertEquals(1, this.buffer.getFragmentCount());
    assertEquals(3, this.buffer.getTotalFragments());

    // Add second fragment
    BsonDocument frag2 =
        createFragment(token, 2, 3, "ns", new BsonDocument("db", new BsonString("test")));
    assertTrue(this.buffer.processEvent(new RawBsonDocument(frag2, CODEC)).isEmpty());

    assertEquals(2, this.buffer.getFragmentCount());
    assertEquals(3, this.buffer.getTotalFragments());

    // Complete the event
    BsonDocument frag3 =
        createFragment(token, 3, 3, "fullDocument", new BsonDocument("_id", new BsonInt32(1)));
    Optional<RawBsonDocument> complete =
        this.buffer.processEvent(new RawBsonDocument(frag3, CODEC));
    assertTrue(complete.isPresent());

    // After completion, should be reset
    assertThat(this.buffer.getCurrentEventOpTime()).isEmpty();
    assertEquals(0, this.buffer.getFragmentCount());
    assertEquals(0, this.buffer.getTotalFragments());
  }

  @Test
  public void testExceptionCleanup() throws FragmentProcessingException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FragmentBuffer buffer = new FragmentBuffer(new MetricsFactory("test", registry));

    String tokenA = "820000000000000000010000"; // Valid resume token hex
    String tokenB = "820000000000000100000000"; // Valid resume token hex with different timestamp

    // Start buffering event A
    BsonDocument a1 = createFragment(tokenA, 1, 3, "operationType", "insert");
    assertTrue(buffer.processEvent(new RawBsonDocument(a1, CODEC)).isEmpty());
    assertTrue(buffer.isBuffering());
    assertEquals(1, buffer.getFragmentCount());

    // Try to process fragment from different event B (should cause exception and cleanup)
    BsonDocument b1 = createFragment(tokenB, 1, 2, "operationType", "update");

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> buffer.processEvent(new RawBsonDocument(b1, CODEC)));
    assertThat(e.getMessage()).contains("different event");
    assertThat(registry.get("test.numFragmentOpTimeMismatches").counter().count()).isEqualTo(1.0);

    // Buffer should be cleared after exception
    assertFalse(buffer.isBuffering());
    assertThat(buffer.getCurrentEventOpTime()).isEmpty();
    assertEquals(0, buffer.getFragmentCount());
    assertEquals(0, buffer.getTotalFragments());

    // Should be able to start fresh after cleanup
    BsonDocument c1 = createFragment("820000000000000300000000", 1, 2, "operationType", "delete");
    assertTrue(buffer.processEvent(new RawBsonDocument(c1, CODEC)).isEmpty());
    assertTrue(buffer.isBuffering());
  }

  // Edge Case Tests

  @Test
  public void testSplitEventCounterIncrement() throws FragmentProcessingException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FragmentBuffer buffer = new FragmentBuffer(new MetricsFactory("test", registry));
    buffer.processEvent(
        new RawBsonDocument(
            createFragment("820000000000000000010000", 1, 2, "operationType", "insert"), CODEC));
    assertThat(registry.get("test.numSplitEvents").counter().count()).isEqualTo(1.0);
  }

  @Test
  public void testEmptyFragments() throws FragmentProcessingException {
    // Edge case: reassembled event with only _id field should throw
    String token = "820000000000000000010000"; // Valid resume token hex

    // Fragment 1: only has splitEvent (no actual data)
    BsonDocument frag1 =
        new BsonDocument()
            .append("_id", createResumeToken(token))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)));

    // Fragment 2: also only has splitEvent
    BsonDocument frag2 =
        new BsonDocument()
            .append("_id", createResumeToken(token))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(2)));

    // Process first fragment
    assertThat(this.buffer.processEvent(new RawBsonDocument(frag1, CODEC))).isEmpty();

    // Second fragment should trigger exception due to empty reassembly
    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(frag2, CODEC)));
    assertThat(e.getMessage()).contains("Reassembled event contains only _id field");
    // Buffer should be cleared after exception
    assertFalse(this.buffer.isBuffering());
  }

  @Test
  public void testMissingResumeToken() {
    // Create fragment with null _id field to test null safety
    BsonDocument fragWithoutId =
        new BsonDocument()
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)))
            .append("operationType", new BsonString("insert"));

    FragmentProcessingException e =
        Assert.assertThrows(
            FragmentProcessingException.class,
            () -> this.buffer.processEvent(new RawBsonDocument(fragWithoutId, CODEC)));
    assertThat(e.getMessage()).contains("Missing resume token");
  }

  // Helper Methods

  private BsonDocument createResumeToken(String base) {
    return new BsonDocument()
        .append("_data", new BsonString(base))
        .append("clusterTime", new BsonInt32(1));
  }

  private BsonDocument createFragment(
      String tokenBase, int fragment, int of, String fieldName, Object fieldValue) {
    BsonDocument doc =
        new BsonDocument()
            .append("_id", createResumeToken(tokenBase))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(fragment))
                    .append("of", new BsonInt32(of)));

    if (fieldValue instanceof String) {
      doc.append(fieldName, new BsonString((String) fieldValue));
    } else if (fieldValue instanceof BsonDocument) {
      doc.append(fieldName, (BsonDocument) fieldValue);
    }

    return doc;
  }
}
