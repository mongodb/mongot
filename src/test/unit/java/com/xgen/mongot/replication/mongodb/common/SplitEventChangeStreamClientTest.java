package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.Callable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for SplitEventChangeStreamClient.
 * 
 * <p>Tests fragment reassembly, batch processing optimizations, resume token management,
 * and error handling with mocked dependencies.
 */
public class SplitEventChangeStreamClientTest {

  @Mock
  private ChangeStreamMongoClient<RuntimeException> mockWrappedClient;
  
  private SplitEventChangeStreamClient<RuntimeException> client;
  private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();
  
  // Base prefix for resume tokens used in tests
  private static final String RESUME_TOKEN_PREFIX = "820000000000000";

  private static String firstToken() {
    return RESUME_TOKEN_PREFIX + "000010000";
  }
  
  private static String secondToken() {
    return RESUME_TOKEN_PREFIX + "000020000";
  }
  
  private static String thirdToken() {
    return RESUME_TOKEN_PREFIX + "000030000";
  }
  
  private static String tokenWithIncrement(int increment) {
    return String.format("%s%d00000000", RESUME_TOKEN_PREFIX, increment);
  }

  // Exception wrapper for tests
  private static final WrapIfThrows<RuntimeException> RUNTIME_EXCEPTION_WRAPPER =
      new WrapIfThrows<RuntimeException>() {
        @Override
        public <T> T wrapIfThrows(Callable<T> callable) throws RuntimeException {
          try {
            return callable.call();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

  private SimpleMeterRegistry meterRegistry;
  private MetricsFactory metricsFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Use real metrics for testing
    this.meterRegistry = new SimpleMeterRegistry();
    this.metricsFactory = new MetricsFactory("test", this.meterRegistry);

    this.client =
        new SplitEventChangeStreamClient<>(
            this.mockWrappedClient, RUNTIME_EXCEPTION_WRAPPER, this.metricsFactory,
            new GenerationId(new ObjectId(), Generation.CURRENT));
  }

  @Test
  public void testReturnsEmptyBatchWhenNoEventsAndNotBuffering() {
    // Client should return empty batches when not buffering to respect timeout behavior
    BsonDocument postBatchResumeToken = createResumeToken(firstToken());
    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    
    when(this.mockWrappedClient.getNext())
        .thenReturn(new ChangeStreamBatch(List.of(), postBatchResumeToken, operationTime));
    
    ChangeStreamBatch result = this.client.getNext();
    assertEquals(0, result.getRawEvents().size());
    assertEquals(postBatchResumeToken, result.getPostBatchResumeToken());
    assertEquals(operationTime, result.getCommandOperationTime());
    
    verify(this.mockWrappedClient, times(1)).getNext();
  }

  @Test
  public void testSkipsEmptyBatchesWhenBufferingFragments() {
    // Client continues polling through empty batches while buffering fragments
    String baseToken = firstToken();
    BsonDocument frag1 = createFragment(baseToken, 1, 2, "operationType", "insert");
    BsonDocument frag2 = createFragment(baseToken, 2, 2, "fullDocument", 
        new BsonDocument("_id", new BsonInt32(1)));
    
    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    
    when(this.mockWrappedClient.getNext())
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag1, CODEC)), 
            createResumeToken(baseToken + ".f1"), operationTime))
        .thenReturn(new ChangeStreamBatch(List.of(), createResumeToken("empty1"), operationTime))
        .thenReturn(new ChangeStreamBatch(List.of(), createResumeToken("empty2"), operationTime))
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag2, CODEC)), 
            createResumeToken(baseToken + ".f2"), operationTime));
    
    ChangeStreamBatch result = this.client.getNext();
    
    assertEquals(1, result.getRawEvents().size());
    BsonDocument reassembled = result.getRawEvents().get(0).decode(CODEC);
    assertEquals("insert", reassembled.getString("operationType").getValue());
    assertEquals(1, reassembled.getDocument("fullDocument").getInt32("_id").getValue());
    
    // Should have polled through empty batches to get both fragments
    verify(this.mockWrappedClient, times(4)).getNext();
  }

  @Test
  public void testReassemblesThreeFragmentEventAcrossMultipleBatches() {
    // Tests fragment reassembly when fragments arrive in separate batches
    String baseToken = firstToken();
    BsonDocument frag1 = createFragment(baseToken, 1, 3, "operationType", "insert");
    BsonDocument frag2 = createFragment(baseToken, 2, 3, "ns", 
        new BsonDocument("db", new BsonString("test")));
    BsonDocument frag3 = createFragment(baseToken, 3, 3, "fullDocument", 
        new BsonDocument("_id", new BsonInt32(1)));
    
    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    
    when(this.mockWrappedClient.getNext())
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag1, CODEC)), 
            createResumeToken(baseToken + ".f1"), operationTime))
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag2, CODEC)), 
            createResumeToken(baseToken + ".f2"), operationTime))
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag3, CODEC)), 
            createResumeToken(baseToken + ".f3"), operationTime));
    
    ChangeStreamBatch result = this.client.getNext();
    
    assertEquals(1, result.getRawEvents().size());
    BsonDocument reassembled = result.getRawEvents().getFirst().decode(CODEC);
    
    assertEquals("insert", reassembled.getString("operationType").getValue());
    assertEquals("test", reassembled.getDocument("ns").getString("db").getValue());
    assertEquals(1, reassembled.getDocument("fullDocument").getInt32("_id").getValue());
    
    assertEquals(createResumeToken(baseToken), reassembled.getDocument("_id"));
    assertFalse(reassembled.containsKey("splitEvent"));
    
    verify(this.mockWrappedClient, times(3)).getNext();
  }

  @Test
  public void testThrowsExceptionWhenFragmentSequenceIsInvalid() {
    // Client should fail fast when fragment sequence is invalid
    String token = firstToken();
    // Invalid: start with fragment 2/3 instead of 1/3
    BsonDocument invalidFrag = createFragment(token, 2, 3, "operationType", "insert");

    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    when(this.mockWrappedClient.getNext()).thenReturn(
        new ChangeStreamBatch(List.of(new RawBsonDocument(invalidFrag, CODEC)), 
            createResumeToken(token), operationTime));
    
    RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> this.client.getNext());
    assertThat(e.getCause()).isInstanceOf(FragmentProcessingException.class);
  }

  @Test
  public void testUsesPostBatchResumeTokenWhenNotBufferingFragments() {
    // When not buffering, use postBatchResumeToken directly for performance
    BsonDocument eventWithoutId =
        new BsonDocument()
            .append("operationType", new BsonString("insert"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    BsonDocument batchResumeToken = createResumeToken(firstToken());
    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    
    ChangeStreamBatch mockBatch = new ChangeStreamBatch(
        List.of(new RawBsonDocument(eventWithoutId, CODEC)),
        batchResumeToken,
        operationTime);
    when(this.mockWrappedClient.getNext()).thenReturn(mockBatch);
    
    // Should use batch resume token without decoding event
    ChangeStreamBatch result = this.client.getNext();
    assertEquals(1, result.getRawEvents().size());
    assertEquals(batchResumeToken, result.getPostBatchResumeToken());
  }

  @Test
  public void testThrowsExceptionWhenCompleteEventReceivedWhileBuffering() {
    // Complete event while buffering should trigger consecutive fragments exception
    String baseToken = firstToken();
    
    // Start buffering with first fragment
    BsonDocument frag1 = createFragment(baseToken, 1, 2, "operationType", "insert");
    
    // Complete event (not a fragment) 
    BsonDocument completeEvent =
        new BsonDocument()
            .append("_id", createResumeToken(secondToken()))
            .append("operationType", new BsonString("update"))
            .append("fullDocument", new BsonDocument("value", new BsonInt32(1)));

    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    
    when(this.mockWrappedClient.getNext())
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag1, CODEC)),
            createResumeToken(baseToken),
            operationTime))
        .thenReturn(new ChangeStreamBatch(
            List.of(new RawBsonDocument(completeEvent, CODEC)),
            createResumeToken(secondToken()),
            operationTime));
    
    // First call buffers fragment, second call should fail on consecutive fragments violation
    RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> {
      this.client.getNext(); // Buffer fragment
      this.client.getNext(); // Should throw on complete event while buffering
    });
    
    assertThat(e.getMessage()).contains("FragmentProcessingException");
    assertThat(e.getMessage()).contains("Received non-fragment event in FragmentBuffer");
  }

  @Test
  public void testOnlyChecksBoundaryEventsForFragments() {
    // Only first and last events in a batch need fragment checking (optimization)
    BsonDocument firstEvent =
        new BsonDocument()
            .append("_id", createResumeToken(firstToken()))
            .append("operationType", new BsonString("insert"));
    BsonDocument middleEvent1 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(1)))
            .append("operationType", new BsonString("update"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(1)));
    BsonDocument middleEvent2 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(2)))
            .append("operationType", new BsonString("update"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(2)));
    BsonDocument middleEvent3 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(3)))
            .append("operationType", new BsonString("update"))
            .append("fullDocument", new BsonDocument("_id", new BsonInt32(3)));
    BsonDocument lastEvent =
        createFragment(secondToken(), 1, 3, "operationType", "delete");

    List<RawBsonDocument> events =
        List.of(
            new RawBsonDocument(firstEvent, CODEC),
            new RawBsonDocument(middleEvent1, CODEC),
            new RawBsonDocument(middleEvent2, CODEC),
            new RawBsonDocument(middleEvent3, CODEC),
            new RawBsonDocument(lastEvent, CODEC));

    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    ChangeStreamBatch mockBatch =
        new ChangeStreamBatch(events, createResumeToken(tokenWithIncrement(4)), operationTime);

    when(this.mockWrappedClient.getNext()).thenReturn(mockBatch);

    ChangeStreamBatch result = this.client.getNext();

    // Should return first 4 complete events, last fragment gets buffered
    assertEquals(4, result.getRawEvents().size());

    BsonDocument firstResult = result.getRawEvents().get(0).decode(CODEC);
    assertEquals("insert", firstResult.getString("operationType").getValue());
    assertFalse(firstResult.containsKey("splitEvent"));

    // Middle events should be passed through without fragment checking
    for (int i = 1; i < result.getRawEvents().size(); i++) {
      BsonDocument event = result.getRawEvents().get(i).decode(CODEC);
      assertFalse(event.containsKey("splitEvent"));
      assertEquals("update", event.getString("operationType").getValue());
      assertEquals(i, event.getDocument("fullDocument").getInt32("_id").getValue());
    }
  }

  @Test
  public void testReturnsCompleteEventsInOrderAfterReassembly() {
    // Reassembled events should maintain correct ordering with other complete events
    String baseToken = firstToken();

    BsonDocument frag1 = createFragment(baseToken, 1, 2, "operationType", "insert");
    BsonDocument frag2 =
        createFragment(baseToken, 2, 2, "fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    // Fragments arrive in separate batches
    ChangeStreamBatch fragmentBatch1 =
        new ChangeStreamBatch(
            List.of(new RawBsonDocument(frag1, CODEC)), 
            createResumeToken(baseToken + ".f1"), 
            new BsonTimestamp(1, 1));
    
    // Complete events follow the final fragment
    BsonDocument complete1 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(1)))
            .append("operationType", new BsonString("update"));
    BsonDocument complete2 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(2)))
            .append("operationType", new BsonString("delete"));

    ChangeStreamBatch fragmentBatch2 =
        new ChangeStreamBatch(
            List.of(
                new RawBsonDocument(frag2, CODEC),
                new RawBsonDocument(complete1, CODEC),
                new RawBsonDocument(complete2, CODEC)),
            createResumeToken(tokenWithIncrement(2)),
            new BsonTimestamp(1, 1));

    when(this.mockWrappedClient.getNext()).thenReturn(fragmentBatch1).thenReturn(fragmentBatch2);

    // First call buffers fragment, second call returns reassembled event with complete events
    ChangeStreamBatch result = this.client.getNext();

    assertEquals(3, result.getRawEvents().size());
    
    // Reassembled event comes first
    BsonDocument reassembled = result.getRawEvents().get(0).decode(CODEC);
    assertFalse(reassembled.containsKey("splitEvent"));
    assertEquals("insert", reassembled.getString("operationType").getValue());
    assertEquals(1, reassembled.getDocument("fullDocument").getInt32("_id").getValue());
    
    // Complete events follow in order
    BsonDocument firstComplete = result.getRawEvents().get(1).decode(CODEC);
    BsonDocument secondComplete = result.getRawEvents().get(2).decode(CODEC);

    assertFalse(firstComplete.containsKey("splitEvent"));
    assertFalse(secondComplete.containsKey("splitEvent"));

    assertEquals("update", firstComplete.getString("operationType").getValue());
    assertEquals("delete", secondComplete.getString("operationType").getValue());
  }

  @Test
  public void testClosePropagatedToWrappedClient() {
    this.client.close();
    verify(this.mockWrappedClient).close();
  }

  @Test
  public void testLastCompleteEventBypassesBufferWhenBuffering() {
    // Optimization test: When buffering, complete events at last position should bypass buffer
    String baseToken = firstToken();

    BsonDocument frag1 = createFragment(baseToken, 1, 2, "operationType", "insert");

    BsonDocument frag2 =
        createFragment(baseToken, 2, 2, "fullDocument", new BsonDocument("_id", new BsonInt32(1)));
    BsonDocument complete1 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(1)))
            .append("operationType", new BsonString("update"));
    BsonDocument complete2 =
        new BsonDocument()
            .append("_id", createResumeToken(tokenWithIncrement(2)))
            .append("operationType", new BsonString("delete"));

    when(this.mockWrappedClient.getNext())
        .thenReturn(
            new ChangeStreamBatch(
                List.of(new RawBsonDocument(frag1, CODEC)),
                createResumeToken(baseToken),
                new BsonTimestamp(1, 1)))
        .thenReturn(
            new ChangeStreamBatch(
                List.of(
                    new RawBsonDocument(frag2, CODEC),
                    new RawBsonDocument(complete1, CODEC),
                    new RawBsonDocument(complete2, CODEC)),
                createResumeToken(tokenWithIncrement(2)),
                new BsonTimestamp(1, 1)));

    ChangeStreamBatch result = this.client.getNext();

    // All three events should be returned (reassembled + 2 complete)
    assertEquals(3, result.getRawEvents().size());

    // Verify complete2 at last position was not sent through buffer
    // otherwise an exception will be thrown: Received non-fragment event in FragmentBuffer
    BsonDocument lastEvent = result.getRawEvents().get(2).decode(CODEC);
    assertEquals("delete", lastEvent.getString("operationType").getValue());
    assertFalse(lastEvent.containsKey("splitEvent"));
  }

  @Test
  public void testFastPathOptimizationReturnsOriginalListWhenNoFragments() {
    // Fast path should return original list without copying when no fragments present
    BsonDocument event1 =
        new BsonDocument()
            .append("_id", createResumeToken(firstToken()))
            .append("operationType", new BsonString("insert"));
    BsonDocument event2 =
        new BsonDocument()
            .append("_id", createResumeToken(secondToken()))
            .append("operationType", new BsonString("update"));
    BsonDocument event3 =
        new BsonDocument()
            .append("_id", createResumeToken(thirdToken()))
            .append("operationType", new BsonString("delete"));

    List<RawBsonDocument> originalEvents =
        List.of(
            new RawBsonDocument(event1, CODEC),
            new RawBsonDocument(event2, CODEC),
            new RawBsonDocument(event3, CODEC));

    BsonTimestamp operationTime = new BsonTimestamp(1, 1);
    ChangeStreamBatch mockBatch =
        new ChangeStreamBatch(originalEvents, createResumeToken("batch1"), operationTime);

    when(this.mockWrappedClient.getNext()).thenReturn(mockBatch);

    ChangeStreamBatch result = this.client.getNext();

    // Should return same list reference (zero-copy optimization)
    assertThat(result.getRawEvents()).isSameInstanceAs(originalEvents);
    assertEquals(3, result.getRawEvents().size());
    
    verify(this.mockWrappedClient, times(1)).getNext();
  }


  @Test
  public void testResumeTokenPersistenceAcrossMultipleGetNextCalls() {
    // Resume tokens should persist correctly across calls with proper fragment sequence
    BsonDocument event1 =
        new BsonDocument()
            .append("_id", createResumeToken(firstToken()))
            .append("operationType", new BsonString("insert"));

    // Complete fragment sequence (1/2, 2/2)
    BsonDocument frag1 =
        createFragment(secondToken(), 1, 2, "operationType", "update");
    BsonDocument frag2 =
        createFragment(
            secondToken(), 2, 2, "fullDocument", new BsonDocument("_id", new BsonInt32(1)));

    // Another complete event
    BsonDocument event2 =
        new BsonDocument()
            .append("_id", createResumeToken(thirdToken()))
            .append("operationType", new BsonString("delete"));

    BsonTimestamp operationTime = new BsonTimestamp(1, 1);

    when(this.mockWrappedClient.getNext())
        .thenReturn(
            new ChangeStreamBatch(
                List.of(new RawBsonDocument(event1, CODEC)),
                createResumeToken("batch1"),
                operationTime))
        .thenReturn(
            new ChangeStreamBatch(
                List.of(new RawBsonDocument(frag1, CODEC)),
                createResumeToken("batch2"),
                operationTime))
        .thenReturn(
            new ChangeStreamBatch(
                List.of(new RawBsonDocument(frag2, CODEC)),
                createResumeToken("batch3"),
                operationTime))
        .thenReturn(
            new ChangeStreamBatch(
                List.of(new RawBsonDocument(event2, CODEC)),
                createResumeToken("batch4"),
                operationTime));

    // First call returns complete event with batch resume token
    ChangeStreamBatch result1 = this.client.getNext();
    assertEquals(createResumeToken("batch1"), result1.getPostBatchResumeToken());

    // Second call returns reassembled event using batch token (fragments complete)
    ChangeStreamBatch result2 = this.client.getNext();
    assertEquals(1, result2.getRawEvents().size()); // Just the reassembled event
    BsonDocument reassembled = result2.getRawEvents().get(0).decode(CODEC);
    assertEquals("update", reassembled.getString("operationType").getValue());
    assertEquals(createResumeToken("batch3"), result2.getPostBatchResumeToken());

    // Third call returns the separate complete event
    ChangeStreamBatch result3 = this.client.getNext();
    assertEquals(1, result3.getRawEvents().size());
    assertEquals(createResumeToken("batch4"), result3.getPostBatchResumeToken());

    verify(this.mockWrappedClient, times(4)).getNext();
  }

  private BsonDocument createResumeToken(String base) {
    return new BsonDocument()
        .append("_data", new BsonString(base))
        .append("clusterTime", new BsonInt32(1));
  }

  // Creates test fragment documents with splitEvent metadata

  private BsonDocument createFragment(
      String tokenBase, int fragment, int of, String fieldName, Object fieldValue) {
    BsonDocument doc = new BsonDocument()
        .append("_id", createResumeToken(tokenBase))
        .append("splitEvent", new BsonDocument()
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
