package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.jetbrains.annotations.TestOnly;

/**
 * Reassembles change stream events that exceed 16MB and have been split into fragments.
 *
 * <p>Used by SplitEventChangeStreamClient when processing change streams with the
 * $changeStreamSplitLargeEvent stage. Fragments are identified by the "splitEvent" field.
 *
 * <p><b>Usage:</b> Called via #processEvent(RawBsonDocument) when SplitEventChangeStreamClient
 * encounters events with "splitEvent" metadata during batch processing.
 *
 * <p><b>Important:</b> This class only accepts fragment events (events containing a "splitEvent"
 * field). Complete events should not be sent to this buffer - they will cause a
 * FragmentProcessingException to be thrown.
 *
 * <p><b>Design:</b> Single buffer approach since fragments arrive sequentially from mongod without
 * interleaving. Fragments must arrive in order (1, 2, 3...). Uses resume token opTime to verify
 * fragments belong to the same event.
 */
public class FragmentBuffer {

  private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

  private final List<Fragment> fragments = new ArrayList<>();
  private int totalFragments;
  private Optional<BsonTimestamp> currentEventOpTime = Optional.empty();
  private final Counter splitEventsCounter;
  private final Counter fragmentOpTimeMismatchCounter;

  public FragmentBuffer(MetricsFactory metricsFactory) {
    // Fragment exceptions are tracked as process-level metrics in ReplicationIndexManager
    this.splitEventsCounter = metricsFactory.counter("numSplitEvents");
    this.fragmentOpTimeMismatchCounter = metricsFactory.counter("numFragmentOpTimeMismatches");
  }

  /** Internal record to store the decoded representation of a fragment. */
  private record Fragment(BsonDocument decoded) {}

  /**
   * Process an incoming change stream event.
   *
   * @return Complete event if reassembled, empty if still buffering
   */
  public Optional<RawBsonDocument> processEvent(RawBsonDocument event)
      throws FragmentProcessingException {
    try {
      return processEventInternal(event);
    } catch (Exception e) {
      // Clear buffer to ensure clean state for non-resumable errors
      clear();
      if (e instanceof FragmentProcessingException fragmentProcessingException) {
        throw fragmentProcessingException;
      }
      // Wrap unexpected exceptions for consistent error handling
      throw new FragmentProcessingException(
          "Fragment processing failed due to unexpected error: " + e.getMessage(), e);
    }
  }

  private Optional<RawBsonDocument> processEventInternal(RawBsonDocument event)
      throws FragmentProcessingException {
    BsonDocument doc = event.decode(CODEC);
    BsonDocument splitEvent = doc.getDocument("splitEvent", null);

    if (splitEvent == null) {
      // This should not happen - the client should only send fragments to the buffer
      throw new FragmentProcessingException(
          "Received non-fragment event in FragmentBuffer. This should not happen - "
              + "the client should only send fragments to the buffer.");
    }

    // Extract fragment metadata
    int fragment = splitEvent.getInt32("fragment").getValue();
    int of = splitEvent.getInt32("of").getValue();

    // Validate fragment metadata
    if (fragment < 1 || fragment > of || of < 1) {
      throw new FragmentProcessingException(
          String.format("Invalid fragment metadata: fragment=%d, of=%d", fragment, of));
    }

    // Extract opTime for this event
    BsonTimestamp eventOpTime = extractEventOpTime(doc);

    if (!isBuffering()) {
      // Start buffering new event - record split event
      this.splitEventsCounter.increment();
      this.totalFragments = of;
      this.currentEventOpTime = Optional.of(eventOpTime);
    } else {
      // Verify this fragment belongs to current event
      if (!isSameEvent(eventOpTime)) {
        this.fragmentOpTimeMismatchCounter.increment();
        throw new FragmentProcessingException(
            "Received fragment from different event while already buffering fragments");
      }

      // Verify consistent total fragments
      if (of != this.totalFragments) {
        throw new FragmentProcessingException(
            String.format(
                "Invalid fragment metadata: inconsistent fragment count. "
                    + "Expected 'of'=%d but got 'of'=%d",
                this.totalFragments, of));
      }
    }

    // Verify fragment sequence, fragments must arrive in order
    if (fragment != this.fragments.size() + 1) {
      throw new FragmentProcessingException(
          String.format(
              "Expected fragment %d but received fragment %d",
              this.fragments.size() + 1, fragment));
    }

    this.fragments.add(new Fragment(doc));

    // Check if all fragments received
    if (this.fragments.size() == this.totalFragments) {
      RawBsonDocument completeEvent = reassemble();
      clear();
      return Optional.of(completeEvent);
    }

    // Still waiting for more fragments
    return Optional.empty();
  }

  /**
   * Check if currently buffering fragments.
   *
   * @return true if currently buffering fragments
   */
  public boolean isBuffering() {
    return !this.fragments.isEmpty();
  }

  /** Clear the buffer. */
  public void clear() {
    this.fragments.clear();
    this.totalFragments = 0;
    this.currentEventOpTime = Optional.empty();
  }

  /** Extract opTime from resume token to identify fragments of the same event. */
  private BsonTimestamp extractEventOpTime(BsonDocument doc) throws FragmentProcessingException {
    BsonDocument resumeToken = doc.getDocument("_id", null);
    if (resumeToken == null) {
      throw new FragmentProcessingException("Missing resume token in change stream event");
    }

    try {
      return ResumeTokenUtils.opTimeFromResumeToken(resumeToken);
    } catch (DecoderException e) {
      throw new FragmentProcessingException("Failed to extract opTime from resume token", e);
    } catch (IllegalStateException e) {
      throw new FragmentProcessingException("Invalid resume token format: " + e.getMessage(), e);
    }
  }

  /** Check if event opTime matches current buffered event. */
  private boolean isSameEvent(BsonTimestamp eventOpTime) {
    return this.currentEventOpTime.isPresent() && this.currentEventOpTime.get().equals(eventOpTime);
  }

  /**
   * Reassemble fragments into complete event. Field merging: first fragment wins for duplicates.
   * Uses final fragment's resume token.
   */
  private RawBsonDocument reassemble() throws FragmentProcessingException {
    // Start with first fragment for "first wins" policy
    if (this.fragments.isEmpty()) {
      throw new IllegalStateException("fragments list should not be empty in reassemble()");
    }
    BsonDocument result = this.fragments.getFirst().decoded().clone();
    result.remove("splitEvent");

    // Add fields from other fragments (skipping duplicates)
    for (int i = 1; i < this.fragments.size(); i++) {
      BsonDocument fragment = this.fragments.get(i).decoded();

      for (String key : fragment.keySet()) {
        if (!key.equals("_id") && !key.equals("splitEvent") && !result.containsKey(key)) {
          result.put(key, fragment.get(key));
        }
      }
    }

    // Use last fragment's resume token
    result.put("_id", this.fragments.getLast().decoded().get("_id"));

    if (result.size() == 1 && result.containsKey("_id")) {
      // This should never happen
      throw new FragmentProcessingException("Reassembled event contains only _id field");
    }

    return new RawBsonDocument(result, CODEC);
  }

  /** Get current event opTime (for debugging). */
  @TestOnly
  @VisibleForTesting
  public Optional<BsonTimestamp> getCurrentEventOpTime() {
    return this.currentEventOpTime;
  }

  /** Get number of fragments buffered. */
  @TestOnly
  @VisibleForTesting
  public int getFragmentCount() {
    return this.fragments.size();
  }

  /** Get total expected fragments. */
  @TestOnly
  @VisibleForTesting
  public int getTotalFragments() {
    return this.totalFragments;
  }
}
