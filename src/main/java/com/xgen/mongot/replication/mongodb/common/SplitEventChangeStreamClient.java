package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reassembles fragmented change stream events that exceed size limits.
 *
 * <p>Wraps a change stream client to handle events split across multiple fragments.
 */
public class SplitEventChangeStreamClient<E extends Exception>
    implements ChangeStreamMongoClient<E> {
  
  private static final Logger LOG = LoggerFactory.getLogger(SplitEventChangeStreamClient.class);

  private final ChangeStreamMongoClient<E> wrapped;
  private final FragmentBuffer fragmentBuffer;
  private final WrapIfThrows<E> exceptionWrapper;
  private final GenerationId generationId;
  private boolean splitEventLogged = false;

  public SplitEventChangeStreamClient(
      ChangeStreamMongoClient<E> wrapped,
      WrapIfThrows<E> exceptionWrapper,
      MetricsFactory metricsFactory,
      GenerationId generationId) {
    this.wrapped = wrapped;
    this.fragmentBuffer = new FragmentBuffer(metricsFactory);
    this.exceptionWrapper = exceptionWrapper;
    this.generationId = generationId;
  }

  @Override
  public ChangeStreamBatch getNext() throws E {
    // Poll until complete events or empty batch needed
    while (true) {
      ChangeStreamBatch originalBatch = this.wrapped.getNext();
      
      CompletedEventBatch processedResult = processBatch(originalBatch);

      if (!processedResult.events().isEmpty()) {
        // Resume token guaranteed when returning events
        BsonDocument resumeToken =
            processedResult
                .resumeToken()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "internal error: resume token must be present when returning"
                                + " complete events"));

        return new ChangeStreamBatch(
            processedResult.events(), resumeToken, originalBatch.getCommandOperationTime());
      }

      // Return empty batch when not buffering
      if (!this.fragmentBuffer.isBuffering()) {
        return new ChangeStreamBatch(
            processedResult.events(),
            originalBatch.getPostBatchResumeToken(),
            originalBatch.getCommandOperationTime());
      }
      // Continue polling while buffering
    }
  }

  @Override
  public void close() {
    this.wrapped.close();
  }

  /** Processes batch events. */
  private CompletedEventBatch processBatch(ChangeStreamBatch batch) throws E {
    List<RawBsonDocument> events = batch.getRawEvents();
    if (canSkipFragmentProcessing(events)) {
      return new CompletedEventBatch(events, Optional.of(batch.getPostBatchResumeToken()));
    }

    return processWithFragments(events, batch.getPostBatchResumeToken());
  }

  /** Process events that may contain fragments. */
  private CompletedEventBatch processWithFragments(
      List<RawBsonDocument> events, BsonDocument batchResumeToken) throws E {
    return this.exceptionWrapper.wrapIfThrows(
        () -> {
          // Preallocate enough space to avoid resizing during list operations
          int eventCount = events.size();
          List<RawBsonDocument> modifiedEvents = new ArrayList<>(eventCount);

          // Only boundary events might be fragments: first (when buffering) and last
          for (int i = 0; i < eventCount; i++) {
            RawBsonDocument event = events.get(i);
            if ((i == 0 && this.fragmentBuffer.isBuffering())
                || (i == eventCount - 1 && event.containsKey("splitEvent"))) {
              // Process a fragment and add any complete event to results
              this.fragmentBuffer
                  .processEvent(event)
                  .ifPresent(
                      completeEvent -> {
                        if (!this.splitEventLogged) {
                          LOG.atInfo()
                              .addKeyValue("generationId", this.generationId)
                              .log("Successfully reassembled split event");
                          this.splitEventLogged = true;
                        }
                        modifiedEvents.add(completeEvent);
                      });
            } else {
              // Middle events are always complete (never fragments)
              modifiedEvents.add(event);
            }
          }

          // Determine the resume token
          Optional<BsonDocument> resumeToken =
              determineResumeToken(modifiedEvents, batchResumeToken);

          // Convert the event list back to immutable
          return new CompletedEventBatch(List.copyOf(modifiedEvents), resumeToken);
        });
  }

  /** Determines resume token based on buffering state. */
  private Optional<BsonDocument> determineResumeToken(
      List<RawBsonDocument> resultEvents, BsonDocument batchResumeToken)
      throws FragmentProcessingException {

    // When not buffering, always use batch resume token
    if (!this.fragmentBuffer.isBuffering()) {
      return Optional.of(batchResumeToken);
    }

    // Handle buffering scenarios
    return resultEvents.isEmpty()
        ? Optional.empty() // When buffering with no complete events, return empty
        : Optional.of(extractResumeTokenFromEvent(resultEvents.getLast()));
    // When buffering with complete events, extract token from last event
  }

  /** Extracts resume token from event. */
  private BsonDocument extractResumeTokenFromEvent(RawBsonDocument event)
      throws FragmentProcessingException {
    var token = event.get("_id");
    if (token == null) {
      throw new FragmentProcessingException("change stream event missing resume token _id field");
    }
    return token.asDocument();
  }

  @VisibleForTesting
  boolean canSkipFragmentProcessing(List<RawBsonDocument> events) {
    // Fragment handling can be skipped under the following conditions:
    // 1. There are no events, OR
    // 2. The fragment buffer is not actively buffering and the last event doesn't contain a
    // fragment.
    // Note: It's sufficient to check only the last event because a fragment would exist either at
    // the beginning or the end of the list. The first event does not require checking
    // because a fragment followed by an event ([fragment, event]) can only occur when the
    // fragmentBuffer is currently buffering.
    return events.isEmpty()
        || (!this.fragmentBuffer.isBuffering() && !events.getLast().containsKey("splitEvent"));
  }

  private record CompletedEventBatch(
      List<RawBsonDocument> events, Optional<BsonDocument> resumeToken) {}
}
