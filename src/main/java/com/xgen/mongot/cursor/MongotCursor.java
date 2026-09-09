package com.xgen.mongot.cursor;

import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.trace.SpanGuard;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceFlags;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import org.bson.BsonArray;

/** Responsible for returning MongotCursorResultInfo's. Associated with an id. */
class MongotCursor {

  private final long id;
  private final BatchProducer batchProducer;
  private final String namespace;
  private final BatchSizeStrategy batchSizeStrategy;
  private volatile Instant lastOperationTime;
  private final Optional<ExplainQueryState> explainQueryState;

  @GuardedBy("this")
  private boolean closed;

  private int count;

  private final String traceId;
  private final TraceFlags traceFlags;

  /**
   * Constructs a new Cursor with the given id for the supplied batchProducer, which must be over
   * the supplied index.
   */
  MongotCursor(
      long id, BatchProducer batchProducer, String namespace, BatchSizeStrategy batchSizeStrategy) {
    this.id = id;
    this.batchProducer = batchProducer;
    this.namespace = namespace;
    this.batchSizeStrategy = batchSizeStrategy;
    this.lastOperationTime = Instant.now();
    // Initialize with explain state if explain query
    this.explainQueryState = Explain.getExplainQueryState();
    this.closed = false;
    this.count = 1;
    this.traceId = Span.current().getSpanContext().getTraceId();
    this.traceFlags = Span.current().getSpanContext().getTraceFlags();
  }

  /**
   * Returns cursor batch, containing search results and metadata. The object is measured in size
   * and expected to be encoded without appending any additional data.
   *
   * <p>If Explain is enabled on the cursor, sets the ExplainQueryState after the initial call to
   * this method.
   */
  synchronized MongotCursorResultInfo getNextBatch(
      Bytes resultsSizeLimit, BatchCursorOptions queryCursorOptions)
      throws MongotCursorClosedException, IOException {

    try (SpanGuard u = getOrCreateSpan()) {
      u.getSpan().setAttribute("cursorId", this.id);
      if (this.closed) {
        throw new MongotCursorClosedException(this.id);
      }

      this.lastOperationTime = Instant.now();
      this.count++;
      this.batchSizeStrategy.adjust(queryCursorOptions);
      BsonArray nextBatch =
          Explain.isEnabled()
              ? getExplainEnabledNextBatch(resultsSizeLimit, queryCursorOptions)
              : getExplainDisabledNextBatch(resultsSizeLimit, queryCursorOptions);

      return new MongotCursorResultInfo(
          this.batchProducer.isExhausted(), nextBatch, Explain.collect(), this.namespace);
    }
  }

  private synchronized BsonArray getExplainDisabledNextBatch(
      Bytes resultSizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    this.batchProducer.execute(resultSizeLimit, queryCursorOptions);
    return this.batchProducer.getNextBatch(resultSizeLimit);
  }

  private synchronized BsonArray getExplainEnabledNextBatch(
      Bytes resultSizeLimit, BatchCursorOptions queryCursorOptions)
      throws IOException, MongotCursorClosedException {
    Check.checkState(getExplainQueryState().isPresent(), "Explain Query State must be present");
    // Search already ran in CursorFactory, so aggregation queryStats may already be present.
    // Stamp before collect: SearchCommand is not on this path (integration tests create the
    // cursor directly), and emit throws if the header exists without a cursor id.
    AggregationFeatureExplainer.recordCursorId(this.id);
    Bytes previousExplainSize =
        BsonUtils.bsonValueSerializedBytes(getExplainQueryState().get().collect().toBson());
    this.batchProducer.execute(resultSizeLimit.subtract(previousExplainSize), queryCursorOptions);

    Bytes newExplainSize = BsonUtils.bsonValueSerializedBytes(Explain.collect().get().toBson());
    if (resultSizeLimit.compareTo(newExplainSize) < 0) {
      throw new ExplainTooLargeException(
          "Serialized Explain output is larger than max possible result size");
    }
    Bytes nextBatchSizeLimit = resultSizeLimit.subtract(newExplainSize);

    return this.batchProducer.getNextBatch(nextBatchSizeLimit);
  }

  long getId() {
    return this.id;
  }

  String getTraceId() {
    return this.traceId;
  }

  TraceFlags getTraceFlags() {
    return this.traceFlags;
  }

  Instant getLastActive() {
    return this.lastOperationTime;
  }

  synchronized Optional<ExplainQueryState> getExplainQueryState()
      throws MongotCursorClosedException {
    if (this.closed) {
      throw new MongotCursorClosedException(this.id);
    }

    return this.explainQueryState;
  }

  @MustBeClosed
  private synchronized SpanGuard getOrCreateSpan() {
    if (this.count == 1) {
      // place span underneath currently-running SearchCommand tree
      return Tracing.simpleSpanGuard("MongotCursor.getNextBatch" + this.count);
    } else {
      // place directly under saved parent context
      return Tracing.withTraceId(
          "MongotCursor.getNextBatch" + this.count, this.traceId, this.traceFlags);
    }
  }

  void close() throws IOException {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      if (!this.closed) {
        this.batchProducer.close();
        this.closed = true;
      }
    }
  }
}
