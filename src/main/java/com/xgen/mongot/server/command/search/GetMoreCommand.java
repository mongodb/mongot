package com.xgen.mongot.server.command.search;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.BatchOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.GetMoreCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.context.Scope;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetMoreCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(GetMoreCommand.class);

  private final GetMoreCommandDefinition definition;
  private final MongotCursorManager cursorManager;
  private final Bytes bsonSizeSoftLimit;
  private final MetricsFactory metricsFactory;

  private GetMoreCommand(
      GetMoreCommandDefinition definition,
      MongotCursorManager cursorManager,
      Bytes bsonSizeSoftLimit,
      MetricsFactory metricsFactory) {
    this.definition = definition;
    this.cursorManager = cursorManager;
    this.bsonSizeSoftLimit = bsonSizeSoftLimit;
    this.metricsFactory = metricsFactory;
  }

  @Override
  public String name() {
    return GetMoreCommandDefinition.NAME;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace().addKeyValue("command", GetMoreCommandDefinition.NAME).log("Received command");

    try {
      Optional<ExplainQueryState> prevExplainState =
          this.cursorManager.getExplainQueryState(this.definition.cursorId());

      // Get the timer consumer first, as the cursor may be killed when the next batch is
      // retrieved.
      QueryBatchTimerRecorder queryBatchTimerRecorder =
          this.cursorManager.getIndexQueryBatchTimerRecorder(this.definition.cursorId());

      try (Scope unused = Explain.setup(prevExplainState)) {
        Timer.Sample sample = Timer.start();

        MongotCursorResultInfo cursorResultInfo =
            this.cursorManager.getNextBatch(
                this.definition.cursorId(),
                this.bsonSizeSoftLimit.subtract(
                    MongotCursorBatch.calculateEmptyBatchSize(Optional.empty(), Optional.empty())),
                this.definition
                    .cursorOptions()
                    .map(BatchOptionsDefinition::toQueryCursorOptions)
                    .orElse(BatchCursorOptions.empty()));

        MongotCursorResult cursorResult =
            cursorResultInfo.toCursorResult(this.definition.cursorId(), Optional.empty());

        MongotCursorBatch batch =
            new MongotCursorBatch(cursorResult, cursorResultInfo.explainResult, Optional.empty());

        queryBatchTimerRecorder.recordSample(sample);
        return batch.toBson();
      }
    } catch (MongotCursorNotFoundException | IndexUnavailableException e) {
      return MessageUtils.createErrorBody(e);
    } catch (Exception e) {
      // we didn't expect this error, so we will log the entire stack trace for diagnostics
      LOG.error("Unexpected error", e);
      return MessageUtils.createErrorBody(e);
    } catch (Throwable e) {
      // Tag with exceptionName + errorType for consistency with the other *InternalFailures
      // metrics. A raw Throwable is always an internal error.
      this.metricsFactory
          .counter(
              "GetMoreCommandInternalFailures",
              Tags.of(
                  "exceptionName",
                  e.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  ErrorType.INTERNAL_ERROR.toString()))
          .increment();
      throw e;
    }
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }

  @Override
  public boolean dependOnCursors() {
    return true;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  public static class Factory implements CommandFactory {

    private final MongotCursorManager cursorManager;
    private final Bytes bsonSizeSoftLimit;
    private final MetricsFactory metricsFactory;

    public Factory(
        MongotCursorManager cursorManager, Bytes bsonSizeSoftLimit, MetricsFactory metricsFactory) {
      this.cursorManager = cursorManager;
      this.bsonSizeSoftLimit = bsonSizeSoftLimit;
      this.metricsFactory = metricsFactory;
    }

    @Override
    public Command create(BsonDocument args) {
      try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
        GetMoreCommandDefinition definition = GetMoreCommandDefinition.fromBson(parser);
        return new GetMoreCommand(
            definition, this.cursorManager, this.bsonSizeSoftLimit, this.metricsFactory);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
