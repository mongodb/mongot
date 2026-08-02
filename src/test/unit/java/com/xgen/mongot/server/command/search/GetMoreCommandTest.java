package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.MOCK_SEARCH_CURSOR_ID;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockMongotCursorBatch;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.GetMoreCommandDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GetMoreCommandTest {

  private static final long INVALID_CURSOR_ID = MOCK_SEARCH_CURSOR_ID + 1;

  private static final BsonDocument INVALID_CURSOR_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append("errmsg", new BsonString(String.format("cursorId: %d", INVALID_CURSOR_ID)));
  private static final BsonDocument IO_ERROR_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append("errmsg", new BsonString("IO error"));

  private static MongotCursorManager getCursorManager() throws Exception {
    return getCursorManager(
        (timer) -> {
          return;
        });
  }

  private static MongotCursorManager getCursorManager(SearchExplainInformation explain)
      throws Exception {
    return getCursorManager(
        (timer) -> {
          return;
        },
        Optional.of(explain));
  }

  private static MongotCursorManager getCursorManager(QueryBatchTimerRecorder metricRecorder)
      throws Exception {
    return getCursorManager(metricRecorder, Optional.empty());
  }

  /**
   * Creates a MongotCursorManager that behaves in a way that can illicit the desired behavior from
   * SearchCommand.
   */
  private static MongotCursorManager getCursorManager(
      QueryBatchTimerRecorder metricRecorder, Optional<SearchExplainInformation> explain)
      throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);

    MongotCursorBatch batch = mockMongotCursorBatch();

    // Only return a cursor batch for the correct cursor id.
    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .then(
            invocation -> {
              long cursorId = invocation.getArgument(0);
              if (cursorId != MOCK_SEARCH_CURSOR_ID) {
                throw new MongotCursorNotFoundException(cursorId);
              }

              return new MongotCursorResultInfo(
                  batch.getCursorExpected().getCursorId() == MongotCursorResult.EXHAUSTED_CURSOR_ID,
                  batch.getCursorExpected().getBatch(),
                  explain,
                  batch.getCursorExpected().getNamespace());
            });

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn(metricRecorder);

    return cursorManager;
  }

  /** Creates a MongotCursorManager that throws an IOException when getting the batch. */
  private MongotCursorManager getThrowableCursorManager(
      Supplier<? extends Throwable> throwableSupplier) throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);

    when(cursorManager.getNextBatch(anyLong(), any(), any())).thenThrow(throwableSupplier.get());
    return cursorManager;
  }

  @Test
  public void testValidGetMoreCommand() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument()
            .append(GetMoreCommandDefinition.NAME, new BsonInt64(MOCK_SEARCH_CURSOR_ID));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(getCursorManager(), CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    BsonDocument result = command.run();
    BsonDocument expected = mockMongotCursorBatch().toBson();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testRegistersMetrics() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument()
            .append(GetMoreCommandDefinition.NAME, new BsonInt64(MOCK_SEARCH_CURSOR_ID));

    AtomicInteger metricUpdateCount = new AtomicInteger(0);
    MongotCursorManager manager =
        getCursorManager(
            (cursorId) -> {
              metricUpdateCount.incrementAndGet();
            });

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(manager, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    command.run();
    checkState(
        metricUpdateCount.get() > 0,
        "SearchCommand should have called accept on the metrics updater.");
  }

  @Test
  public void testValidGetMoreCommandWithCursorOptions() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument()
            .append(GetMoreCommandDefinition.NAME, new BsonInt64(MOCK_SEARCH_CURSOR_ID))
            .append("cursorOptions", new BsonDocument().append("docsRequested", new BsonInt32(25)));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(getCursorManager(), CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    BsonDocument result = command.run();
    BsonDocument expected = mockMongotCursorBatch().toBson();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testValidGetMoreCommandExplain() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument()
            .append(GetMoreCommandDefinition.NAME, new BsonInt64(MOCK_SEARCH_CURSOR_ID));

    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    MongotCursorManager cursorManager = getCursorManager(explainInformation);
    when(cursorManager.getExplainQueryState(MOCK_SEARCH_CURSOR_ID))
        .thenReturn(
            Optional.of(
                new FakeExplain.FakeQueryState(
                    Explain.Verbosity.QUERY_PLANNER,
                    IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
                    explainInformation)));

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      CommandFactory commandFactory =
          new GetMoreCommand.Factory(cursorManager, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

      Command command = commandFactory.create(mockArgs);
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Truth.assertThat(batch.explain()).isPresent();
      Assert.assertEquals(batch.explain().get(), explainInformation);

      Truth.assertThat(cursorManager.getExplainQueryState(MOCK_SEARCH_CURSOR_ID)).isPresent();
    }
  }

  @Test
  public void testInvalidCursorIdGetMoreCommand() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument().append(GetMoreCommandDefinition.NAME, new BsonInt64(INVALID_CURSOR_ID));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(getCursorManager(), CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    BsonDocument result = command.run();

    Assert.assertEquals(INVALID_CURSOR_RESPONSE, result);
  }

  @Test
  public void testIoExceptionGetMoreCommand() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument().append(GetMoreCommandDefinition.NAME, new BsonInt64(INVALID_CURSOR_ID));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(
            getThrowableCursorManager(() -> new IOException("IO error")),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    BsonDocument result = command.run();

    Assert.assertEquals(IO_ERROR_RESPONSE, result);
  }

  @Test
  public void nextBatch_unreachableErrorThrown() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument().append(GetMoreCommandDefinition.NAME, new BsonInt64(INVALID_CURSOR_ID));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(
            getThrowableCursorManager(Check::unreachableError),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);

    Assert.assertThrows(AssertionError.class, command::run);
  }

  @Test
  public void maybeLoadShed_returnsFalse() throws Exception {
    BsonDocument mockArgs =
        new BsonDocument()
            .append(GetMoreCommandDefinition.NAME, new BsonInt64(MOCK_SEARCH_CURSOR_ID));

    CommandFactory commandFactory =
        new GetMoreCommand.Factory(getCursorManager(), CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Command command = commandFactory.create(mockArgs);
    Assert.assertFalse(
        "GetMoreCommand should not be load shed to ensure cursor operations always complete",
        command.maybeLoadShed());
  }
}
