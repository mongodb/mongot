package com.xgen.mongot.cursor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import com.xgen.testing.mongot.mock.index.BatchProducer;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MongotCursorTest {
  private SearchExplainInformation makeLargeExplanation(int byteSize) {
    return new SearchExplainInformation(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(
            new MetadataExplainInformation(
                Optional.empty(),
                Optional.of("x".repeat(byteSize)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Integration tests create a cursor without going through {@code SearchCommand}, which is the
   * other {@code recordCursorId} call site. Search (and {@code recordNodeStats}) already ran before
   * {@code getNextBatch}, so the first explain collect must already have a cursor id or emit
   * throws.
   */
  @Test
  public void getNextBatch_stampsAggregationCursorIdBeforeExplainEmit() throws Exception {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      AggregationFeatureExplainer.recordNodeStats(new AggregationNodeStats("root", List.of()));
      MongotCursor cursor =
          new MongotCursor(
              123L,
              BatchProducer.mockSearchResultBatchProducer().searchBatchProducer,
              "mockCursor",
              new ConstantBatchSizeStrategy());

      cursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());

      Truth.assertThat(
              Explain.collect()
                  .orElseThrow()
                  .aggregation()
                  .orElseThrow()
                  .query()
                  .orElseThrow()
                  .cursorId())
          .hasValue(123L);
    }
  }

  @Test
  public void testExplainQueryStatePresent() throws MongotCursorClosedException {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      MongotCursor mockCursor =
          new MongotCursor(
              123L,
              BatchProducer.mockSearchResultBatchProducer().searchBatchProducer,
              "mockCursor",
              new ConstantBatchSizeStrategy());
      Truth.assertThat(mockCursor.getExplainQueryState()).isPresent();
    }
  }

  @Test
  public void testLargeExplain() throws MongotCursorClosedException, IOException {
    SearchExplainInformation explainInformation = makeLargeExplanation(11_300_000);

    ArgumentCaptor<Bytes> bytesCaptor = ArgumentCaptor.forClass(Bytes.class);

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      var batchProducer = BatchProducer.mockSearchResultBatchProducer().searchBatchProducer;
      MongotCursor mockCursor =
          new MongotCursor(123L, batchProducer, "mockCursor", new ConstantBatchSizeStrategy());
      Truth.assertThat(mockCursor.getExplainQueryState()).isPresent();

      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
      verify(batchProducer).execute(bytesCaptor.capture(), any());
      verify(batchProducer).getNextBatch(bytesCaptor.capture());

      Assert.assertEquals(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT.subtract(
              BsonUtils.bsonValueSerializedBytes(explainInformation.toBson())),
          bytesCaptor.getAllValues().get(1));
    }
  }

  @Test
  public void testOversizedExplain() throws MongotCursorClosedException {
    SearchExplainInformation previousExplainInfo = makeLargeExplanation(10_000);
    SearchExplainInformation explainInformation = makeLargeExplanation(17_000_000);

    var batchProducer = BatchProducer.mockSearchResultBatchProducer().searchBatchProducer;
    MongotCursor mockCursor =
        spy(new MongotCursor(123L, batchProducer, "mockCursor", new ConstantBatchSizeStrategy()));
    when(mockCursor.getExplainQueryState())
        .thenReturn(
            Optional.of(
                new FakeExplain.FakeQueryState(
                    Explain.Verbosity.QUERY_PLANNER,
                    IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
                    previousExplainInfo)));

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      Assert.assertThrows(
          ExplainTooLargeException.class,
          () ->
              mockCursor.getNextBatch(
                  CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty()));
    }
  }

  @Test
  public void testPreviousExplainOutputUsed() throws MongotCursorClosedException, IOException {
    SearchExplainInformation first = makeLargeExplanation(5_500_000);
    SearchExplainInformation second = makeLargeExplanation(6_600_000);

    ArgumentCaptor<Bytes> bytesCaptor = ArgumentCaptor.forClass(Bytes.class);

    var batchProducer = BatchProducer.mockSearchResultBatchProducer().searchBatchProducer;
    MongotCursor mockCursor =
        spy(new MongotCursor(123L, batchProducer, "mockCursor", new ConstantBatchSizeStrategy()));
    when(mockCursor.getExplainQueryState())
        .thenReturn(
            Optional.of(
                new FakeExplain.FakeQueryState(
                    Explain.Verbosity.QUERY_PLANNER,
                    IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
                    first)));

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            second)) {
      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
      verify(batchProducer).execute(bytesCaptor.capture(), any());
      verify(batchProducer).getNextBatch(bytesCaptor.capture());

      Bytes firstBytes =
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT.subtract(
              BsonUtils.bsonValueSerializedBytes(first.toBson()));
      Assert.assertEquals(firstBytes, bytesCaptor.getAllValues().get(0));

      Assert.assertEquals(
          firstBytes.subtract(
              BsonUtils.bsonValueSerializedBytes(second.toBson())
                  .subtract(BsonUtils.bsonValueSerializedBytes(first.toBson()))),
          bytesCaptor.getAllValues().get(1));
    }
  }

  @Test
  public void testPreviousExplainOutputUsedNewExplainTooLarge()
      throws MongotCursorClosedException, IOException {
    SearchExplainInformation first = makeLargeExplanation(5_500_000);
    SearchExplainInformation second = makeLargeExplanation(17_000_000);

    var batchProducer = BatchProducer.mockSearchResultBatchProducer().searchBatchProducer;
    MongotCursor mockCursor =
        spy(new MongotCursor(123L, batchProducer, "mockCursor", new ConstantBatchSizeStrategy()));
    when(mockCursor.getExplainQueryState())
        .thenReturn(
            Optional.of(
                new FakeExplain.FakeQueryState(
                    Explain.Verbosity.QUERY_PLANNER,
                    IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
                    first)));

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            second)) {
      Assert.assertThrows(
          ExplainTooLargeException.class,
          () ->
              mockCursor.getNextBatch(
                  CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT.subtract(Bytes.ofMebi(10)),
                  BatchCursorOptionsBuilder.empty()));
    }
  }

  @Test
  public void testGetNextBatchParentOn() throws Exception {
    try (var u = Tracing.simpleSpanGuard("parent", Tracing.TOGGLE_ON)) {
      // should implicitly pick up parent's traceId and traceFlags
      MongotCursor mockCursor =
          new MongotCursor(
              123L,
              BatchProducer.mockSearchResultBatchProducer().searchBatchProducer,
              "mockCursor",
              new ConstantBatchSizeStrategy());

      assertEquals(u.getSpan().getSpanContext().getTraceId(), mockCursor.getTraceId());
      Assert.assertTrue(mockCursor.getTraceFlags().isSampled());

      // running twice forces it to use this.traceId and this.traceFlags
      // OTel should not throw an exception since traceId and traceFlags are valid.
      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    }
  }

  @Test
  public void testGetNextBatchParentOff() throws Exception {

    try (var u = Tracing.simpleSpanGuard("parent", Tracing.TOGGLE_OFF)) {
      // should implicitly pick up parent's traceId and traceFlags
      MongotCursor mockCursor =
          new MongotCursor(
              123L,
              BatchProducer.mockSearchResultBatchProducer().searchBatchProducer,
              "mockCursor",
              new ConstantBatchSizeStrategy());

      assertEquals(u.getSpan().getSpanContext().getTraceId(), mockCursor.getTraceId());
      Assert.assertFalse(mockCursor.getTraceFlags().isSampled());

      // running twice forces it to use this.traceId and this.traceFlags
      // Should not throw exception.
      // OTel should not throw an exception since traceId and traceFlags are valid.
      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
      mockCursor.getNextBatch(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    }
  }

  @Test
  public void testGetNextBatchWithoutParent() throws Exception {
    // no current span open, so mockCursor.traceId and mockCursor.traceFlags will be default values
    MongotCursor mockCursor =
        new MongotCursor(
            123L,
            BatchProducer.mockSearchResultBatchProducer().searchBatchProducer,
            "mockCursor",
            new ConstantBatchSizeStrategy());

    assertEquals(TraceId.getInvalid(), mockCursor.getTraceId());
    assertEquals(TraceFlags.getDefault(), mockCursor.getTraceFlags()); // unsampled i.e. "default"

    // running twice forces it to use this.traceId and this.traceFlags
    // Should not throw exception.
    // Expected behavior of using invalid TraceId is for OTel to quietly create invalid, unsampled
    // span.
    mockCursor.getNextBatch(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    mockCursor.getNextBatch(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
  }
}
