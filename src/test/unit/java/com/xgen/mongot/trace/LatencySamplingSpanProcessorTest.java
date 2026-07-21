package com.xgen.mongot.trace;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class LatencySamplingSpanProcessorTest {

  private static final String TRACE_ID_1 = "0af7651916cd43dd8448eb211c80319c";
  private static final String TRACE_ID_2 = "1af7651916cd43dd8448eb211c80319c";
  private static final String TRACE_ID_3 = "2af7651916cd43dd8448eb211c80319c";
  private static final String TRACE_ID_4 = "3af7651916cd43dd8448eb211c80319c";
  private static final String ROOT_SPAN_ID = "b7ad6b7169203331";
  private static final String CHILD_SPAN_ID = "c7ad6b7169203332";
  private static final String PARENT_SPAN_ID = "a7ad6b7169203330";

  @Test
  public void nonRootSpanDoesNotExport() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(5).build();

    processor.onEnd(mockChildSpan(TRACE_ID_1, 500L));
    processor.forceFlush();

    verify(exporter, never()).export(any());
    processor.shutdown();
  }

  @Test
  public void rootSpanExported() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(5).build();

    processor.onEnd(mockRootSpan(TRACE_ID_1, 500L));
    processor.forceFlush();

    verify(exporter, times(1)).export(any());
    processor.shutdown();
  }

  @Test
  public void topNStrategy() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(5).build();

    processor.onEnd(mockRootSpan(TRACE_ID_1, 100L));
    processor.onEnd(mockRootSpan(TRACE_ID_2, 200L));
    processor.onEnd(mockRootSpan(TRACE_ID_3, 300L));
    processor.forceFlush();

    ArgumentCaptor<Collection<SpanData>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(exporter, times(3)).export(captor.capture());
    Set<String> exportedTraceIds =
        captor.getAllValues().stream()
            .flatMap(Collection::stream)
            .map(SpanData::getTraceId)
            .collect(Collectors.toSet());

    assertThat(exportedTraceIds).contains(TRACE_ID_1);
    assertThat(exportedTraceIds).contains(TRACE_ID_2);
    assertThat(exportedTraceIds).contains(TRACE_ID_3);

    processor.shutdown();
  }

  @Test
  public void topNStrategyEvictsShortestTrace() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(2).build();

    processor.onEnd(mockRootSpan(TRACE_ID_1, 100L));
    processor.onEnd(mockRootSpan(TRACE_ID_2, 200L));
    processor.onEnd(mockRootSpan(TRACE_ID_3, 300L));
    processor.forceFlush();

    ArgumentCaptor<Collection<SpanData>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(exporter, times(2)).export(captor.capture());
    Set<String> exportedTraceIds =
        captor.getAllValues().stream()
            .flatMap(Collection::stream)
            .map(SpanData::getTraceId)
            .collect(Collectors.toSet());

    assertThat(exportedTraceIds).contains(TRACE_ID_2);
    assertThat(exportedTraceIds).contains(TRACE_ID_3);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_1);

    processor.shutdown();
  }

  @Test
  public void percentileStrategy() {
    SpanExporter exporter = mockExporter();
    // p75: thresholdIdx = ceil(0.75 * 4) - 1 = 2; threshold = 300ns
    // Only traces >= 300ns are exported
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setPercentile(0.75).build();

    processor.onEnd(mockRootSpan(TRACE_ID_1, 100L));
    processor.onEnd(mockRootSpan(TRACE_ID_2, 200L));
    processor.onEnd(mockRootSpan(TRACE_ID_3, 300L));
    processor.onEnd(mockRootSpan(TRACE_ID_4, 400L));
    processor.forceFlush();

    ArgumentCaptor<Collection<SpanData>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(exporter, times(2)).export(captor.capture());
    Set<String> exportedTraceIds =
        captor.getAllValues().stream()
            .flatMap(Collection::stream)
            .map(SpanData::getTraceId)
            .collect(Collectors.toSet());

    assertThat(exportedTraceIds).contains(TRACE_ID_3);
    assertThat(exportedTraceIds).contains(TRACE_ID_4);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_1);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_2);

    processor.shutdown();
  }

  @Test
  public void percentileStrategyRespectsExportBudget() {
    SpanExporter exporter = mockExporter();
    // p0.75 over 4 traces would export 2 (300ns, 400ns), but the 1-trace-per-30s budget caps it to
    // the single slowest.
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter)
            .setPercentile(0.75)
            .setWindowSize(Duration.ofSeconds(30))
            .build();

    processor.onEnd(mockRootSpan(TRACE_ID_1, 100L));
    processor.onEnd(mockRootSpan(TRACE_ID_2, 200L));
    processor.onEnd(mockRootSpan(TRACE_ID_3, 300L));
    processor.onEnd(mockRootSpan(TRACE_ID_4, 400L));
    processor.forceFlush();

    ArgumentCaptor<Collection<SpanData>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(exporter, times(1)).export(captor.capture());
    Set<String> exportedTraceIds =
        captor.getAllValues().stream()
            .flatMap(Collection::stream)
            .map(SpanData::getTraceId)
            .collect(Collectors.toSet());

    assertThat(exportedTraceIds).contains(TRACE_ID_4);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_1);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_2);
    assertThat(exportedTraceIds).doesNotContain(TRACE_ID_3);
    processor.shutdown();
  }

  @Test
  public void remoteParentRootIsExported() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(5).build();

    // A local root that continues a distributed trace has a valid but remote parent. Its end must
    // still trigger the sampling decision; otherwise the trace leaks until expiry, never exported.
    processor.onEnd(mockRemoteParentRootSpan(TRACE_ID_1, 500L));
    processor.forceFlush();

    verify(exporter, times(1)).export(any());
    processor.shutdown();
  }

  @Test
  public void lateChildSpanAfterRootIsDropped() {
    SpanExporter exporter = mockExporter();
    LatencySamplingSpanProcessor processor =
        LatencySamplingSpanProcessor.builder(exporter).setTopN(5).build();

    // Root ends first, completing the trace, then a late child for the same trace arrives. The
    // late child must not resurrect a trace entry nor trigger an extra export.
    processor.onEnd(mockRootSpan(TRACE_ID_1, 500L));
    processor.onEnd(mockChildSpan(TRACE_ID_1, 100L));
    processor.forceFlush();

    verify(exporter, times(1)).export(any());
    processor.shutdown();
  }

  private static SpanExporter mockExporter() {
    SpanExporter exporter = mock(SpanExporter.class);
    when(exporter.export(any())).thenReturn(CompletableResultCode.ofSuccess());
    when(exporter.flush()).thenReturn(CompletableResultCode.ofSuccess());
    when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());
    return exporter;
  }

  private static ReadableSpan mockRootSpan(String traceId, long latencyNanos) {
    ReadableSpan span = mock(ReadableSpan.class);
    SpanContext spanCtx =
        SpanContext.create(traceId, ROOT_SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault());
    when(span.getSpanContext()).thenReturn(spanCtx);
    when(span.getParentSpanContext()).thenReturn(SpanContext.getInvalid());
    when(span.getLatencyNanos()).thenReturn(latencyNanos);
    SpanData spanData = mock(SpanData.class);
    when(spanData.getTraceId()).thenReturn(traceId);
    when(span.toSpanData()).thenReturn(spanData);
    return span;
  }

  private static ReadableSpan mockRemoteParentRootSpan(String traceId, long latencyNanos) {
    ReadableSpan span = mock(ReadableSpan.class);
    SpanContext spanCtx =
        SpanContext.create(traceId, ROOT_SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault());
    SpanContext remoteParentCtx =
        SpanContext.createFromRemoteParent(
            traceId, PARENT_SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault());
    when(span.getSpanContext()).thenReturn(spanCtx);
    when(span.getParentSpanContext()).thenReturn(remoteParentCtx);
    when(span.getLatencyNanos()).thenReturn(latencyNanos);
    SpanData spanData = mock(SpanData.class);
    when(spanData.getTraceId()).thenReturn(traceId);
    when(span.toSpanData()).thenReturn(spanData);
    return span;
  }

  private static ReadableSpan mockChildSpan(String traceId, long latencyNanos) {
    ReadableSpan span = mock(ReadableSpan.class);
    SpanContext spanCtx =
        SpanContext.create(
            traceId, CHILD_SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault());
    SpanContext parentCtx =
        SpanContext.create(
            traceId, PARENT_SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault());
    when(span.getSpanContext()).thenReturn(spanCtx);
    when(span.getParentSpanContext()).thenReturn(parentCtx);
    when(span.getLatencyNanos()).thenReturn(latencyNanos);
    SpanData spanData = mock(SpanData.class);
    when(spanData.getTraceId()).thenReturn(traceId);
    when(span.toSpanData()).thenReturn(spanData);
    return span;
  }
}
