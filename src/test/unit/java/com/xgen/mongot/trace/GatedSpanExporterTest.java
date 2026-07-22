package com.xgen.mongot.trace;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class GatedSpanExporterTest {

  @Test
  public void export_delegatesWhenGateEnabled() {
    SpanExporter delegate = mock(SpanExporter.class);
    when(delegate.export(any())).thenReturn(CompletableResultCode.ofSuccess());
    SpanExporter gated = GatedSpanExporter.create(delegate, () -> true);

    List<SpanData> spans = List.of(mock(SpanData.class));
    gated.export(spans);

    verify(delegate, times(1)).export(spans);
  }

  @Test
  public void export_skipsDelegateWhenGateDisabled() {
    SpanExporter delegate = mock(SpanExporter.class);
    SpanExporter gated = GatedSpanExporter.create(delegate, () -> false);

    CompletableResultCode result = gated.export(List.of(mock(SpanData.class)));

    assertThat(result.isSuccess()).isTrue();
    verify(delegate, never()).export(any());
  }

  @Test
  public void export_reEvaluatesGateOnEachCall() {
    SpanExporter delegate = mock(SpanExporter.class);
    when(delegate.export(any())).thenReturn(CompletableResultCode.ofSuccess());
    AtomicBoolean enabled = new AtomicBoolean(false);
    SpanExporter gated = GatedSpanExporter.create(delegate, enabled::get);

    List<SpanData> spans = List.of(mock(SpanData.class));
    gated.export(spans);
    verify(delegate, never()).export(any());

    enabled.set(true);
    gated.export(spans);
    verify(delegate, times(1)).export(spans);
  }

  @Test
  public void flushAndShutdown_alwaysDelegate() {
    SpanExporter delegate = mock(SpanExporter.class);
    when(delegate.flush()).thenReturn(CompletableResultCode.ofSuccess());
    when(delegate.shutdown()).thenReturn(CompletableResultCode.ofSuccess());
    SpanExporter gated = GatedSpanExporter.create(delegate, () -> false);

    gated.flush();
    gated.shutdown();

    verify(delegate, times(1)).flush();
    verify(delegate, times(1)).shutdown();
  }
}
