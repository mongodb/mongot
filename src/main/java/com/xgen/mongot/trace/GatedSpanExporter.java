package com.xgen.mongot.trace;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.function.BooleanSupplier;

/**
 * A {@link SpanExporter} which delegates to another {@link SpanExporter} only while a supplied gate
 * evaluates to {@code true}, re-checking the gate on every {@link #export}. This lets the dynamic
 * feature flag toggle the OTLP exporter on or off at runtime, without restarting mongot.
 */
public final class GatedSpanExporter implements SpanExporter {

  private final SpanExporter delegate;
  private final BooleanSupplier isEnabled;

  public static SpanExporter create(SpanExporter delegate, BooleanSupplier isEnabled) {
    return new GatedSpanExporter(delegate, isEnabled);
  }

  private GatedSpanExporter(SpanExporter delegate, BooleanSupplier isEnabled) {
    this.delegate = delegate;
    this.isEnabled = isEnabled;
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    if (!this.isEnabled.getAsBoolean()) {
      return CompletableResultCode.ofSuccess();
    }
    return this.delegate.export(spans);
  }

  @Override
  public CompletableResultCode flush() {
    return this.delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    return this.delegate.shutdown();
  }
}
