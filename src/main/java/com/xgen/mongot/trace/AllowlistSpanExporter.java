package com.xgen.mongot.trace;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Drops non-allowlisted span attributes before delegating to another exporter.
 *
 * <p><b>Attribute-level filter, not a general sanitizer.</b> Span names, IDs, timings, status,
 * events, links, and resource attributes pass through unchanged.
 */
public final class AllowlistSpanExporter implements SpanExporter {

  /** Attribute keys mongot instrumentation is permitted to export. */
  public static final Set<String> PERMITTED_ATTRIBUTE_KEYS =
      Set.of(
          "cursorId",
          "resultCursorId",
          "metaCursorId",
          "num search results",
          "num vector results",
          "time",
          "toggleTrace");

  private final SpanExporter delegate;

  public AllowlistSpanExporter(SpanExporter delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    List<SpanData> filtered =
        spans.stream().map(AllowlistSpanExporter::applyAllowlist).collect(Collectors.toList());
    return this.delegate.export(filtered);
  }

  @Override
  public CompletableResultCode flush() {
    return this.delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    return this.delegate.shutdown();
  }

  @SuppressWarnings("unchecked")
  static SpanData applyAllowlist(SpanData span) {
    Attributes attributes = span.getAttributes();
    AttributesBuilder builder = Attributes.builder();
    attributes.forEach(
        (key, value) -> {
          if (PERMITTED_ATTRIBUTE_KEYS.contains(key.getKey())) {
            builder.put((AttributeKey<Object>) key, value);
          }
        });
    Attributes filtered = builder.build();
    return filtered.size() == attributes.size() ? span : new FilteredSpanData(span, filtered);
  }

  /** A {@link SpanData} view exposing only the allowlisted attributes. */
  private static final class FilteredSpanData extends DelegatingSpanData {

    private final Attributes attributes;

    private FilteredSpanData(SpanData delegate, Attributes attributes) {
      super(delegate);
      this.attributes = attributes;
    }

    @Override
    public Attributes getAttributes() {
      return this.attributes;
    }

    @Override
    public int getTotalAttributeCount() {
      return this.attributes.size();
    }
  }
}
