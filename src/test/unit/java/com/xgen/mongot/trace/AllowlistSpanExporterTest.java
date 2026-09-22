package com.xgen.mongot.trace;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AllowlistSpanExporterTest {

  private static final String CANARY = "CANARY_MARKER";

  @Test
  public void export_recordsOnlyAllowlistedAttributesAndDropsCanary() {
    SpanExporter delegate = mock(SpanExporter.class);
    when(delegate.export(any())).thenReturn(CompletableResultCode.ofSuccess());
    new AllowlistSpanExporter(delegate)
        .export(
            List.of(
                spanWith(
                    Attributes.builder()
                        .put("cursorId", 42L)
                        .put("time", "12ms")
                        .put("query", CANARY + " query payload")
                        .put("credentials", CANARY + " secret")
                        .build())));

    ArgumentCaptor<Collection<SpanData>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(delegate).export(captor.capture());
    SpanData exported = captor.getValue().iterator().next();
    assertThat(attributeKeys(exported)).containsExactly("cursorId", "time");
    assertThat(exported.getTotalAttributeCount()).isEqualTo(2);
  }

  private static SpanData spanWith(Attributes attributes) {
    SpanData span = mock(SpanData.class);
    when(span.getAttributes()).thenReturn(attributes);
    return span;
  }

  private static Set<String> attributeKeys(SpanData span) {
    return span.getAttributes().asMap().keySet().stream()
        .map(AttributeKey::getKey)
        .collect(Collectors.toSet());
  }
}
