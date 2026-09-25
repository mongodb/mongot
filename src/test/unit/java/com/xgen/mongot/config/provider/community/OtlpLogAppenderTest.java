package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Context;
import com.xgen.mongot.logging.SafeKeyValuePairsJsonProvider;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.KeyValuePair;

public class OtlpLogAppenderTest {

  private final RecordingExporter exporter = new RecordingExporter();
  private final OtlpLogAppender appender = appenderWith(this.exporter);

  @Test
  public void append_validEvent_mapsCoreFields() {
    LoggingEvent event = event("com.example.Test", Level.WARN, "hello");
    event.addKeyValuePair(new KeyValuePair("count", 3L));
    event.addKeyValuePair(new KeyValuePair("ok", true));

    this.appender.doAppend(event);

    LogRecordData record = this.exporter.records.get(0);
    assertThat(record.getBody().asString()).isEqualTo("hello");
    assertThat(record.getSeverity()).isEqualTo(Severity.WARN);
    assertThat(record.getSeverityText()).isEqualTo("WARN");
    assertThat(record.getInstrumentationScopeInfo().getName()).isEqualTo("com.example.Test");
    assertThat(record.getTimestampEpochNanos()).isEqualTo(1_700_000_000_000_000_000L);
    Map<AttributeKey<?>, Object> attributes = record.getAttributes().asMap();
    assertThat(attributes.get(AttributeKey.stringKey("thread.name"))).isEqualTo("test-thread");
    assertThat(attributes.get(AttributeKey.longKey("count"))).isEqualTo(3L);
    assertThat(attributes.get(AttributeKey.booleanKey("ok"))).isEqualTo(true);
  }

  @Test
  public void append_throwableEvent_setsExceptionAttributes() {
    LoggingEvent event = event("l", Level.ERROR, "boom");
    event.setThrowableProxy(new ThrowableProxy(new IllegalStateException("bad")));

    this.appender.doAppend(event);

    Map<AttributeKey<?>, Object> attributes = this.exporter.records.get(0).getAttributes().asMap();
    assertThat(attributes.get(AttributeKey.stringKey("exception.type")))
        .isEqualTo("java.lang.IllegalStateException");
    assertThat(attributes.get(AttributeKey.stringKey("exception.message"))).isEqualTo("bad");
    assertThat((String) attributes.get(AttributeKey.stringKey("exception.stacktrace")))
        .contains("append_throwableEvent_setsExceptionAttributes");
  }

  /** The parity canary: the collector never sees an attribute the local encoder would not print. */
  @Test
  public void append_filteredKeyValuePairs_matchLocalEncoder() {
    LoggingEvent event = event("l", Level.INFO, "x");
    event.addKeyValuePair(new KeyValuePair("", "blank-key"));
    event.addKeyValuePair(new KeyValuePair("empty", Optional.empty()));
    event.addKeyValuePair(new KeyValuePair("emptyString", ""));
    event.addKeyValuePair(new KeyValuePair("dup", "first"));
    event.addKeyValuePair(new KeyValuePair("dup", "second"));
    event.addKeyValuePair(new KeyValuePair("map", Map.of("a", "b")));
    // A non-throwable event's user exception.type is printed locally, so parity ships it too.
    event.addKeyValuePair(new KeyValuePair("exception.type", "user-value"));

    this.appender.doAppend(event);

    List<String> printable =
        SafeKeyValuePairsJsonProvider.printableKeyValuePairs(event).stream()
            .map(kv -> kv.key)
            .toList();
    assertThat(printable).containsExactly("dup", "map", "exception.type");
    Map<AttributeKey<?>, Object> attributes = this.exporter.records.get(0).getAttributes().asMap();
    assertThat(attributes.get(AttributeKey.stringKey("dup"))).isEqualTo("second");
    assertThat(attributes.get(AttributeKey.stringKey("map")))
        .isEqualTo(String.valueOf(Map.of("a", "b")));
    assertThat(attributes.get(AttributeKey.stringKey("exception.type"))).isEqualTo("user-value");
    assertThat(attributes.keySet().stream().map(AttributeKey::getKey).toList())
        .containsNoneOf("", "empty", "emptyString");
  }

  /**
   * A user pair named {@code thread.name} must not spoof the reserved emitting-thread attribute.
   */
  @Test
  public void append_userThreadNamePair_reservedThreadNameWins() {
    LoggingEvent event = event("l", Level.INFO, "x");
    event.addKeyValuePair(new KeyValuePair("thread.name", "spoofed"));

    this.appender.doAppend(event);

    Map<AttributeKey<?>, Object> attributes = this.exporter.records.get(0).getAttributes().asMap();
    assertThat(attributes.get(AttributeKey.stringKey("thread.name"))).isEqualTo("test-thread");
  }

  /**
   * Span lines and SDK/transport self-logging never reach the collector; prefixes, not substrings.
   */
  @Test
  public void append_excludedLoggerPrefixes_neverReachCollector() {
    this.appender.doAppend(
        event("com.xgen.mongot.trace.Slf4jExporter", Level.INFO, "CANARY-SPAN-ATTR"));
    this.appender.doAppend(event("io.opentelemetry.exporter.internal.grpc.X", Level.WARN, "a"));
    this.appender.doAppend(event("io.grpc.Y", Level.WARN, "b"));
    this.appender.doAppend(event("io.netty.Z", Level.WARN, "c"));
    this.appender.doAppend(event("io.opentelemetryish.foo", Level.WARN, "kept"));

    assertThat(this.exporter.records.stream().map(r -> r.getBody().asString()).toList())
        .containsExactly("kept");
  }

  @Test
  public void append_activeSpanScope_carriesSpanContext() {
    SpanContext spanContext =
        SpanContext.create(
            "0af7651916cd43dd8448eb211c80319c",
            "b7ad6b7169203331",
            TraceFlags.getSampled(),
            TraceState.getDefault());

    try (Scope scope = Span.wrap(spanContext).makeCurrent()) {
      this.appender.doAppend(event("l", Level.INFO, "in span"));
    }

    assertThat(this.exporter.records.get(0).getSpanContext()).isEqualTo(spanContext);
  }

  @Test
  public void stop_queuedRecords_flushesAndShutsDown() {
    this.appender.doAppend(event("l", Level.INFO, "1"));

    this.appender.stop();

    assertThat(this.exporter.records).hasSize(1);
    assertThat(this.exporter.shutdown.get()).isTrue();
  }

  private static OtlpLogAppender appenderWith(LogRecordExporter exporter) {
    OtlpLogAppender appender =
        new OtlpLogAppender(
            SdkLoggerProvider.builder()
                .addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter))
                .build());
    appender.setContext((Context) LoggerFactory.getILoggerFactory());
    appender.start();
    return appender;
  }

  private static LoggingEvent event(String loggerName, Level level, String message) {
    LoggingEvent event = new LoggingEvent();
    event.setLoggerName(loggerName);
    event.setLevel(level);
    event.setMessage(message);
    event.setThreadName("test-thread");
    event.setTimeStamp(1_700_000_000_000L);
    return event;
  }

  private static final class RecordingExporter implements LogRecordExporter {

    final List<LogRecordData> records = new CopyOnWriteArrayList<>();
    final AtomicBoolean shutdown = new AtomicBoolean();

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
      this.records.addAll(logs);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      this.shutdown.set(true);
      return CompletableResultCode.ofSuccess();
    }
  }
}
