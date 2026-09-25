package com.xgen.mongot.config.provider.community;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.logging.SafeKeyValuePairsJsonProvider;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.event.KeyValuePair;

/**
 * A logback appender that forwards each event to an OTLP/gRPC collector, additive to the local
 * console/file sink. Attribute coercion and, for throwables, stack-trace rendering run on the
 * emitting thread inside {@link #append}; a bounded {@link BatchLogRecordProcessor} then batches
 * and exports on its own daemon thread and drops on overflow, so a slow or down collector never
 * delays a caller or a local log line. The reserved {@code thread.name} attribute and, when the
 * event carries a throwable, the {@code exception.*} attributes are set after user pairs, so they
 * win when present without hiding a user pair the local sink also prints (e.g. an {@code
 * exception.type} on a non-throwable event). Attributes are filtered and coerced by the same {@link
 * SafeKeyValuePairsJsonProvider} rules as the local JSON encoder, so nothing leaves the host with
 * more detail than the local sink writes. That parity is per-record content, not per-record
 * delivery: this sink is attached to the root logger and independently queued, so under pressure it
 * may export records the local async sink dropped (and vice versa).
 *
 * <p>{@link #EXCLUDED_LOGGER_PREFIXES} is a targeted exclusion, not a general denylist: the SDK and
 * transport loggers would otherwise re-enter the queue on export failure (self-feedback), and
 * {@code Slf4jExporter} lines are span data already shipped through the trace exporter behind the
 * span-attribute allowlist.
 */
public final class OtlpLogAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  /** Self-feedback and already-exported span data loggers whose events are never shipped. */
  public static final Set<String> EXCLUDED_LOGGER_PREFIXES =
      Set.of("io.opentelemetry.", "io.grpc.", "io.netty.", "com.xgen.mongot.trace.Slf4jExporter");

  private static final AttributeKey<String> THREAD_NAME = AttributeKey.stringKey("thread.name");
  private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(5);

  private final SdkLoggerProvider loggerProvider;

  @VisibleForTesting
  OtlpLogAppender(SdkLoggerProvider loggerProvider) {
    this.loggerProvider = loggerProvider;
  }

  /** Builds the appender with a bounded batch processor in front of {@code exporter}. */
  public static OtlpLogAppender create(LogRecordExporter exporter) {
    SdkLoggerProvider provider =
        SdkLoggerProvider.builder()
            .setResource(
                Resource.getDefault()
                    .merge(
                        Resource.create(
                            Attributes.of(AttributeKey.stringKey("service.name"), "mongot"))))
            .addLogRecordProcessor(
                BatchLogRecordProcessor.builder(exporter)
                    .setMaxQueueSize(2048)
                    .setMaxExportBatchSize(512)
                    .setScheduleDelay(Duration.ofSeconds(1))
                    .setExporterTimeout(OtlpExporterFactory.EXPORT_TIMEOUT)
                    .build())
            .build();
    return new OtlpLogAppender(provider);
  }

  @Override
  protected void append(ILoggingEvent event) {
    String loggerName = event.getLoggerName() == null ? "" : event.getLoggerName();
    if (EXCLUDED_LOGGER_PREFIXES.stream().anyMatch(loggerName::startsWith)) {
      return;
    }
    LogRecordBuilder builder =
        this.loggerProvider
            .get(loggerName)
            .logRecordBuilder()
            .setTimestamp(event.getTimeStamp(), TimeUnit.MILLISECONDS)
            .setSeverity(toSeverity(event.getLevel()))
            .setSeverityText(event.getLevel().toString())
            .setBody(event.getFormattedMessage())
            .setContext(Context.current());
    for (KeyValuePair keyValuePair : SafeKeyValuePairsJsonProvider.printableKeyValuePairs(event)) {
      setAttribute(builder, keyValuePair.key, keyValuePair.value);
    }
    if (event.getThrowableProxy() != null) {
      builder.setAttribute("exception.type", event.getThrowableProxy().getClassName());
      builder.setAttribute(
          "exception.message", String.valueOf(event.getThrowableProxy().getMessage()));
      builder.setAttribute(
          "exception.stacktrace", ThrowableProxyUtil.asString(event.getThrowableProxy()));
    }
    // Reserved attributes are set after user pairs so a user key cannot spoof them.
    builder.setAttribute(THREAD_NAME, event.getThreadName());
    builder.emit();
  }

  /** Flushes bounded, then shuts the provider down; called by {@code LoggerContext.stop()}. */
  @Override
  public void stop() {
    this.loggerProvider.forceFlush().join(FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    this.loggerProvider.shutdown().join(FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    super.stop();
  }

  private static Severity toSeverity(Level level) {
    return switch (level.toInt()) {
      case Level.TRACE_INT -> Severity.TRACE;
      case Level.DEBUG_INT -> Severity.DEBUG;
      case Level.INFO_INT -> Severity.INFO;
      case Level.WARN_INT -> Severity.WARN;
      case Level.ERROR_INT -> Severity.ERROR;
      default -> Severity.UNDEFINED_SEVERITY_NUMBER;
    };
  }

  /** Mirrors the local encoder's JSON types: booleans, integers, floating point, else text. */
  private static void setAttribute(LogRecordBuilder builder, String key, Object value) {
    if (value instanceof Boolean bool) {
      builder.setAttribute(key, bool);
    } else if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      builder.setAttribute(key, ((Number) value).longValue());
    } else if (value instanceof Double || value instanceof Float) {
      builder.setAttribute(key, ((Number) value).doubleValue());
    } else {
      builder.setAttribute(key, String.valueOf(value));
    }
  }
}
