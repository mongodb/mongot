package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.security.PemCertificateBundle;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

/**
 * Builds and validates the Community OTLP/gRPC exporter: endpoint grammar, PEM CA trust material, a
 * 10s export timeout, disabled retries, and a single responsibility for construction and error
 * reporting. The absent-CA case uses the JVM trust store; a configured CA replaces it and is never
 * silently ignored. See {@code docs/internal/development/otel-tracing.md} for the rationale.
 */
public final class OtlpExporterFactory {

  static final Duration EXPORT_TIMEOUT = Duration.ofSeconds(10);

  private OtlpExporterFactory() {}

  /** Builds the OTLP exporter for a validated {@code tracing.otlp} block. */
  public static SpanExporter create(OtlpTracingConfig config) {
    try {
      OtlpGrpcSpanExporterBuilder builder =
          OtlpGrpcSpanExporter.builder()
              .setEndpoint(canonicalEndpoint(config.endpoint()))
              .setTimeout(EXPORT_TIMEOUT)
              // The pinned RetryPolicy cannot express a single attempt, so retries are disabled.
              .setRetryPolicy(null);
      config
          .caCertificateFile()
          .ifPresent(path -> builder.setTrustedCertificates(readCaBundle(path)));
      return builder.build();
    } catch (RuntimeException e) {
      throw new OtlpConfigException(
          "failed to construct the OTLP exporter for endpoint "
              + config.endpoint()
              + ": "
              + e.getMessage(),
          e);
    }
  }

  /** Builds the OTLP log-record exporter for a validated {@code logging.otlp} block. */
  public static LogRecordExporter createLogRecordExporter(OtlpTracingConfig config) {
    try {
      OtlpGrpcLogRecordExporterBuilder builder =
          OtlpGrpcLogRecordExporter.builder()
              .setEndpoint(canonicalEndpoint(config.endpoint()))
              .setTimeout(EXPORT_TIMEOUT)
              // The pinned RetryPolicy cannot express a single attempt, so retries are disabled.
              .setRetryPolicy(null);
      config
          .caCertificateFile()
          .ifPresent(path -> builder.setTrustedCertificates(readCaBundle(path)));
      return builder.build();
    } catch (RuntimeException e) {
      throw new OtlpConfigException(
          "failed to construct the OTLP log exporter for endpoint "
              + config.endpoint()
              + ": "
              + e.getMessage(),
          e);
    }
  }

  /** Validates the {@code https://host:port} grammar, returning an actionable error or empty. */
  static Optional<String> validateEndpoint(String endpoint) {
    if (endpoint == null || endpoint.isEmpty()) {
      return Optional.of("endpoint must not be empty");
    }
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      return Optional.of("endpoint must be a valid https://host:port URI");
    }
    if (uri.getScheme() == null || !uri.getScheme().equalsIgnoreCase("https")) {
      return Optional.of("endpoint must use the https scheme and be of the form https://host:port");
    }
    if (uri.getRawUserInfo() != null) {
      return Optional.of("endpoint must not contain userinfo");
    }
    if (uri.getRawPath() != null && !uri.getRawPath().isEmpty()) {
      return Optional.of("endpoint must not contain a path or trailing slash");
    }
    if (uri.getRawQuery() != null) {
      return Optional.of("endpoint must not contain a query string");
    }
    if (uri.getRawFragment() != null) {
      return Optional.of("endpoint must not contain a fragment");
    }
    // URI returns a null host for missing hosts, malformed ports, and unbracketed IPv6, and
    // rejects invalid bracketed IPv6 at parse time.
    if (uri.getHost() == null) {
      return Optional.of("endpoint must include a valid hostname or bracketed IPv6 host");
    }
    int port = uri.getPort();
    if (port == -1) {
      return Optional.of("endpoint must include a port");
    }
    if (port < 1 || port > 65535) {
      return Optional.of("endpoint port must be an integer between 1 and 65535");
    }
    return Optional.empty();
  }

  /** Validates a configured PEM CA bundle, returning an actionable error or empty. */
  static Optional<String> validateCaCertificateFile(Path path) {
    try {
      readCaBundle(path);
      return Optional.empty();
    } catch (OtlpConfigException e) {
      return Optional.of(String.valueOf(e.getMessage()));
    }
  }

  /** Reads a PEM X.509 bundle as the exporter's complete trust material. */
  static byte[] readCaBundle(Path path) {
    try {
      return PemCertificateBundle.readPem(path).pemBytes();
    } catch (PemCertificateBundle.PemParseException e) {
      throw new OtlpConfigException("CA certificate file " + e.getMessage(), e);
    }
  }

  /** Canonicalizes only the scheme case; the pinned SDK compares the scheme case-sensitively. */
  static String canonicalEndpoint(String endpoint) {
    return endpoint.startsWith("https://") ? endpoint : "https://" + endpoint.substring(8);
  }

  /** Raised when the OTLP configuration cannot produce a usable exporter. */
  public static final class OtlpConfigException extends RuntimeException {

    OtlpConfigException(String message) {
      super(message);
    }

    OtlpConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
