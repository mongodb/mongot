package com.xgen.mongot.util.security;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A PEM-only X.509 certificate bundle: the original PEM bytes plus the parsed certificates, so
 * callers that need either representation can share one strict parser.
 *
 * <p>Non-PEM input (for example DER) is rejected: the configuration contract for on-prem PEM
 * bundles requires it, and silently accepting DER would defer the failure past configuration
 * validation. Use {@link #readPem(Path)} / {@link #readPem(byte[])}.
 */
public record PemCertificateBundle(byte[] pemBytes, List<X509Certificate> certificates) {

  // The whole file must be one or more complete PEM certificate blocks, with only whitespace
  // between/around them. The base64 body class excludes '-' so it cannot swallow an END marker.
  private static final Pattern PEM_BUNDLE =
      Pattern.compile(
          "\\s*(?:-----BEGIN CERTIFICATE-----\\s*[A-Za-z0-9+/=\\s]*"
              + "-----END CERTIFICATE-----\\s*)+");

  /** Defensively copies the mutable components so the bundle is immutable. */
  public PemCertificateBundle(byte[] pemBytes, List<X509Certificate> certificates) {
    this.pemBytes = pemBytes.clone();
    this.certificates = List.copyOf(certificates);
  }

  /** Returns a copy of the PEM bytes so the bundle's internal array cannot be mutated. */
  @Override
  public byte[] pemBytes() {
    return this.pemBytes.clone();
  }

  /** Reads and parses a PEM-only X.509 bundle, rejecting unreadable, non-PEM, or invalid input. */
  public static PemCertificateBundle readPem(Path path) {
    byte[] bytes;
    try {
      if (!Files.isRegularFile(path)) {
        throw new PemParseException("not a readable regular file: " + path);
      }
      bytes = Files.readAllBytes(path);
    } catch (IOException e) {
      throw new PemParseException("failed to read " + path + ": " + e.getMessage(), e);
    }
    return readPem(bytes);
  }

  /** Parses a PEM-only X.509 bundle, rejecting non-PEM (e.g. DER) or invalid input. */
  public static PemCertificateBundle readPem(byte[] bytes) {
    String pem = new String(bytes, StandardCharsets.US_ASCII);
    // "PEM-only" means the file is nothing but complete PEM certificate blocks and whitespace;
    // anything else (DER, stray text, an unterminated block) is rejected rather than silently
    // ignored by the certificate parser.
    if (!PEM_BUNDLE.matcher(pem).matches()) {
      throw new PemParseException("must contain only PEM certificate blocks");
    }
    Collection<? extends Certificate> parsed;
    try {
      parsed =
          CertificateFactory.getInstance("X.509")
              .generateCertificates(new ByteArrayInputStream(bytes));
    } catch (CertificateException e) {
      throw new PemParseException("is not a valid PEM X.509 bundle: " + e.getMessage(), e);
    }
    if (parsed.isEmpty()) {
      throw new PemParseException("contains no X.509 certificates");
    }
    return new PemCertificateBundle(
        bytes, parsed.stream().map(X509Certificate.class::cast).toList());
  }

  /** Raised when a PEM bundle is unreadable, not PEM, or malformed. */
  public static class PemParseException extends RuntimeException {

    public PemParseException(String message) {
      super(message);
    }

    public PemParseException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
