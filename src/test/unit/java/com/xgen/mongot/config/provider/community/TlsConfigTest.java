package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/** Unit tests for {@link TlsConfig} parsing and round-trip. */
public class TlsConfigTest {

  @Test
  public void parse_valid_defaultEnabled() throws BsonParseException {
    BsonDocument doc = new BsonDocument();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      TlsConfig parsed = TlsConfig.fromBson(parser);
      assertFalse("enabled should default to false when omitted", parsed.enabled());
      assertTrue(
          "tlsCertificateKeyFile should be empty when omitted",
          parsed.tlsCertificateKeyFile().isEmpty());
      assertTrue(
          "tlsCertificateKeyFilePasswordFile should be empty when omitted",
          parsed.tlsCertificateKeyFilePasswordFile().isEmpty());
    }
  }

  @Test
  public void parse_valid_withCertificateKeyFile() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("enabled", BsonBoolean.TRUE)
            .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      TlsConfig parsed = TlsConfig.fromBson(parser);
      assertTrue(parsed.enabled());
      assertTrue(parsed.tlsCertificateKeyFile().isPresent());
      assertEquals(Path.of("/etc/tls/client.pem"), parsed.tlsCertificateKeyFile().get());
      assertTrue(parsed.tlsCertificateKeyFilePasswordFile().isEmpty());
    }
  }

  @Test
  public void parse_valid_allFields() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("enabled", BsonBoolean.TRUE)
            .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"))
            .append(
                "tlsCertificateKeyFilePasswordFile", new BsonString("/etc/tls/cert-pass"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      TlsConfig parsed = TlsConfig.fromBson(parser);
      assertTrue(parsed.enabled());
      assertTrue(parsed.tlsCertificateKeyFile().isPresent());
      assertEquals(Path.of("/etc/tls/client.pem"), parsed.tlsCertificateKeyFile().get());
      assertTrue(parsed.tlsCertificateKeyFilePasswordFile().isPresent());
      assertEquals(
          Path.of("/etc/tls/cert-pass"), parsed.tlsCertificateKeyFilePasswordFile().get());
    }
  }

  @Test
  public void roundTrip_toBsonFromBson_preservesValue() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("enabled", BsonBoolean.TRUE)
            .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"))
            .append(
                "tlsCertificateKeyFilePasswordFile", new BsonString("/etc/tls/cert-pass"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      TlsConfig parsed = TlsConfig.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 = BsonDocumentParser.fromRoot(encoded).build()) {
        TlsConfig roundTripped = TlsConfig.fromBson(parser2);
        assertEquals(parsed.enabled(), roundTripped.enabled());
        assertEquals(parsed.tlsCertificateKeyFile(), roundTripped.tlsCertificateKeyFile());
        assertEquals(
            parsed.tlsCertificateKeyFilePasswordFile(),
            roundTripped.tlsCertificateKeyFilePasswordFile());
      }
    }
  }

  @Test
  public void roundTrip_defaultEnabled_preservesValue() throws BsonParseException {
    BsonDocument doc = new BsonDocument();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      TlsConfig parsed = TlsConfig.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 = BsonDocumentParser.fromRoot(encoded).build()) {
        TlsConfig roundTripped = TlsConfig.fromBson(parser2);
        assertFalse(roundTripped.enabled());
        assertTrue(roundTripped.tlsCertificateKeyFile().isEmpty());
        assertTrue(roundTripped.tlsCertificateKeyFilePasswordFile().isEmpty());
      }
    }
  }

  @Test
  public void validate_valid_enabledWithAllFields_passes() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.of(Path.of("/etc/tls/client.pem")),
            Optional.of(Path.of("/etc/tls/cert-pass")),
            Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      tls.validate(parser);
    }
  }

  @Test
  public void validate_certKeyFile_withTlsDisabled_throws() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            false, Optional.of(Path.of("/etc/tls/client.pem")), Optional.empty(), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> tls.validate(parser));
      assertEquals("tls must be enabled when tlsCertificateKeyFile is configured", e.getMessage());
    }
  }

  @Test
  public void validate_caFile_withTlsDisabled_throws() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            false, Optional.empty(), Optional.empty(), Optional.of(Path.of("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> tls.validate(parser));
      assertEquals("tls must be enabled when caFile is configured", e.getMessage());
    }
  }

  @Test
  public void validate_passwordFile_withoutCertKeyFile_throws() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/cert-pass")),
            Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> tls.validate(parser));
      assertEquals(
          "tlsCertificateKeyFile is required when tlsCertificateKeyFilePasswordFile is provided",
          e.getMessage());
    }
  }
}
