package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/** Unit tests for {@link X509Config} parsing and round-trip. */
public class X509AuthConfigTest {

  @Test
  public void parse_valid_onlyTlsCertificateKeyFile() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("tlsCertificateKeyFile", new BsonString("/etc/mongot/tls/client.pem"));

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      X509Config parsed = X509Config.fromBson(parser);
      assertEquals(
          Path.of("/etc/mongot/tls/client.pem"), parsed.tlsConfig().tlsCertificateKeyFile().get());
      assertTrue(
          "tlsCertificateKeyFilePasswordFile should be empty when omitted",
          parsed.tlsConfig().tlsCertificateKeyFilePasswordFile().isEmpty());
    }
  }

  @Test
  public void parse_valid_withPasswordFile() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("tlsCertificateKeyFile", new BsonString("/etc/mongot/tls/client-combined.pem"))
            .append(
                "tlsCertificateKeyFilePasswordFile",
                new BsonString("/etc/mongot/secrets/cert-key-pass"));

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      X509Config parsed = X509Config.fromBson(parser);
      assertEquals(
          Path.of("/etc/mongot/tls/client-combined.pem"),
          parsed.tlsConfig().tlsCertificateKeyFile().get());
      assertTrue(parsed.tlsConfig().tlsCertificateKeyFilePasswordFile().isPresent());
      assertEquals(
          Path.of("/etc/mongot/secrets/cert-key-pass"),
          parsed.tlsConfig().tlsCertificateKeyFilePasswordFile().get());
    }
  }

  @Test
  public void validate_caFilePresent_succeeds() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"))
            .append("caFile", new BsonString("/etc/mongot/ca.pem"));

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      X509Config config = X509Config.fromBson(parser);
      config.validate(parser);
    }
  }

  @Test
  public void validate_caFileAbsent_throws() {
    BsonDocument doc =
        new BsonDocument("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"));

    var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build();
    @Var BsonParseException caught = null;
    try {
      X509Config config = X509Config.fromBson(parser);
      config.validate(parser);
    } catch (BsonParseException e) {
      caught = e;
    }
    try {
      parser.close();
    } catch (BsonParseException e) {
      if (caught == null) {
        caught = e;
      }
    }
    assertNotNull("Expected BsonParseException when caFile is absent", caught);
    assertTrue(
        "Expected message about caFile being required for x509",
        caught.getMessage() != null
            && caught.getMessage().contains("caFile must be set within x509 config"));
  }

  @Test
  public void validate_tlsCertificateKeyFileAbsent_throws() {
    BsonDocument doc = new BsonDocument("caFile", new BsonString("/etc/tls/client.pem"));

    var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build();
    @Var BsonParseException caught = null;
    try {
      X509Config config = X509Config.fromBson(parser);
      config.validate(parser);
    } catch (BsonParseException e) {
      caught = e;
    }
    try {
      parser.close();
    } catch (BsonParseException e) {
      if (caught == null) {
        caught = e;
      }
    }
    assertNotNull("Expected BsonParseException when tlsCertificateKeyFile is absent", caught);
    assertTrue(
        "Expected message about tlsCertificateKeyFile being required for x509",
        caught.getMessage() != null
            && caught.getMessage().contains("tlsCertificateKeyFile is required using x509 auth"));
  }

  @Test
  public void validate_tlsCertificateKeyFileAbsentPassportPresent_throws() {
    BsonDocument doc =
        new BsonDocument("tlsCertificateKeyFilePasswordFile", new BsonString("/etc/tls/client.pem"))
            .append("caFile", new BsonString("/etc/mongot/ca.pem"));

    var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build();
    @Var BsonParseException caught = null;
    try {
      X509Config config = X509Config.fromBson(parser);
      config.validate(parser);
    } catch (BsonParseException e) {
      caught = e;
    }
    try {
      parser.close();
    } catch (BsonParseException e) {
      if (caught == null) {
        caught = e;
      }
    }
    assertNotNull("Expected BsonParseException when tlsCertificateKeyFile is absent", caught);
    assertTrue(
        "Expected message about tlsCertificateKeyFile being required for x509",
        caught.getMessage() != null
            && caught.getMessage().contains("tlsCertificateKeyFile is required using x509 auth"));
  }

  @Test
  public void roundTrip_toBsonFromBson_preservesValue() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"))
            .append("tlsCertificateKeyFilePasswordFile", new BsonString("/etc/secrets/key-pass"));

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      X509Config parsed = X509Config.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 =
          BsonDocumentParser.withContext(
                  PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)),
                  encoded)
              .build()) {
        X509Config roundTripped = X509Config.fromBson(parser2);
        assertEquals(
            parsed.tlsConfig().tlsCertificateKeyFile(),
            roundTripped.tlsConfig().tlsCertificateKeyFile());
        assertEquals(
            parsed.tlsConfig().tlsCertificateKeyFilePasswordFile(),
            roundTripped.tlsConfig().tlsCertificateKeyFilePasswordFile());
      }
    }
  }

  @Test
  public void roundTrip_onlyCertificateKeyFile_preservesValue() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("tlsCertificateKeyFile", new BsonString("/etc/tls/client-only.pem"));

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      X509Config parsed = X509Config.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 =
          BsonDocumentParser.withContext(
                  PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)),
                  encoded)
              .build()) {
        X509Config roundTripped = X509Config.fromBson(parser2);
        assertEquals(
            Path.of("/etc/tls/client-only.pem"),
            roundTripped.tlsConfig().tlsCertificateKeyFile().get());
        assertTrue(roundTripped.tlsConfig().tlsCertificateKeyFilePasswordFile().isEmpty());
      }
    }
  }
}
