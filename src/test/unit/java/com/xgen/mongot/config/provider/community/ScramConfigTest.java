package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/** Unit tests for {@link ScramConfig} parsing, round-trip, and validation. */
public class ScramConfigTest {

  private static BsonDocument minimalTlsDoc() {
    return new BsonDocument("enabled", BsonBoolean.FALSE);
  }

  private static BsonDocument minimalScramDoc() {
    return new BsonDocument()
        .append("username", new BsonString("__system"))
        .append("passwordFile", new BsonString("/etc/mongot/keyfile"))
        .append("tls", minimalTlsDoc());
  }

  @Test
  public void parse_valid_defaultAuthSource() throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(minimalScramDoc()).build()) {
      ScramConfig parsed = ScramConfig.fromBson(parser);
      assertEquals("admin", parsed.authSource());
      assertEquals("__system", parsed.username());
      assertEquals(Path.of("/etc/mongot/keyfile"), parsed.passwordFile());
    }
  }

  @Test
  public void parse_valid_explicitAuthSource() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("authSource", new BsonString("local"))
            .append("username", new BsonString("__system"))
            .append("passwordFile", new BsonString("/etc/mongot/keyfile"))
            .append(
                "tls",
                new BsonDocument("enabled", BsonBoolean.TRUE)
                    .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      ScramConfig parsed = ScramConfig.fromBson(parser);
      assertEquals("local", parsed.authSource());
      assertEquals("__system", parsed.username());
      assertEquals(Path.of("/etc/mongot/keyfile"), parsed.passwordFile());
      assertTrue(parsed.tls().enabled());
      assertTrue(parsed.tls().tlsCertificateKeyFile().isPresent());
      assertEquals(Path.of("/etc/tls/client.pem"), parsed.tls().tlsCertificateKeyFile().get());
    }
  }

  @Test
  public void parse_missingUsername_throws() {
    BsonDocument doc =
        new BsonDocument()
            .append("passwordFile", new BsonString("/etc/mongot/keyfile"))
            .append("tls", minimalTlsDoc());

    var parser = BsonDocumentParser.fromRoot(doc).build();
    @Var BsonParseException caught = null;
    try {
      ScramConfig.fromBson(parser);
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
    assertNotNull("Expected BsonParseException when username is absent", caught);
    assertTrue(
        "Expected message about missing username",
        caught.getMessage() != null && caught.getMessage().contains("username"));
  }

  @Test
  public void parse_missingPasswordFile_throws() {
    BsonDocument doc =
        new BsonDocument()
            .append("username", new BsonString("__system"))
            .append("tls", minimalTlsDoc());

    var parser = BsonDocumentParser.fromRoot(doc).build();
    @Var BsonParseException caught = null;
    try {
      ScramConfig.fromBson(parser);
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
    assertNotNull("Expected BsonParseException when passwordFile is absent", caught);
    assertTrue(
        "Expected message about missing passwordFile",
        caught.getMessage() != null && caught.getMessage().contains("passwordFile"));
  }

  @Test
  public void parse_missingTls_defaultsToDisabled() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("username", new BsonString("__system"))
            .append("passwordFile", new BsonString("/etc/mongot/keyfile"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      ScramConfig parsed = ScramConfig.fromBson(parser);
      assertFalse("tls should default to disabled when omitted", parsed.tls().enabled());
      assertTrue(
          "tlsCertificateKeyFile should be empty when tls is omitted",
          parsed.tls().tlsCertificateKeyFile().isEmpty());
      assertTrue(
          "tlsCertificateKeyFilePasswordFile should be empty when tls is omitted",
          parsed.tls().tlsCertificateKeyFilePasswordFile().isEmpty());
    }
  }

  @Test
  public void roundTrip_toBsonFromBson_preservesValue() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("authSource", new BsonString("local"))
            .append("username", new BsonString("__system"))
            .append("passwordFile", new BsonString("/etc/mongot/keyfile"))
            .append(
                "tls",
                new BsonDocument("enabled", BsonBoolean.TRUE)
                    .append("tlsCertificateKeyFile", new BsonString("/etc/tls/client.pem"))
                    .append(
                        "tlsCertificateKeyFilePasswordFile", new BsonString("/etc/tls/cert-pass")));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      ScramConfig parsed = ScramConfig.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 = BsonDocumentParser.fromRoot(encoded).build()) {
        ScramConfig roundTripped = ScramConfig.fromBson(parser2);
        assertEquals(parsed.authSource(), roundTripped.authSource());
        assertEquals(parsed.username(), roundTripped.username());
        assertEquals(parsed.passwordFile(), roundTripped.passwordFile());
        assertEquals(parsed.tls().enabled(), roundTripped.tls().enabled());
        assertEquals(
            parsed.tls().tlsCertificateKeyFile(), roundTripped.tls().tlsCertificateKeyFile());
        assertEquals(
            parsed.tls().tlsCertificateKeyFilePasswordFile(),
            roundTripped.tls().tlsCertificateKeyFilePasswordFile());
      }
    }
  }

  @Test
  public void validate_valid_succeeds() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig config = new ScramConfig("admin", "__system", Path.of("/etc/mongot/keyfile"), tls);

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      config.validate(parser, Optional.empty());
    }
  }

  @Test
  public void validate_certKeyFileAndPassword_withCaFile_succeeds() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.of(Path.of("/etc/tls/client.pem")),
            Optional.of(Path.of("/etc/tls/cert-pass")),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    ScramConfig config = new ScramConfig("admin", "__system", Path.of("/etc/mongot/keyfile"), tls);

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      config.validate(parser, Optional.empty());
    }
  }

  @Test
  public void validate_passwordFile_withoutCertKeyFile_throws() {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/cert-pass")),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    ScramConfig config = new ScramConfig("admin", "__system", Path.of("/etc/mongot/keyfile"), tls);

    var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build();
    @Var BsonParseException caught = null;
    try {
      config.validate(parser, Optional.empty());
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
    assertNotNull(
        "Expected BsonParseException when passwordFile provided without certKeyFile", caught);
    assertTrue(
        caught.getMessage() != null
            && caught
                .getMessage()
                .contains(
                    "tlsCertificateKeyFile is required when tlsCertificateKeyFilePasswordFile "
                        + "is provided"));
  }

  @Test
  public void validate_certKeyFile_withoutCaFile_succeeds() throws BsonParseException {
    // caFile is optional for SCRAM: when absent the driver falls back to the JVM default trust
    // store for server certificate verification.
    TlsConfig tls =
        new TlsConfig(
            true, Optional.of(Path.of("/etc/tls/client.pem")), Optional.empty(), Optional.empty());
    ScramConfig config = new ScramConfig("admin", "__system", Path.of("/etc/mongot/keyfile"), tls);

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      config.validate(parser, Optional.empty());
    }
  }

  @Test
  public void validate_parentCaFilePresent_throws() {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig config = new ScramConfig("admin", "__system", Path.of("/etc/mongot/keyfile"), tls);

    var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build();
    @Var BsonParseException caught = null;
    try {
      config.validate(parser, Optional.of(Path.of("/etc/tls/ca.pem")));
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
    assertNotNull("Expected BsonParseException when parent caFile is present", caught);
    assertTrue(
        "Expected message about CA file not being supported within sync source definition",
        caught.getMessage() != null
            && caught
                .getMessage()
                .contains(
                    "CA file must be defined within SCRAM's TLS config."
                        + " CA file not supported within sync source definition."));
  }

  @Test
  public void roundTrip_defaultAuthSource_preservesValue() throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(minimalScramDoc()).build()) {
      ScramConfig parsed = ScramConfig.fromBson(parser);
      BsonDocument encoded = parsed.toBson();

      try (var parser2 = BsonDocumentParser.fromRoot(encoded).build()) {
        ScramConfig roundTripped = ScramConfig.fromBson(parser2);
        assertEquals("admin", roundTripped.authSource());
        assertEquals(parsed.username(), roundTripped.username());
        assertEquals(parsed.passwordFile(), roundTripped.passwordFile());
      }
    }
  }
}
