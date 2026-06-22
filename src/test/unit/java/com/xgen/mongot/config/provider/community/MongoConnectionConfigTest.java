package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Databases;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;

/** Unit tests for {@link MongoConnectionConfig#validate} validation logic. */
public class MongoConnectionConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));

  private static ReplicaSetConfig config(Optional<X509Config> x509, Optional<ScramConfig> scram) {
    return new ReplicaSetConfig(HOSTS, x509, scram);
  }

  private static X509Config x509(Path certKeyFile) {
    TlsConfig tlsConfig =
        new TlsConfig(
            true,
            Optional.of(certKeyFile),
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    return new X509Config(tlsConfig);
  }

  /** Valid: x509 only. */
  @Test
  public void testValidate_x509Only_Passes() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.of(x509(Path.of("/etc/tls/client.pem"))), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      cfg.validate(parser);
    }
  }

  /** Valid: scram only. */
  @Test
  public void testValidate_scramOnly_Passes() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.of(scram));

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      cfg.validate(parser);
    }
  }

  @Test
  public void testValidate_x509WithoutTls_ThrowsError() throws BsonParseException {
    TlsConfig tlsConfig =
        new TlsConfig(
            false,
            Optional.of(Path.of("/etc/tls/client.pem")),
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    X509Config x509 = new X509Config(tlsConfig);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals("tls must be enabled when tlsCertificateKeyFile is configured", e.getMessage());
    }
  }

  @Test
  public void testValidate_X509ConfigAndScram_ThrowsError() throws BsonParseException {
    X509Config x509 = x509(Path.of("/etc/tls/client.pem"));
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.of(scram));

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509 or scram)", e.getMessage());
    }
  }

  @Test
  public void testValidate_NoAuth_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509 or scram)", e.getMessage());
    }
  }

  @Test
  public void testToBson_ScramConfigured_roundTrips() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.of(scram));

    BsonDocument encoded = cfg.toBson();

    try (var parser = BsonDocumentParser.fromRoot(encoded).build()) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      roundTripped.validate(parser);
    }
  }

  @Test
  public void testToBson_X509Configured_roundTrips() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.of(x509(Path.of("/etc/tls/client.pem"))), Optional.empty());

    BsonDocument encoded = cfg.toBson();

    try (var parser = BsonDocumentParser.fromRoot(encoded).build()) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      roundTripped.validate(parser);
    }
  }
}
