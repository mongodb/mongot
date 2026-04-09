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
import org.junit.Test;

/** Unit tests for {@link MongoConnectionConfig#validate} validation logic. */
public class MongoConnectionConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));

  private static ReplicaSetConfig config(
      Optional<String> username, Optional<Path> passwordFile, Optional<X509Config> x509) {
    return config(username, passwordFile, x509, false);
  }

  private static ReplicaSetConfig config(
      Optional<String> username,
      Optional<Path> passwordFile,
      Optional<X509Config> x509,
      boolean tls) {
    return new ReplicaSetConfig(
        HOSTS,
        username,
        passwordFile,
        Databases.ADMIN,
        tls,
        Optional.empty(),
        x509);
  }

  private static Optional<X509Config> x509(Path certKeyFile) {
    return Optional.of(new X509Config(certKeyFile, Optional.empty()));
  }

  /** Valid: username + passwordFile. */
  @Test
  public void testValidate_UsernameAndPasswordFile_Passes() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.of("user"), Optional.of(Path.of("/etc/passwd")), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      cfg.validate(parser, Optional.empty());
    }
  }

  /** Valid: x509 + caFile + tls. */
  @Test
  public void testValidate_x509WithCaFileAndTls_Passes() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(
            Optional.empty(),
            Optional.empty(),
            x509(Path.of("/etc/tls/client.pem")),
            true);

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      cfg.validate(parser, Optional.of(Path.of("/etc/ca.pem")));
    }
  }

  @Test
  public void testValidate_x509WithoutTls_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.empty(), Optional.empty(), x509(Path.of("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(
              BsonParseException.class,
              () -> cfg.validate(parser, Optional.of(Path.of("/etc/ca.pem"))));
      assertEquals("tls must be set to true when using x509", e.getMessage());
    }
  }

  @Test
  public void testValidate_x509WithUsername_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.of("user"), Optional.empty(), x509(Path.of("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(
              BsonParseException.class,
              () -> cfg.validate(parser, Optional.of(Path.of("/etc/ca.pem"))));
      assertEquals("x509 and username/passwordFile cannot be used together", e.getMessage());
    }
  }

  @Test
  public void testValidate_x509WithPasswordFile_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(
            Optional.empty(),
            Optional.of(Path.of("/etc/passwd")),
            x509(Path.of("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(
              BsonParseException.class,
              () -> cfg.validate(parser, Optional.of(Path.of("/etc/ca.pem"))));
      assertEquals("x509 and username/passwordFile cannot be used together", e.getMessage());
    }
  }

  @Test
  public void testValidate_x509WithoutCaFile_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.empty(), Optional.empty(), x509(Path.of("/etc/tls/client.pem")));

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> cfg.validate(parser, Optional.empty()));
      assertEquals("caFile must be set when using x509 auth", e.getMessage());
    }
  }

  @Test
  public void testValidate_UsernameWithoutPasswordFile_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.of("user"), Optional.empty(), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> cfg.validate(parser, Optional.empty()));
      assertEquals("passwordFile is required when username is provided", e.getMessage());
    }
  }

  @Test
  public void testValidate_PasswordFileWithoutUsername_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(Optional.empty(), Optional.of(Path.of("/etc/passwd")), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> cfg.validate(parser, Optional.empty()));
      assertEquals("username is required when passwordFile is provided", e.getMessage());
    }
  }

  @Test
  public void testValidate_NoAuth_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.empty());

    try (var parser = BsonDocumentParser.fromRoot(new org.bson.BsonDocument()).build()) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> cfg.validate(parser, Optional.empty()));
      assertEquals("username/passwordFile or x509 is required for authentication", e.getMessage());
    }
  }
}
