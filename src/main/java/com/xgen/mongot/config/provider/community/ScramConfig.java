package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import com.xgen.mongot.util.mongodb.Databases;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record ScramConfig(String authSource, String username, Path passwordFile, TlsConfig tls)
    implements DocumentEncodable {

  private static class Fields {
    public static final Field.WithDefault<String> AUTH_SOURCE =
        Field.builder("authSource")
            .stringField()
            .mustNotBeEmpty()
            .optional()
            .withDefault(Databases.ADMIN);

    public static final Field.Required<String> USERNAME =
        Field.builder("username").stringField().mustNotBeEmpty().required();

    public static final Field.Required<Path> PASSWORD_FILE =
        Field.builder("passwordFile").classField(PathField.PARSER, PathField.ENCODER).required();

    public static final Field.WithDefault<TlsConfig> TLS =
        Field.builder("tls")
            .classField(TlsConfig::fromBson, TlsConfig::toBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(
                new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty()));
  }

  /** The {@link CliOptionRegistry} for the ScramConfig within syncSource.replicaSet. */
  public static final class ReplicaSetScramConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--replicaSetScramAuthUsername",
        description = "The username to authenticate with")
    public Optional<String> username = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetScramAuthPasswordFile",
        description = "Path to a file that contains the password to authenticate with")
    public Optional<Path> passwordFile = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetScramAuthSource",
        description = "Name of the database associated with the mongot authentication credentials")
    public Optional<String> authSource = Optional.empty();

    @CommandLine.Mixin
    public TlsConfig.ReplicaSetTlsConfigCliOptionRegistry tlsCliOptionRegistry =
        new TlsConfig.ReplicaSetTlsConfigCliOptionRegistry();

    public ReplicaSetScramConfigCliOptionRegistry() {
      addChildRegistry(Fields.TLS, this.tlsCliOptionRegistry);
      addStringLeafValue(Fields.AUTH_SOURCE, () -> this.authSource);
      addStringLeafValue(Fields.USERNAME, () -> this.username);
      addPathLeafValue(Fields.PASSWORD_FILE, () -> this.passwordFile);
    }
  }

  /** The {@link CliOptionRegistry} for the ScramConfig within syncSource.router. */
  public static final class RouterScramConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--routerScramAuthUsername",
        description = "The username to authenticate with")
    public Optional<String> username = Optional.empty();

    @CommandLine.Option(
        names = "--routerScramAuthPasswordFile",
        description = "Path to a file that contains the password to authenticate with")
    public Optional<Path> passwordFile = Optional.empty();

    @CommandLine.Option(
        names = "--routerScramAuthSource",
        description = "Name of the database associated with the mongot authentication credentials")
    public Optional<String> authSource = Optional.empty();

    @CommandLine.Mixin
    public TlsConfig.RouterTlsConfigCliOptionRegistry tlsCliOptionRegistry =
        new TlsConfig.RouterTlsConfigCliOptionRegistry();

    public RouterScramConfigCliOptionRegistry() {
      addChildRegistry(Fields.TLS, this.tlsCliOptionRegistry);
      addStringLeafValue(Fields.AUTH_SOURCE, () -> this.authSource);
      addStringLeafValue(Fields.USERNAME, () -> this.username);
      addPathLeafValue(Fields.PASSWORD_FILE, () -> this.passwordFile);
    }
  }

  public static ScramConfig fromBson(DocumentParser parser, boolean isSrvConfig)
      throws BsonParseException {
    // If TLS is missing, generate a default TLS config based on isSrvConfig
    // If isSrvConfig is true, generate a default enabled TLS
    // Otherwise, disable TLS by providing default field definition
    ParsedField.WithDefault<TlsConfig> parsedTlsConfig = parser.getField(Fields.TLS);
    TlsConfig tlsConfig =
        (parsedTlsConfig.isEmpty() && isSrvConfig)
            ? getDefaultSevTlsConfig()
            : parsedTlsConfig.unwrap();

    return new ScramConfig(
        parser.getField(Fields.AUTH_SOURCE).unwrap(),
        parser.getField(Fields.USERNAME).unwrap(),
        parser.getField(Fields.PASSWORD_FILE).unwrap(),
        tlsConfig);
  }

  private static TlsConfig getDefaultSevTlsConfig() {
    return new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Validates this SCRAM config, enforcing constraints on TLS fields and inherited CA file usage.
   */
  public void validate(DocumentParser parser) throws BsonParseException {
    // caFile is intentionally not required when tlsCertificateKeyFile is set: SCRAM authenticates
    // via password, so when no caFile is provided the driver falls back to the JVM default trust
    // store for server certificate verification.

    this.tls.validate(parser);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.AUTH_SOURCE, this.authSource)
        .field(Fields.USERNAME, this.username)
        .field(Fields.PASSWORD_FILE, this.passwordFile)
        .field(Fields.TLS, this.tls)
        .build();
  }
}
