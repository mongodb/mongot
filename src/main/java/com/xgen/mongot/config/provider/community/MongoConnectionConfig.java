package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import com.xgen.mongot.util.mongodb.Databases;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

public abstract sealed class MongoConnectionConfig implements DocumentEncodable
    permits ReplicaSetConfig, RouterConfig {

  private final List<HostAndPort> hostandPorts;
  private final Optional<String> username;
  private final Optional<Path> passwordFile;
  private final String authSource;
  private final boolean tls;

  // TODO(CLOUDP-395903) - remove before community GA.
  @Deprecated(forRemoval = true)
  private final Optional<MongoReadPreferenceName> readPreference;

  private final Optional<X509Config> x509;

  public MongoConnectionConfig(
      List<HostAndPort> hostandPorts,
      Optional<String> username,
      Optional<Path> passwordFile,
      String authSource,
      boolean tls,
      Optional<MongoReadPreferenceName> readPreference,
      Optional<X509Config> x509) {
    this.hostandPorts = hostandPorts;
    this.username = username;
    this.passwordFile = passwordFile;
    this.authSource = authSource;
    this.tls = tls;
    this.readPreference = readPreference;
    this.x509 = x509;
  }

  protected static class Fields {
    public static final Field.Required<List<String>> HOST_AND_PORT =
        Field.builder("hostAndPort")
            .singleValueOrListOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .mustNotBeEmpty()
            .required();

    public static final Field.Optional<String> USERNAME =
        Field.builder("username").stringField().optional().noDefault();

    public static final Field.Optional<Path> PASSWORD_FILE =
        Field.builder("passwordFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.WithDefault<String> AUTH_SOURCE =
        Field.builder("authSource")
            .stringField()
            .mustNotBeEmpty()
            .optional()
            .withDefault(Databases.ADMIN);

    public static final Field.WithDefault<Boolean> TLS =
        Field.builder("tls").booleanField().optional().withDefault(false);

    public static final Field.Optional<MongoReadPreferenceName> READ_PREFERENCE =
        Field.builder("readPreference")
            .enumField(MongoReadPreferenceName.class)
            .asCamelCase()
            .optional()
            .noDefault();

    public static final Field.Optional<X509Config> X509 =
        Field.builder("x509")
            .classField(X509Config::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public List<HostAndPort> hostandPorts() {
    return this.hostandPorts;
  }

  public Optional<String> username() {
    return this.username;
  }

  public Optional<Path> passwordFile() {
    return this.passwordFile;
  }

  public String authSource() {
    return this.authSource;
  }

  public boolean tls() {
    return this.tls;
  }

  @Deprecated(forRemoval = true)
  public Optional<MongoReadPreferenceName> readPreference() {
    return this.readPreference;
  }

  public Optional<X509Config> x509() {
    return this.x509;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HOST_AND_PORT, this.hostandPorts.stream().map(HostAndPort::toString).toList())
        .field(Fields.USERNAME, this.username)
        .field(Fields.PASSWORD_FILE, this.passwordFile)
        .field(Fields.AUTH_SOURCE, this.authSource)
        .field(Fields.TLS, this.tls)
        .field(Fields.READ_PREFERENCE, this.readPreference)
        .field(Fields.X509, this.x509)
        .build();
  }

  // TODO(CLOUDP-389333): add dedicated x509 and SCRAM auth config blocks to simplify this
  //  validation logic.
  public void validate(DocumentParser parser, Optional<Path> caFile) throws BsonParseException {
    if (this.x509.isPresent() && (this.username.isPresent() || this.passwordFile.isPresent())) {
      parser
          .getContext()
          .handleSemanticError("x509 and username/passwordFile cannot be used together");
    } else if (this.x509.isPresent() && caFile.isEmpty()) {
      parser.getContext().handleSemanticError("caFile must be set when using x509 auth");
    } else if (this.x509.isPresent() && !this.tls) {
      parser.getContext().handleSemanticError("tls must be set to true when using x509");
    } else if (this.username.isPresent() && this.passwordFile.isEmpty()) {
      parser.getContext().handleSemanticError("passwordFile is required when username is provided");
    } else if (this.passwordFile.isPresent() && this.username.isEmpty()) {
      parser.getContext().handleSemanticError("username is required when passwordFile is provided");
    } else if (this.x509.isEmpty() && this.username.isEmpty()) {
      parser
          .getContext()
          .handleSemanticError("username/passwordFile or x509 is required for authentication");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MongoConnectionConfig that = (MongoConnectionConfig) o;
    return this.tls == that.tls
        && Objects.equals(this.hostandPorts, that.hostandPorts)
        && Objects.equals(this.username, that.username)
        && Objects.equals(this.passwordFile, that.passwordFile)
        && Objects.equals(this.authSource, that.authSource)
        && Objects.equals(this.readPreference, that.readPreference)
        && Objects.equals(this.x509, that.x509);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.hostandPorts,
        this.username,
        this.passwordFile,
        this.authSource,
        this.tls,
        this.readPreference,
        this.x509);
  }
}
