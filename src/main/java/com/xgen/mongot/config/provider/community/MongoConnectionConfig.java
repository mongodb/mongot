package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.bson.BsonDocument;

public abstract sealed class MongoConnectionConfig implements DocumentEncodable
    permits ReplicaSetConfig, RouterConfig {

  private final List<HostAndPort> hostandPorts;

  private final Optional<String> srvHost;

  private final Optional<ScramConfig> scram;

  private final Optional<X509Config> x509;

  private final Optional<OidcConfig> oidc;

  public MongoConnectionConfig(
      List<HostAndPort> hostandPorts,
      Optional<String> srvHost,
      Optional<X509Config> x509,
      Optional<ScramConfig> scram,
      Optional<OidcConfig> oidc) {
    this.hostandPorts = hostandPorts;
    this.srvHost = srvHost;
    this.x509 = x509;
    this.scram = scram;
    this.oidc = oidc;
  }

  protected static class Fields {
    public static final Field.Required<List<String>> HOST_AND_PORT =
        Field.builder("hostAndPort")
            .singleValueOrListOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .edition(DeploymentEnvironment.COMMUNITY)
            .required();

    public static final Field.Optional<String> SRV_HOST =
        Field.builder("srvHost")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static final Field.Optional<X509Config> X509 =
        Field.builder("x509")
            .classField(X509Config::fromBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static Field.Optional<ScramConfig> scramField(boolean isSrvConfig) {
      return Field.builder("scramAuth")
          .classField(documentParser -> ScramConfig.fromBson(documentParser, isSrvConfig))
          .disallowUnknownFields()
          .edition(DeploymentEnvironment.COMMUNITY)
          .optional()
          .noDefault();
    }

    public static final Field.Optional<OidcConfig> OIDC =
        Field.builder("oidc")
            .classField(OidcConfig::fromBson, OidcConfig::toBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .optional()
            .noDefault();
  }

  public List<HostAndPort> hostandPorts() {
    return this.hostandPorts;
  }

  public Optional<String> srvHost() {
    return this.srvHost;
  }

  public Optional<X509Config> x509() {
    return this.x509;
  }

  public Optional<ScramConfig> scram() {
    return this.scram;
  }

  /** The OIDC auth configuration, or empty when OIDC authentication is not configured. */
  public Optional<OidcConfig> oidc() {
    return this.oidc;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HOST_AND_PORT, this.hostandPorts.stream().map(HostAndPort::toString).toList())
        .field(Fields.SRV_HOST, this.srvHost)
        .field(Fields.X509, this.x509)
        .field(Fields.scramField(this.srvHost.isPresent()), this.scram)
        .field(Fields.OIDC, this.oidc)
        .build();
  }

  /**
   * Validates the configured authentication mechanism. x509, scram, and oidc are mutually exclusive
   * — the {@code oidc} block is Enterprise-edition-gated, so community parses reject it at the
   * field and the exactly-one hint lists it only for Enterprise. Once that check passes the call is
   * dispatched to a path-specific validator. Also validates that HostAndPorts and SrvString fields
   * are mutually exclusive.
   */
  public void validate(DocumentParser parser) throws BsonParseException {
    this.validateOnlyOneAuthType(parser);
    this.validateHostAndPortsSrvString(parser);

    if (this.x509.isPresent()) {
      this.x509.get().validate(parser);
    } else if (this.scram.isPresent()) {
      this.scram.get().validate(parser);
    } else if (this.oidc.isPresent()) {
      this.oidc.get().validate(parser);
    } else {
      throw Check.unreachableError();
    }
  }

  private void validateOnlyOneAuthType(DocumentParser parser) throws BsonParseException {
    long presentCount =
        Stream.of(this.scram, this.x509, this.oidc).filter(Optional::isPresent).count();
    if (presentCount != 1) {
      // The oidc block is Enterprise-edition-gated — community parses reject it at the field —
      // so the hint must not advertise an option community operators cannot use.
      boolean isEnterprise =
          parser.getContext().getDeploymentEnvironment().orElse(null)
              == DeploymentEnvironment.ENTERPRISE;
      String mechanisms = isEnterprise ? "x509, scram, or oidc" : "x509 or scram";
      parser
          .getContext()
          .handleSemanticError(
              "Exactly one authentication mechanism must be used (" + mechanisms + ")");
    }
  }

  private void validateHostAndPortsSrvString(DocumentParser parser) throws BsonParseException {
    // If no host ports are specified, there must be a valid srv string
    if (this.hostandPorts.isEmpty() && this.srvHost.isEmpty()) {
      parser.getContext().handleSemanticError("HostAndPorts or SrvHost must be specified");
    } else if (!this.hostandPorts.isEmpty() && this.srvHost.isPresent()) {
      parser.getContext().handleSemanticError("HostAndPorts and SrvHost are mutually exclusive");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MongoConnectionConfig that = (MongoConnectionConfig) o;
    return Objects.equals(this.hostandPorts, that.hostandPorts)
        && Objects.equals(this.srvHost, that.srvHost)
        && Objects.equals(this.x509, that.x509)
        && Objects.equals(this.scram, that.scram)
        && Objects.equals(this.oidc, that.oidc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.hostandPorts, this.srvHost, this.x509, this.scram, this.oidc);
  }
}
