package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

public final class RouterConfig extends MongoConnectionConfig {

  public RouterConfig(
      List<HostAndPort> hostandPorts,
      Optional<String> srvHost,
      Optional<X509Config> x509,
      Optional<ScramConfig> scram,
      Optional<OidcConfig> oidc) {
    super(hostandPorts, srvHost, x509, scram, oidc);
  }

  public static final class RouterConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--routerHostPorts",
        description = "List of host and ports used to specify mongos endpoints")
    public List<String> hostPorts;

    @CommandLine.Option(
        names = "--routerSrvHost",
        description =
            "Hostname that mongot uses to construct a DNS seed list connection string for"
                + " connecting to mongos. Cannot be used when routerHostPorts is specified")
    public Optional<String> srvHost = Optional.empty();

    @CommandLine.Mixin
    public ScramConfig.RouterScramConfigCliOptionRegistry scramConfigCliOptionRegistry =
        new ScramConfig.RouterScramConfigCliOptionRegistry();

    @CommandLine.Mixin
    public X509Config.RouterX509ConfigCliOptionRegistry x509ConfigCliOptionRegistry =
        new X509Config.RouterX509ConfigCliOptionRegistry();

    public RouterConfigCliOptionRegistry() {
      // The isSrvConfig flag only selects the defaults ScramConfig.fromBson applies when parsing,
      // so it has no effect on the "scramAuth" field name and cannot change the config path.
      addChildRegistry(
          MongoConnectionConfig.Fields.scramField(true), this.scramConfigCliOptionRegistry);
      addChildRegistry(MongoConnectionConfig.Fields.X509, this.x509ConfigCliOptionRegistry);
      addStringListLeafValue(MongoConnectionConfig.Fields.HOST_AND_PORT, () -> this.hostPorts);
      addStringLeafValue(MongoConnectionConfig.Fields.SRV_HOST, () -> this.srvHost);
    }
  }

  public static RouterConfig fromBson(DocumentParser parser) throws BsonParseException {
    Optional<String> srvHost = parser.getField(Fields.SRV_HOST).unwrap();
    return new RouterConfig(
        parser.getField(Fields.HOST_AND_PORT).unwrap().stream()
            .map(HostAndPort::fromString)
            .toList(),
        srvHost,
        parser.getField(Fields.X509).unwrap(),
        parser.getField(Fields.scramField(srvHost.isPresent())).unwrap(),
        parser.getField(Fields.OIDC).unwrap());
  }
}
