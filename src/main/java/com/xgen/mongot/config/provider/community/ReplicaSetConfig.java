package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

public final class ReplicaSetConfig extends MongoConnectionConfig {

  public ReplicaSetConfig(
      List<HostAndPort> hostAndPorts,
      Optional<String> srvHost,
      Optional<X509Config> x509,
      Optional<ScramConfig> scram) {
    super(hostAndPorts, srvHost, x509, scram);
  }

  public static final class ReplicaSetConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--replicaSetHostPorts",
        description =
            "List of host and port specifiers used to construct the seed list in the mongod "
                + "connection string")
    public List<String> hostPorts;

    @CommandLine.Option(
        names = "--replicaSetSrvHost",
        description =
            "Hostname that mongot uses to construct a DNS seed list connection string for"
                + " connecting to mongod. Cannot be used when replicaSetHostPorts is specified")
    public Optional<String> srvHost = Optional.empty();

    @CommandLine.Mixin
    public ScramConfig.ReplicaSetScramConfigCliOptionRegistry scramConfigCliOptionRegistry =
        new ScramConfig.ReplicaSetScramConfigCliOptionRegistry();

    @CommandLine.Mixin
    public X509Config.ReplicaSetX509ConfigCliOptionRegistry x509ConfigCliOptionRegistry =
        new X509Config.ReplicaSetX509ConfigCliOptionRegistry();

    public ReplicaSetConfigCliOptionRegistry() {
      // The isSrvConfig flag only selects the defaults ScramConfig.fromBson applies when parsing,
      // so it has no effect on the "scramAuth" field name and cannot change the config path.
      addChildRegistry(
          MongoConnectionConfig.Fields.scramField(true), this.scramConfigCliOptionRegistry);
      addChildRegistry(MongoConnectionConfig.Fields.X509, this.x509ConfigCliOptionRegistry);
      addStringListLeafValue(MongoConnectionConfig.Fields.HOST_AND_PORT, () -> this.hostPorts);
      addStringLeafValue(MongoConnectionConfig.Fields.SRV_HOST, () -> this.srvHost);
    }
  }

  public static ReplicaSetConfig fromBson(DocumentParser parser) throws BsonParseException {
    Optional<String> srvHost = parser.getField(Fields.SRV_HOST).unwrap();
    return new ReplicaSetConfig(
        parser.getField(Fields.HOST_AND_PORT).unwrap().stream()
            .map(HostAndPort::fromString)
            .toList(),
        srvHost,
        parser.getField(Fields.X509).unwrap(),
        parser.getField(Fields.scramField(srvHost.isPresent())).unwrap());
  }
}
