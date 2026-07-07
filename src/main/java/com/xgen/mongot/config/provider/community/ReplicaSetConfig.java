package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import java.util.Optional;

public final class ReplicaSetConfig extends MongoConnectionConfig {

  public ReplicaSetConfig(
      List<HostAndPort> hostAndPorts,
      Optional<String> srvHost,
      Optional<X509Config> x509,
      Optional<ScramConfig> scram) {
    super(hostAndPorts, srvHost, x509, scram);
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
