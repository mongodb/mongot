package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import java.util.Optional;

public final class RouterConfig extends MongoConnectionConfig {

  public RouterConfig(
      List<HostAndPort> hostandPorts, Optional<X509Config> x509, Optional<ScramConfig> scram) {
    super(hostandPorts, x509, scram);
  }

  public static RouterConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new RouterConfig(
        parser.getField(Fields.HOST_AND_PORT).unwrap().stream()
            .map(HostAndPort::fromString)
            .toList(),
        parser.getField(Fields.X509).unwrap(),
        parser.getField(Fields.SCRAM).unwrap());
  }
}
