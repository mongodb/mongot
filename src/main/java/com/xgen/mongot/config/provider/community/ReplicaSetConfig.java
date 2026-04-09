package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public final class ReplicaSetConfig extends MongoConnectionConfig {

  public ReplicaSetConfig(
      List<HostAndPort> hostandPorts,
      Optional<String> username,
      Optional<Path> passwordFile,
      String authSource,
      boolean tls,
      Optional<MongoReadPreferenceName> readPreference,
      Optional<X509Config> x509) {
    super(hostandPorts, username, passwordFile, authSource, tls, readPreference, x509);
  }

  public static ReplicaSetConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new ReplicaSetConfig(
        parser.getField(Fields.HOST_AND_PORT).unwrap().stream()
            .map(HostAndPort::fromString)
            .toList(),
        parser.getField(Fields.USERNAME).unwrap(),
        parser.getField(Fields.PASSWORD_FILE).unwrap(),
        parser.getField(Fields.AUTH_SOURCE).unwrap(),
        parser.getField(Fields.TLS).unwrap(),
        parser.getField(Fields.READ_PREFERENCE).unwrap(),
        parser.getField(Fields.X509).unwrap());
  }
}
