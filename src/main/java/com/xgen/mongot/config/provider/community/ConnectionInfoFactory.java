package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.mongodb.ConnectionString;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.SecretsParser;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.ConnectionStringBuilder;
import com.xgen.mongot.util.mongodb.SslContextFactory;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionInfoFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionInfoFactory.class);

  public static ConnectionInfo getSingleHostConnectionInfo(
      MongoConnectionConfig config, Optional<Path> caFile) {
    return new ConnectionInfo(getSingleHostConnectionString(config), getSslContext(config, caFile));
  }

  public static ConnectionInfo getClusterConnectionInfo(
      MongoConnectionConfig config, ReadPreference readPreference, Optional<Path> caFile) {
    return new ConnectionInfo(
        getClusterConnectionString(config, readPreference), getSslContext(config, caFile));
  }

  private static ConnectionString getSingleHostConnectionString(MongoConnectionConfig config) {
    HostAndPort hostAndPort =
        config
            .hostandPorts()
            .get(ThreadLocalRandom.current().nextInt(config.hostandPorts().size()));

    LOG.atInfo()
        .addKeyValue("hostAndPort", hostAndPort)
        .log("Selected host and port for sync source config");

    ConnectionStringBuilder connectionStringBuilder =
        ConnectionStringBuilder.standard()
            .withHostAndPort(hostAndPort)
            .withOption("directConnection", "true");
    return getConnectionString(config, connectionStringBuilder);
  }

  private static ConnectionString getClusterConnectionString(
      MongoConnectionConfig config, ReadPreference readPreference) {
    ConnectionStringBuilder connectionStringBuilder =
        ConnectionStringBuilder.standard()
            .withHostAndPorts(config.hostandPorts())
            .withOption("readPreference", readPreference.getName())
            .withOption("directConnection", "false");
    addTagSets(connectionStringBuilder, readPreference);
    return getConnectionString(config, connectionStringBuilder);
  }

  private static void addTagSets(ConnectionStringBuilder builder, ReadPreference readPreference) {
    if (!(readPreference instanceof TaggableReadPreference taggable)) {
      return;
    }
    taggable
        .getTagSetList()
        .forEach(
            tagSet -> {
              String encoded =
                  StreamSupport.stream(tagSet.spliterator(), false)
                      .map(
                          tag ->
                              URLEncoder.encode(tag.getName(), StandardCharsets.UTF_8)
                                  + ":"
                                  + URLEncoder.encode(tag.getValue(), StandardCharsets.UTF_8))
                      .collect(Collectors.joining(","));
              builder.withRepeatableOption("readPreferenceTags", encoded);
            });
  }

  private static ConnectionString getConnectionString(
      MongoConnectionConfig config, ConnectionStringBuilder connectionStringBuilder) {
    connectionStringBuilder
        .withOption("readConcernLevel", ReadConcernLevel.MAJORITY.getValue())
        .withOption("tls", Boolean.toString(config.tls()));

    if (config.x509().isPresent()) {
      connectionStringBuilder.withX509Config();
    } else {
      String replicaSetPassword =
          Crash.because("failed to read password file")
              .ifThrowsExceptionOrError(
                  () -> SecretsParser.readSecretFile(config.passwordFile().get()));

      connectionStringBuilder
          .withAuthenticationCredentials(config.username().get(), replicaSetPassword)
          .withAuthenticationDatabase(config.authSource());
    }

    return Crash.because("failed to construct connection string")
        .ifThrowsExceptionOrError(connectionStringBuilder::build);
  }

  private static Optional<SSLContext> getSslContext(
      MongoConnectionConfig connectionConfig, Optional<Path> caFile) {

    if (connectionConfig.x509().isPresent()) {
      Check.checkArg(caFile.isPresent(), "caFile must be present with x509");

      X509Config x509Config = connectionConfig.x509().get();
      return Optional.of(
          SslContextFactory.getWithCaAndCertificateFile(
              caFile.get(),
              x509Config.tlsCertificateKeyFile(),
              x509Config.tlsCertificateKeyFilePasswordFile()));
    } else {
      return caFile.map(SslContextFactory::getWithCaFile);
    }
  }
}
