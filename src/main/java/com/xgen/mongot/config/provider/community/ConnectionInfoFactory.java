package com.xgen.mongot.config.provider.community;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.net.ssl.SSLContext;

public class ConnectionInfoFactory {

  public static ConnectionInfo getClusterConnectionInfo(
      MongoConnectionConfig config, ReadPreference readPreference) {
    return new ConnectionInfo(
        getClusterConnectionString(config, readPreference), getSslContext(config));
  }

  /**
   * Creates a {@link ConnectionInfo} for a direct connection ({@code directConnection=true}) to the
   * specified {@code hostAndPort}, using the authentication and TLS settings from {@code config}.
   */
  public static ConnectionInfo getSingleHostConnectionInfo(
      MongoConnectionConfig config, HostAndPort hostAndPort) {
    return new ConnectionInfo(
        getSingleHostConnectionString(config, hostAndPort, Optional.empty()),
        getSslContext(config));
  }

  /**
   * Creates a {@link ConnectionInfo} targeting the specified {@code hostAndPort} with the given
   * {@code readPreference} embedded in the URI. The driver rejects a connection string that
   * combines {@code directConnection=true} with a read preference, so {@code directConnection} is
   * omitted — the single-host URI still causes the driver to use {@link
   * com.mongodb.connection.ClusterConnectionMode#SINGLE}, and the read preference is forwarded in
   * each command so that mongos can route to the correct shard members.
   */
  public static ConnectionInfo getSingleHostConnectionInfo(
      MongoConnectionConfig config, HostAndPort hostAndPort, ReadPreference readPreference) {
    return new ConnectionInfo(
        getSingleHostConnectionString(config, hostAndPort, Optional.of(readPreference)),
        getSslContext(config));
  }

  private static ConnectionString getSingleHostConnectionString(
      MongoConnectionConfig config,
      HostAndPort hostAndPort,
      Optional<ReadPreference> readPreference) {
    // directConnection=true and readPreference are mutually exclusive: the driver rejects a URI
    // that carries both. For direct mongod connections (no readPreference) we set
    // directConnection=true. For mongos connections that embed a readPreference, we omit
    // directConnection — the single-host URI causes the driver to use SINGLE mode anyway.
    var builder = ConnectionStringBuilder.standard().withHostAndPort(hostAndPort);
    if (readPreference.isPresent()) {
      builder.withOption("readPreference", readPreference.get().getName());
      addTagSets(builder, readPreference.get());
    } else {
      builder.withOption("directConnection", "true");
    }
    return getConnectionString(config, builder);
  }

  private static ConnectionString getClusterConnectionString(
      MongoConnectionConfig config, ReadPreference readPreference) {
    ConnectionStringBuilder connectionStringBuilder;
    if (config.srvHost().isPresent()) {
      connectionStringBuilder = ConnectionStringBuilder.srv().withHost(config.srvHost().get());
    } else {
      connectionStringBuilder =
          ConnectionStringBuilder.standard().withHostAndPorts(config.hostandPorts());
    }
    connectionStringBuilder
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

    if (config.scram().isPresent()) {
      ScramConfig scramConfig = config.scram().get();
      return buildScramConnectionString(scramConfig, connectionStringBuilder);
    }
    if (config.x509().isPresent()) {
      return buildx509ConnectionString(connectionStringBuilder);
    }
    throw Check.unreachableError();
  }

  private static ConnectionString buildScramConnectionString(
      ScramConfig scramConfig, ConnectionStringBuilder connectionStringBuilder) {

    connectionStringBuilder
        .withOption("readConcernLevel", ReadConcernLevel.MAJORITY.getValue())
        .withOption("tls", Boolean.toString(scramConfig.tls().enabled()));

    String replicaSetPassword =
        Crash.because("failed to read password file")
            .ifThrowsExceptionOrError(
                () -> SecretsParser.readSecretFile(scramConfig.passwordFile()));

    connectionStringBuilder
        .withAuthenticationCredentials(scramConfig.username(), replicaSetPassword)
        .withAuthenticationDatabase(scramConfig.authSource());

    return Crash.because("failed to construct connection string")
        .ifThrowsExceptionOrError(() -> connectionStringBuilder.build());
  }

  private static ConnectionString buildx509ConnectionString(
      ConnectionStringBuilder connectionStringBuilder) {

    connectionStringBuilder
        .withOption("readConcernLevel", ReadConcernLevel.MAJORITY.getValue())
        .withOption("tls", Boolean.toString(true));

    connectionStringBuilder.withX509Config();

    return Crash.because("failed to construct connection string")
        .ifThrowsExceptionOrError(() -> connectionStringBuilder.build());
  }

  @VisibleForTesting
  public static Optional<SSLContext> getSslContext(MongoConnectionConfig connectionConfig) {

    if (connectionConfig.scram().isPresent()) {
      return getScramSslContext(connectionConfig.scram().get());
    }

    if (connectionConfig.x509().isPresent()) {
      X509Config x509Config = connectionConfig.x509().get();
      return Optional.of(getX509SslContext(x509Config));
    }

    throw Check.unreachableError();
  }

  /**
   * Builds an {@link SSLContext} for x509 client-certificate authentication. A CA file is required
   * inline on {@code x509Config}. x509 setups use a private CA not present in the JVM default trust
   * store, so falling back to it would either fail verification or silently accept any server
   * certificate signed by a public CA.
   *
   * @param x509Config the x509 auth configuration, which may carry an inline CA and cert-key file
   * @return an {@link SSLContext} configured with the resolved CA and client certificate
   */
  private static SSLContext getX509SslContext(X509Config x509Config) {

    Check.checkArg(
        x509Config.tlsConfig().caFile().isPresent(), "caFile must be set within x509 config");

    Check.checkArg(
        x509Config.tlsConfig().tlsCertificateKeyFile().isPresent(),
        "tlsCertificateKeyFile required for x509 auth");

    return SslContextFactory.getWithCertKeyFile(
        x509Config.tlsConfig().caFile(),
        x509Config.tlsConfig().tlsCertificateKeyFile().get(),
        x509Config.tlsConfig().tlsCertificateKeyFilePasswordFile());
  }

  /**
   * Builds an {@link SSLContext} for SCRAM authentication. Because SCRAM authenticates via
   * password, a CA file is optional: when absent the driver falls back to the JVM default trust
   * store for server certificate verification. When {@code tlsCertificateKeyFile} is also
   * configured, a client certificate is included to enable mTLS on top of SCRAM.
   *
   * @param scramConfig the SCRAM auth configuration, including TLS settings
   * @return an {@link SSLContext} if any TLS material is configured, or empty to use driver
   *     defaults
   */
  private static Optional<SSLContext> getScramSslContext(ScramConfig scramConfig) {
    TlsConfig tlsConfig = scramConfig.tls();
    if (tlsConfig.tlsCertificateKeyFile().isPresent()) {
      return Optional.of(
          SslContextFactory.getWithCertKeyFile(
              tlsConfig.caFile(),
              tlsConfig.tlsCertificateKeyFile().get(),
              tlsConfig.tlsCertificateKeyFilePasswordFile()));
    }
    return tlsConfig.caFile().map(SslContextFactory::getWithCaFile);
  }
}
