package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.Var;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.net.ssl.SSLContext;

public class ConnectionInfoFactory {

  public static ConnectionInfo getClusterConnectionInfo(
      MongoConnectionConfig config, ReadPreference readPreference, Optional<Path> caFile) {
    return new ConnectionInfo(
        getClusterConnectionString(config, readPreference), getSslContext(config, caFile));
  }

  /**
   * Creates a {@link ConnectionInfo} for a direct connection ({@code directConnection=true}) to the
   * specified {@code hostAndPort}, using the authentication and TLS settings from {@code config}.
   */
  public static ConnectionInfo getSingleHostConnectionInfo(
      MongoConnectionConfig config, HostAndPort hostAndPort, Optional<Path> caFile) {
    return new ConnectionInfo(
        getSingleHostConnectionString(config, hostAndPort, Optional.empty()),
        getSslContext(config, caFile));
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
      MongoConnectionConfig config,
      HostAndPort hostAndPort,
      Optional<Path> caFile,
      ReadPreference readPreference) {
    return new ConnectionInfo(
        getSingleHostConnectionString(config, hostAndPort, Optional.of(readPreference)),
        getSslContext(config, caFile));
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

    if (config.scram().isPresent()) {
      ScramConfig scramConfig = config.scram().get();
      return buildScramConnectionString(scramConfig, connectionStringBuilder);
    }
    if (config.x509().isPresent()) {
      return buildx509ConnectionString(connectionStringBuilder);
    }
    return buildLegacyConnectionString(config, connectionStringBuilder);
  }

  // Todo(CLOUDP-395903): Remove legacy connection string approach through top level members
  private static ConnectionString buildLegacyConnectionString(
      MongoConnectionConfig config, ConnectionStringBuilder connectionStringBuilder) {
    // Legacy Approach - Uses top level attributes.
    connectionStringBuilder
        .withOption("readConcernLevel", ReadConcernLevel.MAJORITY.getValue())
        .withOption("tls", Boolean.toString(config.tls()));

    String replicaSetPassword =
        Crash.because("failed to read password file")
            .ifThrowsExceptionOrError(
                () -> SecretsParser.readSecretFile(config.passwordFile().get()));

    connectionStringBuilder
        .withAuthenticationCredentials(config.username().get(), replicaSetPassword)
        .withAuthenticationDatabase(config.authSource());

    return Crash.because("failed to construct connection string")
        .ifThrowsExceptionOrError(connectionStringBuilder::build);
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
        .ifThrowsExceptionOrError(connectionStringBuilder::build);
  }

  private static ConnectionString buildx509ConnectionString(
      ConnectionStringBuilder connectionStringBuilder) {

    connectionStringBuilder
        .withOption("readConcernLevel", ReadConcernLevel.MAJORITY.getValue())
        .withOption("tls", Boolean.toString(true));

    connectionStringBuilder.withX509Config();

    return Crash.because("failed to construct connection string")
        .ifThrowsExceptionOrError(connectionStringBuilder::build);
  }

  private static Optional<SSLContext> getSslContext(
      MongoConnectionConfig connectionConfig, Optional<Path> caFile) {

    if (connectionConfig.scram().isPresent()) {
      return getScramSslContext(connectionConfig.scram().get());
    }

    if (connectionConfig.x509().isPresent()) {
      X509Config x509Config = connectionConfig.x509().get();
      return Optional.of(getX509SslContext(x509Config, caFile));
    }

    // Legacy approach to generate ssl context from top level members
    return caFile.map(SslContextFactory::getWithCaFile);
  }

  /**
   * Builds an {@link SSLContext} for x509 client-certificate authentication. A CA file is required
   * (either inline on {@code x509Config} or via the legacy {@code caFile} parameter). x509 setups
   * use a private CA not present in the JVM default trust store, so falling back to it would either
   * fail verification or silently accept any server certificate signed by a public CA.
   *
   * @param x509Config the x509 auth configuration, which may carry an inline CA and cert-key file
   * @param caFile legacy CA file path inherited from the parent config; used when not set inline
   * @return an {@link SSLContext} configured with the resolved CA and client certificate
   */
  private static SSLContext getX509SslContext(X509Config x509Config, Optional<Path> caFile) {

    // Todo(CLOUDP-395903): Remove reference to legacy CA location
    @Var Optional<Path> caPath = x509Config.tlsConfig().caFile();
    if (caPath.isEmpty()) {
      caPath = caFile;
    }
    // For x509 approaches, we support both ca file passed from parent (legacy), and in-line.
    // One must be populated (mutex).
    Check.checkArg(
        caPath.isPresent(), "caFile must be present with x509 at either parent or inline");

    Check.checkArg(
        x509Config.tlsConfig().tlsCertificateKeyFile().isPresent(),
        "tlsCertificateKeyFile required for x509 auth");

    return SslContextFactory.getWithCertKeyFile(
        caPath,
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
