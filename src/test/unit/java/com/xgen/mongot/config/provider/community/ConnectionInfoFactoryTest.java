package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.net.HostAndPort;
import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.OidcCallback;
import com.mongodb.MongoCredential.OidcCallbackContext;
import com.mongodb.MongoCredential.OidcCallbackResult;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SslContextFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectionInfoFactoryTest {

  private static Path tlsCaFile;
  private static Path tlsClientCombinedFile;

  @BeforeClass
  public static void generateTlsCerts() throws Exception {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048, new SecureRandom());

    KeyPair caKeyPair = kpg.generateKeyPair();
    X509Certificate caCert = buildSelfSignedCert(caKeyPair, "CN=Test CA");

    tlsCaFile = Files.createTempFile("conn-info-test-ca", ".pem");
    Files.writeString(tlsCaFile, toPemCert(caCert));

    KeyPair clientKeyPair = kpg.generateKeyPair();
    X509Certificate clientCert =
        buildSignedCert(clientKeyPair, "CN=Test Client", caKeyPair, caCert);

    tlsClientCombinedFile = Files.createTempFile("conn-info-test-client", ".pem");
    Files.writeString(
        tlsClientCombinedFile, toPemPrivateKey(clientKeyPair) + toPemCert(clientCert));
  }

  @AfterClass
  public static void deleteTlsCerts() throws Exception {
    if (tlsCaFile != null) {
      Files.deleteIfExists(tlsCaFile);
    }
    if (tlsClientCombinedFile != null) {
      Files.deleteIfExists(tlsClientCombinedFile);
    }
  }

  private static X509Certificate buildSelfSignedCert(KeyPair keyPair, String dn) throws Exception {
    X500Name name = new X500Name(dn);
    Instant now = Instant.now();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            name,
            BigInteger.valueOf(1),
            Date.from(now),
            Date.from(now.plus(365, ChronoUnit.DAYS)),
            name,
            keyPair.getPublic());
    ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());
    return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
  }

  private static X509Certificate buildSignedCert(
      KeyPair subjectKeyPair, String subjectDn, KeyPair caKeyPair, X509Certificate caCert)
      throws Exception {
    X500Name issuer = new X500Name(caCert.getSubjectX500Principal().getName());
    X500Name subject = new X500Name(subjectDn);
    Instant now = Instant.now();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            issuer,
            BigInteger.valueOf(2),
            Date.from(now),
            Date.from(now.plus(365, ChronoUnit.DAYS)),
            subject,
            subjectKeyPair.getPublic());
    ContentSigner signer =
        new JcaContentSignerBuilder("SHA256WithRSA").build(caKeyPair.getPrivate());
    return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
  }

  private static String toPemPrivateKey(KeyPair keyPair) {
    byte[] encoded = keyPair.getPrivate().getEncoded();
    String b64 =
        Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(encoded);
    return "-----BEGIN PRIVATE KEY-----\n" + b64 + "\n-----END PRIVATE KEY-----\n";
  }

  private static String toPemCert(X509Certificate cert) throws Exception {
    byte[] encoded = cert.getEncoded();
    String b64 =
        Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(encoded);
    return "-----BEGIN CERTIFICATE-----\n" + b64 + "\n-----END CERTIFICATE-----\n";
  }

  private static final List<HostAndPort> HOSTS =
      List.of(HostAndPort.fromParts("localhost", 27017), HostAndPort.fromParts("localhost", 27018));
  private static final String SRV_HOST = "srv.example.com";

  private static final ReadPreference SECONDARY_PREFERRED_RP = ReadPreference.secondaryPreferred();
  private static final ReadPreference PRIMARY_RP = ReadPreference.primary();

  private static final TlsConfig TLS_DISABLED =
      new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());

  /** For tests that don't use OIDC: any call to {@code create} fails. */
  private static final OidcCallbackFactory NO_OIDC = OidcCallbackFactory.unsupported();

  private static final TlsConfig TLS_ENABLED =
      new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.empty());

  private static ReplicaSetConfig replicaSetConfig(
      String username, Path passwordFile, TlsConfig tls) {
    return replicaSetConfig(HOSTS, username, passwordFile, tls);
  }

  private static ReplicaSetConfig replicaSetConfig(
      List<HostAndPort> hosts, String username, Path passwordFile, TlsConfig tls) {
    return new ReplicaSetConfig(
        hosts,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)),
        Optional.empty());
  }

  private static ReplicaSetConfig replicaSetConfig(X509Config x509) {
    return new ReplicaSetConfig(
        HOSTS, Optional.empty(), Optional.of(x509), Optional.empty(), Optional.empty());
  }

  private static ReplicaSetConfig replicaSetConfig(ScramConfig scramConfig) {
    return new ReplicaSetConfig(
        HOSTS, Optional.empty(), Optional.empty(), Optional.of(scramConfig), Optional.empty());
  }

  /** A config with an {@code oidc} block. */
  private static ReplicaSetConfig replicaSetConfig(OidcConfig oidcConfig) {
    return new ReplicaSetConfig(
        HOSTS, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(oidcConfig));
  }

  private static ReplicaSetConfig replicaSetConfigSrv(
      String username, Path passwordFile, TlsConfig tls) {
    return new ReplicaSetConfig(
        List.of(),
        Optional.of(SRV_HOST),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)),
        Optional.empty());
  }

  private static RouterConfig routerConfig(String username, Path passwordFile, TlsConfig tls) {
    return new RouterConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)),
        Optional.empty());
  }

  private static Path createPasswordFile(String password) throws IOException {
    Path temp = Files.createTempFile("mongot-connection-info-test", ".pass");
    Files.writeString(temp, password);
    try {
      Files.setPosixFilePermissions(temp, PosixFilePermissions.fromString("r--------"));
    } catch (UnsupportedOperationException ignored) {
      // POSIX permissions not supported on this filesystem (e.g., Windows)
    }
    return temp;
  }

  @Test
  public void getClusterConnectionInfo_replicaSet_usernamePassword_parsesAsExpectedClusterUri()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("testuser", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).containsExactly("localhost:27017", "localhost:27018");
      assertThat(cs.getReadPreference()).isEqualTo(ReadPreference.secondaryPreferred());
      assertThat(cs.getReadConcern()).isEqualTo(ReadConcern.MAJORITY);
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("testuser");
      assertThat(cs.getCredential().getSource()).isEqualTo("admin");
      String raw = info.uri().getConnectionString();
      assertThat(raw).contains("tls=false");
      assertThat(raw).contains("directConnection=false");
      assertThat(raw).contains("readConcernLevel=majority");
      assertThat(info.sslContextFactory()).isEmpty();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getClusterConnectionInfo_replicaSet_tlsTrue_uriContainsTlsTrue() throws IOException {
    Path passwordFile = createPasswordFile("pass"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("u", passwordFile, TLS_ENABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getReadConcern()).isEqualTo(ReadConcern.MAJORITY);
      assertThat(info.uri().getConnectionString()).contains("tls=true");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getClusterConnectionInfo_router_primaryReadPreference() throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      RouterConfig config = routerConfig("u", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getClusterConnectionInfo(config, PRIMARY_RP, NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getReadPreference()).isEqualTo(ReadPreference.primary());
      assertThat(cs.getReadConcern()).isEqualTo(ReadConcern.MAJORITY);
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSingleHostConnectionInfo_router_singleHostInUri() throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      RouterConfig config = routerConfig("u", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).containsExactly("localhost:27017");
      assertThat(info.uri().getConnectionString()).contains("directConnection=true");
      assertThat(info.uri().getConnectionString()).doesNotContain("readPreference");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getClusterConnectionInfo_noAuthConfig_throwsAssertionError() {
    ReplicaSetConfig config =
        new ReplicaSetConfig(
            HOSTS, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    assertThrows(
        AssertionError.class,
        () ->
            ConnectionInfoFactory.getClusterConnectionInfo(
                config, SECONDARY_PREFERRED_RP, NO_OIDC));
  }

  @Test
  public void getSingleHostConnectionInfo_noAuthConfig_throwsAssertionError() {
    ReplicaSetConfig config =
        new ReplicaSetConfig(
            HOSTS, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    assertThrows(
        AssertionError.class,
        () -> ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), NO_OIDC));
  }

  @Test
  public void getClusterConnectionInfo_X509ConfigWithoutCaFile_ThrowsIllegalArgumentException() {
    TlsConfig tlsConfig =
        new TlsConfig(
            true,
            Optional.of(Path.of("/etc/certs/client.pem")),
            Optional.empty(),
            Optional.empty());
    X509Config x509Config = new X509Config(tlsConfig);
    ReplicaSetConfig config = replicaSetConfig(x509Config);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ConnectionInfoFactory.getClusterConnectionInfo(
                    config, SECONDARY_PREFERRED_RP, NO_OIDC));

    assertThat(e).hasMessageThat().contains("caFile must be set within x509 config");
  }

  @Test
  public void getSingleHostConnectionInfo_usernamePassword_parsesAsExpectedUri()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("testuser", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).hasSize(1);
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("testuser");
      assertThat(cs.getCredential().getSource()).isEqualTo("admin");
      assertThat(cs.getReadConcern()).isEqualTo(ReadConcern.MAJORITY);
      String uri = info.uri().getConnectionString();
      assertThat(uri).contains("tls=false");
      assertThat(uri).contains("directConnection=true");
      assertThat(uri).contains("readConcernLevel=majority");
      assertThat(info.sslContextFactory()).isEmpty();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSingleHostConnectionInfo_singleReplicaHost_uriHasOnlyThatHost()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      List<HostAndPort> oneHost = List.of(HostAndPort.fromParts("sync.example", 27019));
      ReplicaSetConfig config = replicaSetConfig(oneHost, "u", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, oneHost.get(0), NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).containsExactly("sync.example:27019");
      assertThat(info.uri().getConnectionString()).contains("directConnection=true");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void
      getSingleHostConnectionInfo_withReadPreference_uriHasReadPreferenceAndNoDirectConnect()
          throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("u", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(
              config, HOSTS.get(0), ReadPreference.secondary(), NO_OIDC);

      String uri = info.uri().getConnectionString();
      assertThat(uri).contains("readPreference=secondary");
      assertThat(uri).doesNotContain("directConnection");
      assertThat(new ConnectionString(uri).getHosts()).containsExactly("localhost:27017");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSingleHostConnectionInfo_withReadPreferenceAndTagSets_uriHasTagSets()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("u", passwordFile, TLS_DISABLED);
      ReadPreference rpWithTags =
          ReadPreference.secondary(List.of(new TagSet(List.of(new Tag("dc", "east")))));

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(
              config, HOSTS.get(0), rpWithTags, NO_OIDC);

      String uri = info.uri().getConnectionString();
      assertThat(uri).contains("readPreference=secondary");
      assertThat(uri).contains("readPreferenceTags=dc:east");
      assertThat(uri).doesNotContain("directConnection");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getClusterConnectionInfo_withTagSets_encodedAsReadPreferenceTags()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("u", passwordFile, TLS_DISABLED);
      ReadPreference rpWithTags =
          ReadPreference.nearest(
              List.of(
                  new TagSet(List.of(new Tag("dc", "east"), new Tag("rack", "1"))),
                  new TagSet(List.of(new Tag("dc", "west")))));

      ConnectionInfo info =
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithTags, NO_OIDC);

      String uri = info.uri().getConnectionString();
      assertThat(uri).contains("readPreference=nearest");
      assertThat(uri).contains("readPreferenceTags=dc:east,rack:1");
      assertThat(uri).contains("readPreferenceTags=dc:west");
      assertThat(new ConnectionString(uri).getReadPreference()).isEqualTo(rpWithTags);
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getClusterConnectionInfo_tagValueSpecialCharacters_urlEncodedAsUri()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("u", passwordFile, TLS_DISABLED);
      ReadPreference rpWithSpace =
          ReadPreference.nearest(List.of(new TagSet(List.of(new Tag("dc", "east coast")))));

      ConnectionInfo infoWithSpace =
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithSpace, NO_OIDC);
      assertThat(infoWithSpace.uri().getConnectionString())
          .contains("readPreferenceTags=dc:east+coast");

      ReadPreference rpWithPlus =
          ReadPreference.nearest(List.of(new TagSet(List.of(new Tag("key+name", "value")))));

      ConnectionInfo infoWithPlus =
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithPlus, NO_OIDC);

      assertThat(infoWithPlus.uri().getConnectionString())
          .contains("readPreferenceTags=key%2Bname:value");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSingleHostConnectionInfo_scramConfig_buildsScramConnectionString()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      TlsConfig tls = new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.empty());
      ScramConfig scram = new ScramConfig("dummy", "__system", passwordFile, tls);
      ReplicaSetConfig config = replicaSetConfig(scram);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("__system");
      assertThat(cs.getCredential().getSource()).isEqualTo("dummy");
      assertThat(new String(cs.getCredential().getPassword())).isEqualTo("secret");

      String uri = info.uri().getConnectionString();
      assertThat(uri).contains("tls=true");
      assertThat(uri).contains("readConcernLevel=majority");
      assertThat(uri).contains("directConnection=true");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSingleHostConnectionInfo_scramWithoutCertificateKeyFile_sslContextFactoryIsEmpty()
      throws IOException {
    Path passwordFile = createPasswordFile("hunter2"); // kingfisher:ignore
    try {
      TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
      ScramConfig scram = new ScramConfig(Databases.ADMIN, "__system", passwordFile, tls);
      ReplicaSetConfig config = replicaSetConfig(scram);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("__system");
      assertThat(cs.getCredential().getSource()).isEqualTo("admin");
      assertThat(new String(cs.getCredential().getPassword())).isEqualTo("hunter2");

      assertThat(info.sslContextFactory()).isEmpty();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSslContextFactory_scramWithCaFileOnly_returnsPresentSslContextFactory()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      TlsConfig tls =
          new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.of(tlsCaFile));
      ScramConfig scram = new ScramConfig(Databases.ADMIN, "__system", passwordFile, tls);
      ReplicaSetConfig config = replicaSetConfig(scram);

      Optional<SslContextFactory> result = ConnectionInfoFactory.getSslContextFactory(config);

      assertThat(result).isPresent();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSslContextFactory_scramWithCaFileAndCertKey_returnsPresentSslContextFactory()
      throws IOException {
    Path passwordFile = createPasswordFile("p"); // kingfisher:ignore
    try {
      TlsConfig tls =
          new TlsConfig(
              true, Optional.of(tlsClientCombinedFile), Optional.empty(), Optional.of(tlsCaFile));
      ScramConfig scram = new ScramConfig(Databases.ADMIN, "__system", passwordFile, tls);
      ReplicaSetConfig config = replicaSetConfig(scram);

      Optional<SslContextFactory> result = ConnectionInfoFactory.getSslContextFactory(config);

      assertThat(result).isPresent();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  @Test
  public void getSslContextFactory_x509WithoutCertKeyFile_throwsIllegalArgumentException() {
    TlsConfig tls = new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.of(tlsCaFile));
    X509Config x509 = new X509Config(tls);
    ReplicaSetConfig config = replicaSetConfig(x509);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConnectionInfoFactory.getSslContextFactory(config));

    assertThat(e).hasMessageThat().contains("tlsCertificateKeyFile required for x509 auth");
  }

  @Test
  public void getClusterConnectionInfo_srvReplicaSet_usernamePassword_parsesAsExpected()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfigSrv("testuser", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, NO_OIDC);

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).containsExactly(SRV_HOST);
      assertThat(cs.getHosts()).hasSize(1);
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("testuser");
      assertThat(cs.getCredential().getSource()).isEqualTo("admin");
      String raw = info.uri().getConnectionString();
      assertThat(raw).startsWith("mongodb+srv://");
      assertThat(raw).contains("tls=false");
      assertThat(raw).contains("directConnection=false");
      assertThat(raw).contains("readConcernLevel=majority");
      assertThat(info.sslContextFactory()).isEmpty();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

  /** OIDC TLS settings; the CA file must exist because the SSL context is built from it. */
  private static TlsConfig oidcTls() {
    return new TlsConfig(true, Optional.empty(), Optional.empty(), Optional.of(tlsCaFile));
  }

  /** A custom {@code oidc} config. */
  private static CustomOidcConfig oidcConfig(TlsConfig tls) {
    return new CustomOidcConfig(
        URI.create("https://idp.example.com/token"),
        "mongot",
        Path.of("/etc/mongot/secrets/oidc-client-secret"),
        Path.of("/etc/mongot/tls/idp-ca.pem"),
        Optional.empty(),
        tls);
  }

  private static OidcCallback noopCallback() {
    return context -> new OidcCallbackResult("token");
  }

  @Test
  public void getClusterConnectionInfo_oidc_uriHasMechanismAndNoMechanismProperties() {
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(
            config, SECONDARY_PREFERRED_RP, new OidcCallbackFactory(oidc -> noopCallback()));

    String uri = info.uri().getConnectionString();
    assertThat(uri).contains("authMechanism=MONGODB-OIDC");
    assertThat(uri).contains("authSource=$external");
    assertThat(uri).doesNotContain("authMechanismProperties");
    assertThat(uri).contains("tls=true");
    assertThat(uri).contains("readConcernLevel=majority");
    assertThat(uri).doesNotContain("@");
    // The token callback travels via ConnectionInfo; the URI carries only the mechanism.
    ConnectionString cs = new ConnectionString(uri);
    assertThat(cs.getHosts()).containsExactly("localhost:27017", "localhost:27018");
    assertThat(cs.getCredential().getMechanism()).isEqualTo("MONGODB-OIDC");
    assertThat(info.oidcCredential()).isPresent();
  }

  @Test
  public void getSingleHostConnectionInfo_oidc_hasDirectConnectionAndMechanism() {
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getSingleHostConnectionInfo(
            config, HOSTS.get(0), new OidcCallbackFactory(oidc -> noopCallback()));

    String uri = info.uri().getConnectionString();
    assertThat(uri).contains("authMechanism=MONGODB-OIDC");
    assertThat(uri).contains("directConnection=true");
    assertThat(uri).doesNotContain("@");
  }

  @Test
  public void getClusterConnectionInfo_oidc_credentialWrapsFactoryCallback() {
    OidcCallback callback = noopCallback();
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(
            config, SECONDARY_PREFERRED_RP, new OidcCallbackFactory(oidc -> callback));

    assertThat(info.oidcCredential()).isPresent();
    MongoCredential credential = info.oidcCredential().get();
    assertThat(credential.getAuthenticationMechanism())
        .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
    OidcCallback credentialCallback =
        credential.getMechanismProperty(MongoCredential.OIDC_CALLBACK_KEY, null);
    assertThat(credentialCallback).isSameInstanceAs(callback);
  }

  @Test
  public void getClusterConnectionInfo_oidc_clientSettingsCarryCallbackCredential() {
    OidcCallback callback = noopCallback();
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));
    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(
            config, SECONDARY_PREFERRED_RP, new OidcCallbackFactory(oidc -> callback));

    try (MongoClient client =
        MongoClientBuilder.buildNonReplicationWithDefaults(
            info, "oidcTest", new SimpleMeterRegistry())) {
      MongoCredential credential = ((MongoClientImpl) client).getSettings().getCredential();

      assertThat(credential.getAuthenticationMechanism())
          .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
      OidcCallback credentialCallback =
          credential.getMechanismProperty(MongoCredential.OIDC_CALLBACK_KEY, null);
      assertThat(credentialCallback).isSameInstanceAs(callback);
    }
  }

  @Test
  public void getClusterConnectionInfo_oidc_returnsEqualCredentialAcrossBuilds() {
    // A new callback instance per build, so equality only holds if the factory caches by config.
    var factory =
        new OidcCallbackFactory(
            oidc ->
                new OidcCallback() {
                  @Override
                  public OidcCallbackResult onRequest(OidcCallbackContext context) {
                    return new OidcCallbackResult("token");
                  }
                });
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo first =
        ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, factory);
    ConnectionInfo second =
        ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, factory);

    assertThat(second.oidcCredential()).isEqualTo(first.oidcCredential());
  }

  @Test
  public void getSingleHostConnectionInfo_oidc_credentialPresent() {
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getSingleHostConnectionInfo(
            config, HOSTS.get(0), new OidcCallbackFactory(oidc -> noopCallback()));

    String uri = info.uri().getConnectionString();
    assertThat(uri).contains("directConnection=true");
    assertThat(uri).contains("authMechanism=MONGODB-OIDC");
    assertThat(info.oidcCredential()).isPresent();
    assertThat(info.oidcCredential().get().getAuthenticationMechanism())
        .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
  }

  @Test
  public void getClusterConnectionInfo_oidcTlsCaFile_sslContextFactoryPresent() {
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(
            config, SECONDARY_PREFERRED_RP, new OidcCallbackFactory(oidc -> noopCallback()));

    assertThat(info.sslContextFactory()).isPresent();
  }

  @Test
  public void getClusterConnectionInfo_oidcClientCertKeyFile_sslContextUsableForMtls() {
    TlsConfig tls =
        new TlsConfig(
            true, Optional.of(tlsClientCombinedFile), Optional.empty(), Optional.of(tlsCaFile));
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(tls));

    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(
            config, SECONDARY_PREFERRED_RP, new OidcCallbackFactory(oidc -> noopCallback()));

    assertThat(info.sslContextFactory()).isPresent();
    assertThat(info.sslContextFactory().get().getJavaContext()).isNotNull();
  }

  /** The callback is built exactly once per ConnectionInfo. */
  @Test
  public void getClusterConnectionInfo_oidc_buildsCallbackOnce() {
    AtomicInteger createCount = new AtomicInteger();
    OidcCallbackFactory factory =
        new OidcCallbackFactory(
            oidc -> {
              createCount.incrementAndGet();
              return noopCallback();
            });
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP, factory);

    assertThat(info.uri().getConnectionString()).isNotEmpty();
    assertThat(info.oidcCredential()).isPresent();
    assertThat(createCount.get()).isEqualTo(1);
  }

  @Test
  public void getSingleHostConnectionInfo_oidc_buildsCallbackOnce() {
    AtomicInteger createCount = new AtomicInteger();
    OidcCallbackFactory factory =
        new OidcCallbackFactory(
            oidc -> {
              createCount.incrementAndGet();
              return noopCallback();
            });
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    ConnectionInfo info =
        ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), factory);

    assertThat(info.uri().getConnectionString()).isNotEmpty();
    assertThat(info.oidcCredential()).isPresent();
    assertThat(createCount.get()).isEqualTo(1);
  }

  @Test
  public void getClusterConnectionInfo_oidcWithUnsupportedCallbackFactory_throwsIllegalState() {
    ReplicaSetConfig config = replicaSetConfig(oidcConfig(oidcTls()));

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                ConnectionInfoFactory.getClusterConnectionInfo(
                    config, SECONDARY_PREFERRED_RP, NO_OIDC));

    assertThat(e).hasMessageThat().contains("no OIDC callback factory is installed");
  }
}
