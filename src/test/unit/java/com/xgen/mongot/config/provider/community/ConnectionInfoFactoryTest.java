package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.net.HostAndPort;
import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.mongot.util.mongodb.SslContextFactory;
import java.io.IOException;
import java.math.BigInteger;
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
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)));
  }

  private static ReplicaSetConfig replicaSetConfig(X509Config x509) {
    return new ReplicaSetConfig(HOSTS, Optional.empty(), Optional.of(x509), Optional.empty());
  }

  private static ReplicaSetConfig replicaSetConfig(ScramConfig scramConfig) {
    return new ReplicaSetConfig(
        HOSTS, Optional.empty(), Optional.empty(), Optional.of(scramConfig));
  }

  private static ReplicaSetConfig replicaSetConfigSrv(
      String username, Path passwordFile, TlsConfig tls) {
    return new ReplicaSetConfig(
        List.of(),
        Optional.of(SRV_HOST),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)));
  }

  private static RouterConfig routerConfig(String username, Path passwordFile, TlsConfig tls) {
    return new RouterConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)));
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
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP);

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP);

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

      ConnectionInfo info = ConnectionInfoFactory.getClusterConnectionInfo(config, PRIMARY_RP);

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

      ConnectionInfo info = ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0));

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
        new ReplicaSetConfig(HOSTS, Optional.empty(), Optional.empty(), Optional.empty());

    assertThrows(
        AssertionError.class,
        () -> ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP));
  }

  @Test
  public void getSingleHostConnectionInfo_noAuthConfig_throwsAssertionError() {
    ReplicaSetConfig config =
        new ReplicaSetConfig(HOSTS, Optional.empty(), Optional.empty(), Optional.empty());

    assertThrows(
        AssertionError.class,
        () -> ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0)));
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
            () -> ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP));

    assertThat(e).hasMessageThat().contains("caFile must be set within x509 config");
  }

  @Test
  public void getSingleHostConnectionInfo_usernamePassword_parsesAsExpectedUri()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("testuser", passwordFile, TLS_DISABLED);

      ConnectionInfo info = ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0));

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
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, oneHost.get(0));

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
              config, HOSTS.get(0), ReadPreference.secondary());

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
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), rpWithTags);

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

      ConnectionInfo info = ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithTags);

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithSpace);
      assertThat(infoWithSpace.uri().getConnectionString())
          .contains("readPreferenceTags=dc:east+coast");

      ReadPreference rpWithPlus =
          ReadPreference.nearest(List.of(new TagSet(List.of(new Tag("key+name", "value")))));

      ConnectionInfo infoWithPlus =
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithPlus);

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

      ConnectionInfo info = ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0));

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

      ConnectionInfo info = ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0));

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, SECONDARY_PREFERRED_RP);

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
}
