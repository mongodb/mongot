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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class ConnectionInfoFactoryTest {

  private static final List<HostAndPort> HOSTS =
      List.of(HostAndPort.fromParts("localhost", 27017), HostAndPort.fromParts("localhost", 27018));

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
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(new ScramConfig(Databases.ADMIN, username, passwordFile, tls)));
  }

  private static ReplicaSetConfig replicaSetConfig(X509Config x509) {
    return new ReplicaSetConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(x509),
        Optional.empty());
  }

  private static ReplicaSetConfig replicaSetConfig(ScramConfig scramConfig) {
    return new ReplicaSetConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(scramConfig));
  }

  private static RouterConfig routerConfig(String username, Path passwordFile, TlsConfig tls) {
    return new RouterConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
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
          ConnectionInfoFactory.getClusterConnectionInfo(
              config, SECONDARY_PREFERRED_RP, Optional.empty());

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
      assertThat(info.sslContext()).isEmpty();
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
          ConnectionInfoFactory.getClusterConnectionInfo(
              config, SECONDARY_PREFERRED_RP, Optional.empty());

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, PRIMARY_RP, Optional.empty());

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
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), Optional.empty());

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getHosts()).containsExactly("localhost:27017");
      assertThat(info.uri().getConnectionString()).contains("directConnection=true");
      assertThat(info.uri().getConnectionString()).doesNotContain("readPreference");
    } finally {
      Files.deleteIfExists(passwordFile);
    }
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
                    config, SECONDARY_PREFERRED_RP, Optional.empty()));

    assertThat(e).hasMessageThat().contains("caFile must be present with x509");
  }

  @Test
  public void getSingleHostConnectionInfo_usernamePassword_parsesAsExpectedUri()
      throws IOException {
    Path passwordFile = createPasswordFile("secret"); // kingfisher:ignore
    try {
      ReplicaSetConfig config = replicaSetConfig("testuser", passwordFile, TLS_DISABLED);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), Optional.empty());

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
      assertThat(info.sslContext()).isEmpty();
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
          ConnectionInfoFactory.getSingleHostConnectionInfo(
              config, oneHost.get(0), Optional.empty());

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
              config, HOSTS.get(0), Optional.empty(), ReadPreference.secondary());

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
              config, HOSTS.get(0), Optional.empty(), rpWithTags);

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithTags, Optional.empty());

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
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithSpace, Optional.empty());
      assertThat(infoWithSpace.uri().getConnectionString())
          .contains("readPreferenceTags=dc:east+coast");

      ReadPreference rpWithPlus =
          ReadPreference.nearest(List.of(new TagSet(List.of(new Tag("key+name", "value")))));

      ConnectionInfo infoWithPlus =
          ConnectionInfoFactory.getClusterConnectionInfo(config, rpWithPlus, Optional.empty());

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
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), Optional.empty());

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
  public void getSingleHostConnectionInfo_scramWithoutCertificateKeyFile_sslContextIsEmpty()
      throws IOException {
    Path passwordFile = createPasswordFile("hunter2"); // kingfisher:ignore
    try {
      TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
      ScramConfig scram = new ScramConfig(Databases.ADMIN, "__system", passwordFile, tls);
      ReplicaSetConfig config = replicaSetConfig(scram);

      ConnectionInfo info =
          ConnectionInfoFactory.getSingleHostConnectionInfo(config, HOSTS.get(0), Optional.empty());

      ConnectionString cs = new ConnectionString(info.uri().getConnectionString());
      assertThat(cs.getCredential()).isNotNull();
      assertThat(cs.getCredential().getUserName()).isEqualTo("__system");
      assertThat(cs.getCredential().getSource()).isEqualTo("admin");
      assertThat(new String(cs.getCredential().getPassword())).isEqualTo("hunter2");

      assertThat(info.sslContext()).isEmpty();
    } finally {
      Files.deleteIfExists(passwordFile);
    }
  }

}
