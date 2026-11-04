package com.xgen.testing.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.config.provider.community.CustomOidcConfig;
import com.xgen.mongot.config.provider.community.TlsConfig;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.Test;

public class CustomOidcConfigBuilderTest {

  @Test
  public void build_noOverrides_returnsDefaults() {
    CustomOidcConfig config = CustomOidcConfigBuilder.builder().build();

    assertThat(config.tokenEndpoint()).isEqualTo(URI.create("https://idp.example.com/token"));
    assertThat(config.clientId()).isEqualTo("mongot");
    assertThat(config.clientSecretFile())
        .isEqualTo(Path.of("/etc/mongot/secrets/oidc-client-secret"));
    assertThat(config.idpCaFile()).isEqualTo(Path.of("/etc/mongot/tls/idp-ca.pem"));
    assertThat(config.scope()).isEmpty();
    assertThat(config.audience()).isEmpty();
    assertThat(config.resource()).isEmpty();
    assertThat(config.tls().enabled()).isTrue();
    assertThat(config.tls().caFile()).hasValue(Path.of("/etc/mongot/tls/ca.pem"));
  }

  @Test
  public void build_allOverrides_returnsConfiguredValues() {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.of(Path.of("/etc/mongot/tls/client.pem")),
            Optional.empty(),
            Optional.of(Path.of("/etc/mongot/tls/other-ca.pem")));

    CustomOidcConfig config =
        CustomOidcConfigBuilder.builder()
            .tokenEndpoint("https://other-idp.example.com/oauth/token")
            .clientId("other-client")
            .clientSecretFile(Path.of("/var/secrets/other-secret"))
            .idpCaFile(Path.of("/var/tls/other-idp-ca.pem"))
            .scope("search read")
            .audience("https://mongodb.example.com")
            .resource("mongodb://rs.example.com")
            .tls(tls)
            .build();

    assertThat(config.tokenEndpoint())
        .isEqualTo(URI.create("https://other-idp.example.com/oauth/token"));
    assertThat(config.clientId()).isEqualTo("other-client");
    assertThat(config.clientSecretFile()).isEqualTo(Path.of("/var/secrets/other-secret"));
    assertThat(config.idpCaFile()).isEqualTo(Path.of("/var/tls/other-idp-ca.pem"));
    assertThat(config.scope()).hasValue("search read");
    assertThat(config.audience()).hasValue("https://mongodb.example.com");
    assertThat(config.resource()).hasValue("mongodb://rs.example.com");
    assertThat(config.tls()).isEqualTo(tls);
  }

  @Test
  public void build_optionalOverloads_acceptOptionalValues() {
    CustomOidcConfig config =
        CustomOidcConfigBuilder.builder()
            .scope(Optional.of("search read"))
            .audience(Optional.of(""))
            .resource(Optional.empty())
            .build();

    assertThat(config.scope()).hasValue("search read");
    assertThat(config.audience()).hasValue("");
    assertThat(config.resource()).isEmpty();
  }
}
