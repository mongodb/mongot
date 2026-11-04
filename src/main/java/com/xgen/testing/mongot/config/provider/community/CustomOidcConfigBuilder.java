package com.xgen.testing.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.CustomOidcConfig;
import com.xgen.mongot.config.provider.community.TlsConfig;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;

/** Test-data builder for {@link CustomOidcConfig}, with sane defaults for every field. */
public class CustomOidcConfigBuilder {
  private URI tokenEndpoint = URI.create("https://idp.example.com/token");
  private String clientId = "mongot";
  private Path clientSecretFile = Path.of("/etc/mongot/secrets/oidc-client-secret");
  private Path idpCaFile = Path.of("/etc/mongot/tls/idp-ca.pem");
  private Optional<String> scope = Optional.empty();
  private Optional<String> audience = Optional.empty();
  private Optional<String> resource = Optional.empty();
  private TlsConfig tls =
      new TlsConfig(
          true, Optional.empty(), Optional.empty(), Optional.of(Path.of("/etc/mongot/tls/ca.pem")));

  public static CustomOidcConfigBuilder builder() {
    return new CustomOidcConfigBuilder();
  }

  public CustomOidcConfigBuilder tokenEndpoint(URI tokenEndpoint) {
    this.tokenEndpoint = tokenEndpoint;
    return this;
  }

  public CustomOidcConfigBuilder tokenEndpoint(String tokenEndpoint) {
    return tokenEndpoint(URI.create(tokenEndpoint));
  }

  public CustomOidcConfigBuilder clientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public CustomOidcConfigBuilder clientSecretFile(Path clientSecretFile) {
    this.clientSecretFile = clientSecretFile;
    return this;
  }

  public CustomOidcConfigBuilder idpCaFile(Path idpCaFile) {
    this.idpCaFile = idpCaFile;
    return this;
  }

  public CustomOidcConfigBuilder scope(String scope) {
    return scope(Optional.of(scope));
  }

  public CustomOidcConfigBuilder scope(Optional<String> scope) {
    this.scope = scope;
    return this;
  }

  public CustomOidcConfigBuilder audience(String audience) {
    return audience(Optional.of(audience));
  }

  public CustomOidcConfigBuilder audience(Optional<String> audience) {
    this.audience = audience;
    return this;
  }

  public CustomOidcConfigBuilder resource(String resource) {
    return resource(Optional.of(resource));
  }

  public CustomOidcConfigBuilder resource(Optional<String> resource) {
    this.resource = resource;
    return this;
  }

  public CustomOidcConfigBuilder tls(TlsConfig tls) {
    this.tls = tls;
    return this;
  }

  public CustomOidcConfig build() {
    return new CustomOidcConfig(
        this.tokenEndpoint,
        this.clientId,
        this.clientSecretFile,
        this.idpCaFile,
        this.scope,
        this.audience,
        this.resource,
        this.tls);
  }
}
