package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import com.xgen.mongot.util.mongodb.Databases;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/** Unit tests for {@link MongoConnectionConfig#validate} validation logic. */
public class MongoConnectionConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));
  private static final String SRV_HOST = "srv.example.com";
  private static final URI TOKEN_ENDPOINT = URI.create("https://idp.example.com/token");

  private static ReplicaSetConfig config(
      Optional<X509Config> x509, Optional<ScramConfig> scram, Optional<OidcConfig> oidc) {
    return new ReplicaSetConfig(HOSTS, Optional.empty(), x509, scram, oidc);
  }

  private static ReplicaSetConfig srvConfig(
      Optional<X509Config> x509, Optional<ScramConfig> scram, Optional<OidcConfig> oidc) {
    return new ReplicaSetConfig(List.of(), Optional.of(SRV_HOST), x509, scram, oidc);
  }

  private static CustomOidcConfig oidc() {
    return oidc(oidcTls());
  }

  private static CustomOidcConfig oidc(TlsConfig tls) {
    return new CustomOidcConfig(
        TOKEN_ENDPOINT,
        "mongot",
        Path.of("/etc/mongot/secrets/oidc-client-secret"),
        Path.of("/etc/mongot/idp-ca.pem"),
        Optional.empty(),
        tls);
  }

  private static TlsConfig oidcTls() {
    return new TlsConfig(
        true, Optional.empty(), Optional.empty(), Optional.of(Path.of("/etc/mongot/tls/ca.pem")));
  }

  private static X509Config x509(Path certKeyFile) {
    TlsConfig tlsConfig =
        new TlsConfig(
            true,
            Optional.of(certKeyFile),
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    return new X509Config(tlsConfig);
  }

  private static BsonDocumentParser parser(BsonDocument document) {
    return parser(document, DeploymentEnvironment.COMMUNITY);
  }

  /**
   * The {@code oidc} field is enterprise-gated: parsing a document that carries it requires an
   * enterprise parse context.
   */
  private static BsonDocumentParser parser(BsonDocument document, DeploymentEnvironment edition) {
    return BsonDocumentParser.withContext(
            PermissiveBsonParseContext.root(Optional.of(edition)), document)
        .build();
  }

  /** Valid: x509 only. */
  @Test
  public void testValidate_x509Only_Passes() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(
            Optional.of(x509(Path.of("/etc/tls/client.pem"))), Optional.empty(), Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      cfg.validate(parser);
    }
  }

  /** Valid: scram only. */
  @Test
  public void testValidate_scramOnly_Passes() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.of(scram), Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      cfg.validate(parser);
    }
  }

  /** Valid: oidc only. */
  @Test
  public void validate_oidcOnly_passes() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(oidc()));

    try (var parser = parser(new BsonDocument())) {
      cfg.validate(parser);
    }
  }

  @Test
  public void testValidate_x509WithoutTls_ThrowsError() throws BsonParseException {
    TlsConfig tlsConfig =
        new TlsConfig(
            false,
            Optional.of(Path.of("/etc/tls/client.pem")),
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/ca.pem")));
    X509Config x509 = new X509Config(tlsConfig);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.empty(), Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals("tls must be enabled when tlsCertificateKeyFile is configured", e.getMessage());
    }
  }

  @Test
  public void testValidate_X509ConfigAndScram_ThrowsError() throws BsonParseException {
    X509Config x509 = x509(Path.of("/etc/tls/client.pem"));
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.of(scram), Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509 or scram)", e.getMessage());
    }
  }

  @Test
  public void testValidate_X509ConfigAndScram_EnterpriseHintListsOidc() throws BsonParseException {
    X509Config x509 = x509(Path.of("/etc/tls/client.pem"));
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.of(scram), Optional.empty());

    try (var parser = parser(new BsonDocument(), DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509, scram, or oidc)",
          e.getMessage());
    }
  }

  @Test
  public void validate_oidcAndScram_throwsError() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.of(scram), Optional.of(oidc()));

    // An oidc block only arises from an enterprise parse, so validate under one.
    try (var parser = parser(new BsonDocument(), DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509, scram, or oidc)",
          e.getMessage());
    }
  }

  @Test
  public void validate_oidcAndX509_throwsError() throws BsonParseException {
    X509Config x509 = x509(Path.of("/etc/tls/client.pem"));
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.empty(), Optional.of(oidc()));

    // An oidc block only arises from an enterprise parse, so validate under one.
    try (var parser = parser(new BsonDocument(), DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509, scram, or oidc)",
          e.getMessage());
    }
  }

  @Test
  public void testValidate_NoAuth_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509 or scram)", e.getMessage());
    }
  }

  @Test
  public void validate_x509AndScramAndOidc_throwsError() throws BsonParseException {
    X509Config x509 = x509(Path.of("/etc/tls/client.pem"));
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.of(x509), Optional.of(scram), Optional.of(oidc()));

    // An oidc block only arises from an enterprise parse, so validate under one.
    try (var parser = parser(new BsonDocument(), DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "Exactly one authentication mechanism must be used (x509, scram, or oidc)",
          e.getMessage());
    }
  }

  @Test
  public void testToBson_ScramConfigured_roundTrips() throws BsonParseException {
    TlsConfig tls = new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty());
    ScramConfig scram =
        new ScramConfig(Databases.ADMIN, "__system", Path.of("/etc/mongot/keyfile"), tls);
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.of(scram), Optional.empty());

    BsonDocument encoded = cfg.toBson();

    try (var parser = parser(encoded)) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      roundTripped.validate(parser);
    }
  }

  @Test
  public void testToBson_X509Configured_roundTrips() throws BsonParseException {
    ReplicaSetConfig cfg =
        config(
            Optional.of(x509(Path.of("/etc/tls/client.pem"))), Optional.empty(), Optional.empty());

    BsonDocument encoded = cfg.toBson();

    try (var parser = parser(encoded)) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      roundTripped.validate(parser);
    }
  }

  /** The typed oidc config survives a toBson round-trip unchanged. */
  @Test
  public void toBson_oidcConfigured_roundTrips() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(oidc()));

    BsonDocument encoded = cfg.toBson();

    try (var parser = parser(encoded, DeploymentEnvironment.ENTERPRISE)) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      assertEquals(oidc(), roundTripped.oidc().get());
      roundTripped.validate(parser);
    }
  }

  /** Parsing an {@code oidc} block under an enterprise context yields the typed custom config. */
  @Test
  public void fromBson_oidcCustom_parsesTypedConfig() throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.of(Path.of("/etc/tls/client.pem")),
            Optional.of(Path.of("/etc/tls/client.pw")),
            Optional.of(Path.of("/ca.pem")));
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(oidc(tls)));

    try (var parser = parser(cfg.toBson(), DeploymentEnvironment.ENTERPRISE)) {
      ReplicaSetConfig parsed = ReplicaSetConfig.fromBson(parser);

      CustomOidcConfig oidc = (CustomOidcConfig) parsed.oidc().get();
      assertEquals(TOKEN_ENDPOINT, oidc.tokenEndpoint());
      assertEquals("mongot", oidc.clientId());
      assertEquals(Path.of("/etc/mongot/secrets/oidc-client-secret"), oidc.clientSecretFile());
      assertEquals(Path.of("/etc/mongot/idp-ca.pem"), oidc.idpCaFile());
      assertEquals(Optional.empty(), oidc.scope());
      assertEquals(tls, oidc.tls());
    }
  }

  /** A configured {@code scope} survives a serialization round trip. */
  @Test
  public void fromBson_oidcWithScope_roundTripsScope() throws BsonParseException {
    CustomOidcConfig withScope =
        new CustomOidcConfig(
            TOKEN_ENDPOINT,
            "mongot",
            Path.of("/etc/mongot/secrets/oidc-client-secret"),
            Path.of("/etc/mongot/idp-ca.pem"),
            Optional.of("mongot-access"),
            oidcTls());
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(withScope));

    try (var parser = parser(cfg.toBson(), DeploymentEnvironment.ENTERPRISE)) {
      ReplicaSetConfig parsed = ReplicaSetConfig.fromBson(parser);

      assertEquals(cfg, parsed);
      assertEquals(Optional.of("mongot-access"), ((CustomOidcConfig) parsed.oidc().get()).scope());
    }
  }

  /** An empty {@code scope} is rejected at parse time. */
  @Test
  public void fromBson_oidcEmptyScope_throwsError() throws BsonParseException {
    CustomOidcConfig withScope =
        new CustomOidcConfig(
            TOKEN_ENDPOINT,
            "mongot",
            Path.of("/etc/mongot/secrets/oidc-client-secret"),
            Path.of("/etc/mongot/idp-ca.pem"),
            Optional.of(""),
            oidcTls());
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(withScope));

    try (var parser = parser(cfg.toBson(), DeploymentEnvironment.ENTERPRISE)) {
      assertThrows(BsonParseException.class, () -> ReplicaSetConfig.fromBson(parser));
    }
  }

  private static ReplicaSetConfig parseOidc(String tokenEndpoint) throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append("oidc", oidcDocument(tokenEndpoint, defaultTlsDocument()));
    try (var parser = parser(doc, DeploymentEnvironment.ENTERPRISE)) {
      return ReplicaSetConfig.fromBson(parser);
    }
  }

  private static void assertTokenEndpointRejected(String tokenEndpoint) {
    BsonParseException e = assertThrows(BsonParseException.class, () -> parseOidc(tokenEndpoint));
    assertEquals(
        "\"oidc.tokenEndpoint\" must be an absolute https URI with a host, without userinfo or a"
            + " fragment",
        e.getMessage());
  }

  private static void assertTokenEndpointAccepted(String tokenEndpoint) throws BsonParseException {
    ReplicaSetConfig parsed = parseOidc(tokenEndpoint);
    assertEquals(
        URI.create(tokenEndpoint), ((CustomOidcConfig) parsed.oidc().get()).tokenEndpoint());
  }

  @Test
  public void fromBson_oidcRelativeTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("realms/mongot/token");
  }

  @Test
  public void fromBson_oidcEmptyTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("");
  }

  @Test
  public void fromBson_oidcHttpTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("http://idp.example.com/token");
  }

  @Test
  public void fromBson_oidcFileTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("file:/token");
  }

  @Test
  public void fromBson_oidcOpaqueTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("https:token");
  }

  @Test
  public void fromBson_oidcHostlessTokenEndpoint_throwsError() {
    assertTokenEndpointRejected("https:///x");
  }

  @Test
  public void fromBson_oidcTokenEndpointWithUserInfo_throwsError() {
    assertTokenEndpointRejected("https://user@idp.example.com/token");
  }

  @Test
  public void fromBson_oidcTokenEndpointWithFragment_throwsError() {
    assertTokenEndpointRejected("https://idp.example.com/token#frag");
  }

  @Test
  public void fromBson_oidcTokenEndpointWithOutOfRangePort_throwsError() {
    BsonParseException e =
        assertThrows(
            BsonParseException.class, () -> parseOidc("https://idp.example.com:99999/token"));
    assertEquals("\"oidc.tokenEndpoint\" must specify a port between 1 and 65535", e.getMessage());
  }

  @Test
  public void fromBson_oidcTokenEndpointWithPortZero_throwsError() {
    BsonParseException e =
        assertThrows(BsonParseException.class, () -> parseOidc("https://idp.example.com:0/token"));
    assertEquals("\"oidc.tokenEndpoint\" must specify a port between 1 and 65535", e.getMessage());
  }

  @Test
  public void fromBson_oidcInvalidSyntaxTokenEndpoint_throwsError() {
    BsonParseException e =
        assertThrows(BsonParseException.class, () -> parseOidc("https://idp example.com/token"));
    assertTrue(
        e.getMessage(), e.getMessage().startsWith("\"oidc.tokenEndpoint\" is not a valid URI: "));
  }

  @Test
  public void fromBson_oidcUppercaseHttpsScheme_passes() throws BsonParseException {
    assertTokenEndpointAccepted("HTTPS://idp.example.com/token");
  }

  @Test
  public void fromBson_oidcHttpsTokenEndpointWithPort_passes() throws BsonParseException {
    assertTokenEndpointAccepted("https://idp.example.com:8443/realms/x/token");
  }

  private static BsonDocument defaultTlsDocument() {
    return new BsonDocument().append("caFile", new BsonString("/etc/mongot/tls/ca.pem"));
  }

  private static BsonDocument oidcDocument(BsonDocument tls) {
    return oidcDocument(TOKEN_ENDPOINT.toString(), tls);
  }

  private static BsonDocument oidcDocument(String tokenEndpoint, BsonDocument tls) {
    BsonDocument oidc =
        new BsonDocument()
            .append("environment", new BsonString("custom"))
            .append("tokenEndpoint", new BsonString(tokenEndpoint))
            .append("clientId", new BsonString("mongot"))
            .append("clientSecretFile", new BsonString("/etc/mongot/secrets/oidc-client-secret"))
            .append("idpCaFile", new BsonString("/etc/mongot/idp-ca.pem"));
    if (tls != null) {
      oidc.append("tls", tls);
    }
    return oidc;
  }

  @Test
  public void fromBson_oidcWithoutTls_throwsError() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append("oidc", oidcDocument(null));

    try (var parser = parser(doc, DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> ReplicaSetConfig.fromBson(parser));
      assertEquals("\"oidc.tls\" is required", e.getMessage());
    }
  }

  /**
   * An oidc tls block without a CA file parses (caFile is optional in the shared {@link
   * TlsConfig}); the CA requirement is enforced at validate time, with no fallback to the JVM
   * default trust store.
   */
  @Test
  public void validate_oidcTlsWithoutCaFile_throwsError() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append("oidc", oidcDocument(new BsonDocument()));

    try (var parser = parser(doc, DeploymentEnvironment.ENTERPRISE)) {
      ReplicaSetConfig cfg = ReplicaSetConfig.fromBson(parser);
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals("caFile must be set within the oidc config", e.getMessage());
    }
  }

  @Test
  public void fromBson_oidcMissingIdpCaFile_throwsError() throws BsonParseException {
    BsonDocument oidc = oidcDocument(defaultTlsDocument());
    oidc.remove("idpCaFile");
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append("oidc", oidc);

    try (var parser = parser(doc, DeploymentEnvironment.ENTERPRISE)) {
      BsonParseException e =
          assertThrows(BsonParseException.class, () -> ReplicaSetConfig.fromBson(parser));
      assertEquals("\"oidc.idpCaFile\" is required", e.getMessage());
    }
  }

  /** TLS is always enabled for oidc; an omitted {@code enabled} parses as true. */
  @Test
  public void fromBson_oidcTlsEnabledOmitted_tlsEnabled() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append("oidc", oidcDocument(defaultTlsDocument()));

    try (var parser = parser(doc, DeploymentEnvironment.ENTERPRISE)) {
      ReplicaSetConfig cfg = ReplicaSetConfig.fromBson(parser);
      assertTrue(cfg.oidc().get().tls().enabled());
      cfg.validate(parser);
    }
  }

  /** {@code enabled} is not a field of the oidc tls block, so an explicit value is unknown. */
  @Test
  public void fromBson_oidcTlsExplicitEnabled_permissiveContextCollectsUnknownFieldWarning()
      throws BsonParseException {
    for (boolean enabled : List.of(true, false)) {
      BsonDocument doc =
          new BsonDocument()
              .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
              .append(
                  "oidc",
                  oidcDocument(defaultTlsDocument().append("enabled", new BsonBoolean(enabled))));

      PermissiveBsonParseContext context =
          PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.ENTERPRISE));
      try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
        ReplicaSetConfig cfg = ReplicaSetConfig.fromBson(parser);
        assertTrue(cfg.oidc().get().tls().enabled());
      }

      assertEquals(1, context.getUnknownFieldExceptions().size());
      assertEquals(
          "\"oidc.tls\" unrecognized field \"enabled\". Expected fields are: "
              + "caFile, tlsCertificateKeyFile, tlsCertificateKeyFilePasswordFile",
          context.getUnknownFieldExceptions().get(0).getMessage());
    }
  }

  /** The encoded oidc tls block has no {@code enabled} key and re-parses to an equal config. */
  @Test
  public void toBson_oidcTls_omitsEnabledAndRoundTrips() throws BsonParseException {
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(oidc()));
    BsonDocument encoded = cfg.toBson();
    BsonDocument tls = encoded.getDocument("oidc").getDocument("tls");
    assertFalse(tls.containsKey("enabled"));

    try (var parser = parser(encoded, DeploymentEnvironment.ENTERPRISE)) {
      assertEquals(oidcTls(), ReplicaSetConfig.fromBson(parser).oidc().get().tls());
    }
  }

  @Test
  public void fromBson_oidcTlsUnknownField_permissiveContextCollectsUnknownFieldWarning()
      throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("hostAndPort", new BsonArray(List.of(new BsonString("localhost:27017"))))
            .append(
                "oidc",
                oidcDocument(
                    defaultTlsDocument()
                        .append("caFilee", new BsonString("/etc/mongot/tls/ca.pem"))));

    PermissiveBsonParseContext context =
        PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.ENTERPRISE));
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      ReplicaSetConfig.fromBson(parser);
    }

    assertEquals(1, context.getUnknownFieldExceptions().size());
    assertEquals(
        "\"oidc.tls\" unrecognized field \"caFilee\". Expected fields are: "
            + "caFile, tlsCertificateKeyFile, tlsCertificateKeyFilePasswordFile",
        context.getUnknownFieldExceptions().get(0).getMessage());
  }

  /** The nested oidc tls block is validated: a password file requires a cert-key file. */
  @Test
  public void validate_oidcTlsPasswordFileWithoutCertKeyFile_throwsError()
      throws BsonParseException {
    TlsConfig tls =
        new TlsConfig(
            true,
            Optional.empty(),
            Optional.of(Path.of("/etc/tls/client.pw")),
            Optional.of(Path.of("/ca.pem")));
    ReplicaSetConfig cfg = config(Optional.empty(), Optional.empty(), Optional.of(oidc(tls)));

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals(
          "tlsCertificateKeyFile is required when tlsCertificateKeyFilePasswordFile is provided",
          e.getMessage());
    }
  }

  @Test
  public void testValidate_bothHostAndPortsAndSrvHost_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        new ReplicaSetConfig(
            HOSTS,
            Optional.of("srv.example.com"),
            Optional.of(x509(Path.of("/etc/tls/client.pem"))),
            Optional.empty(),
            Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals("HostAndPorts and SrvHost are mutually exclusive", e.getMessage());
    }
  }

  @Test
  public void testValidate_neitherHostAndPortsNorSrvHost_ThrowsError() throws BsonParseException {
    ReplicaSetConfig cfg =
        new ReplicaSetConfig(
            List.of(),
            Optional.empty(),
            Optional.of(x509(Path.of("/etc/tls/client.pem"))),
            Optional.empty(),
            Optional.empty());

    try (var parser = parser(new BsonDocument())) {
      BsonParseException e = assertThrows(BsonParseException.class, () -> cfg.validate(parser));
      assertEquals("HostAndPorts or SrvHost must be specified", e.getMessage());
    }
  }

  @Test
  public void testValidate_srvHostOnly_Passes() throws BsonParseException {
    ReplicaSetConfig cfg =
        srvConfig(
            Optional.of(x509(Path.of("/etc/tls/client.pem"))), Optional.empty(), Optional.empty());

    BsonDocument encoded = cfg.toBson();

    try (var parser = parser(encoded)) {
      ReplicaSetConfig roundTripped = ReplicaSetConfig.fromBson(parser);
      assertEquals(cfg, roundTripped);
      roundTripped.validate(parser);
    }
  }
}
