package com.xgen.mongot.config.provider.community;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common interface for OIDC authentication configuration. The {@code environment} field
 * discriminates which implementation a given {@code oidc} block parses into; currently only {@link
 * CustomOidcConfig}, for an external identity provider.
 */
public sealed interface OidcConfig extends DocumentEncodable permits CustomOidcConfig {

  Logger LOG = LoggerFactory.getLogger(OidcConfig.class);

  enum Environment {
    CUSTOM
  }

  Environment environment();

  TlsConfig tls();

  /**
   * Validates this OIDC config's TLS fields. Every environment shares the same TLS validation
   * rules: TLS is always enabled for {@code oidc} ({@code enabled} is not configurable, as with
   * x509) and a CA file must be set, with no fallback to the JVM default trust store (threat model
   * #1; mirrors the x509 required-CA rule) — plus the shared {@link TlsConfig} cross-field rules.
   */
  default void validate(DocumentParser parser) throws BsonParseException {
    if (this.tls().caFile().isEmpty()) {
      parser.getContext().handleSemanticError("caFile must be set within the oidc config");
    }
    this.tls().validate(parser);
  }

  class Fields {
    public static final Field.Required<Environment> ENVIRONMENT =
        Field.builder("environment")
            .enumField(Environment.class)
            .asCaseInsensitive()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();

    public static final Field.Required<TlsConfig> TLS =
        Field.builder("tls")
            .classField(OidcConfig::tlsFromBson, OidcConfig::tlsToBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();
  }

  private static TlsConfig tlsFromBson(DocumentParser parser) throws BsonParseException {
    return new TlsConfig(
        true,
        parser.getField(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE).unwrap(),
        parser.getField(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE).unwrap(),
        parser.getField(TlsConfig.Fields.TLS_CA_FILE).unwrap());
  }

  private static BsonDocument tlsToBson(TlsConfig tls) {
    return BsonDocumentBuilder.builder()
        .field(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE, tls.tlsCertificateKeyFile())
        .field(
            TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
            tls.tlsCertificateKeyFilePasswordFile())
        .field(TlsConfig.Fields.TLS_CA_FILE, tls.caFile())
        .build();
  }

  static OidcConfig fromBson(DocumentParser parser) throws BsonParseException {
    Environment environment = parser.getField(Fields.ENVIRONMENT).unwrap();
    TlsConfig tls = parser.getField(Fields.TLS).unwrap();
    return switch (environment) {
      case CUSTOM -> CustomOidcConfig.fromBson(parser, tls);
    };
  }

  /**
   * Fails startup when a {@code syncSource.replicaSet.oidc} or {@code syncSource.router.oidc} block
   * (or its {@code tls} block) contains an unknown field. The main parse is permissive and only
   * warns on unknown keys, which would silently drop a misspelled {@code tlsCertificateKeyFile}.
   * Only unknown-field errors are raised here; any other error is left to the main parse. A block
   * that fails to parse in this pre-pass is skipped, because an aborted parse would report the
   * not-yet-read valid keys as unknown; the main parse reports the real error on the same config. A
   * Community config whose oidc block has an unknown key reports the unknown-field error before the
   * edition error.
   */
  static void validateStrict(BsonDocument root) throws BsonParseException {
    BsonValue syncSource = root.get("syncSource");
    if (syncSource == null || !syncSource.isDocument()) {
      return;
    }
    for (String connection : List.of("replicaSet", "router")) {
      BsonValue connectionValue = syncSource.asDocument().get(connection);
      if (connectionValue == null || !connectionValue.isDocument()) {
        continue;
      }
      BsonValue oidc = connectionValue.asDocument().get("oidc");
      if (oidc == null || !oidc.isDocument()) {
        continue;
      }
      // Enterprise context so edition checks do not fire; the real edition is enforced later.
      PermissiveBsonParseContext context =
          PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.ENTERPRISE))
              .child("syncSource")
              .child(connection)
              .child("oidc");
      @Var boolean parsed = false;
      try (BsonDocumentParser parser =
          BsonDocumentParser.withContext(context, oidc.asDocument()).build()) {
        fromBson(parser);
        parsed = true;
      } catch (BsonParseException e) {
        // Reported by the main parse; unknown-field results are unreliable after an aborted parse.
        LOG.debug("oidc strict pre-parse failed; the main parse reports the error", e);
      }
      if (!parsed) {
        continue;
      }
      if (!context.getUnknownFieldExceptions().isEmpty()) {
        throw context.getUnknownFieldExceptions().get(0);
      }
    }
  }
}
