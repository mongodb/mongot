package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.config.provider.community.parser.TokenEndpointField;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * OIDC configuration for an external identity provider using the client-credentials flow ({@code
 * environment: custom}).
 */
public record CustomOidcConfig(
    URI tokenEndpoint,
    String clientId,
    Path clientSecretFile,
    Path idpCaFile,
    Optional<String> scope,
    TlsConfig tls)
    implements OidcConfig {

  @Override
  public Environment environment() {
    return Environment.CUSTOM;
  }

  public static CustomOidcConfig fromBson(DocumentParser parser, TlsConfig tls)
      throws BsonParseException {
    return new CustomOidcConfig(
        parser.getField(Fields.TOKEN_ENDPOINT).unwrap(),
        parser.getField(Fields.CLIENT_ID).unwrap(),
        parser.getField(Fields.CLIENT_SECRET_FILE).unwrap(),
        parser.getField(Fields.IDP_CA_FILE).unwrap(),
        parser.getField(Fields.SCOPE).unwrap(),
        tls);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(OidcConfig.Fields.ENVIRONMENT, this.environment())
        .field(OidcConfig.Fields.TLS, this.tls)
        .field(Fields.TOKEN_ENDPOINT, this.tokenEndpoint)
        .field(Fields.CLIENT_ID, this.clientId)
        .field(Fields.CLIENT_SECRET_FILE, this.clientSecretFile)
        .field(Fields.IDP_CA_FILE, this.idpCaFile)
        .field(Fields.SCOPE, this.scope)
        .build();
  }

  static class Fields {
    static final Field.Required<URI> TOKEN_ENDPOINT =
        Field.builder("tokenEndpoint")
            .classField(TokenEndpointField.PARSER, TokenEndpointField.ENCODER)
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();

    static final Field.Required<String> CLIENT_ID =
        Field.builder("clientId")
            .stringField()
            .mustNotBeEmpty()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();

    static final Field.Required<Path> CLIENT_SECRET_FILE =
        Field.builder("clientSecretFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();

    /**
     * CA bundle that verifies the IdP token endpoint's TLS certificate for the token-acquisition
     * callback — the {@code mongot}→IdP leg, distinct from {@code tls.caFile}, which verifies the
     * {@code mongot}→{@code mongod} leg.
     */
    static final Field.Required<Path> IDP_CA_FILE =
        Field.builder("idpCaFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .edition(DeploymentEnvironment.ENTERPRISE)
            .required();

    static final Field.Optional<String> SCOPE =
        Field.builder("scope")
            .stringField()
            .mustNotBeEmpty()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .optional()
            .noDefault();
  }
}
