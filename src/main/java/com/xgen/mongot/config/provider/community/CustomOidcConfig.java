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
    Optional<String> audience,
    Optional<String> resource,
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
        parser.getField(Fields.AUDIENCE).unwrap(),
        parser.getField(Fields.RESOURCE).unwrap(),
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
        .field(Fields.AUDIENCE, this.audience)
        .field(Fields.RESOURCE, this.resource)
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

    /**
     * Sent as the {@code audience} form parameter on the token request. Auth0's client-credentials
     * flow only mints a JWT access token (carrying the {@code aud} claim {@code mongod} validates)
     * when the request asks for one.
     */
    static final Field.Optional<String> AUDIENCE =
        Field.builder("audience")
            .stringField()
            .mustNotBeEmpty()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .optional()
            .noDefault();

    /**
     * RFC 8707 resource indicator, sent as the {@code resource} form parameter on the token request
     * for IdPs that scope the token's {@code aud} claim to the target resource (Entra v1 style).
     */
    static final Field.Optional<String> RESOURCE =
        Field.builder("resource")
            .stringField()
            .mustNotBeEmpty()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .optional()
            .noDefault();
  }
}
