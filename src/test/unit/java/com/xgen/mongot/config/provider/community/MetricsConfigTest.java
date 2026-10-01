package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/** Unit tests for the optional mTLS block of {@link MetricsConfig}. */
public class MetricsConfigTest {

  @Test
  public void tlsBlock_parsesFieldsAndEmitsCliOverrides() throws BsonParseException {
    BsonDocument tls =
        new BsonDocument()
            .append("certificateKeyFile", new BsonString("/etc/mongot/metrics-server.pem"))
            .append(
                "certificateKeyFilePasswordFile", new BsonString("/etc/mongot/metrics.pem.pass"))
            .append("caFile", new BsonString("/etc/mongot/prometheus-clients-ca.pem"));
    BsonDocument doc = new BsonDocument().append("enabled", BsonBoolean.TRUE).append("tls", tls);

    try (var parser =
        BsonDocumentParser.withContext(
                PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)), doc)
            .build()) {
      MetricsConfig.Tls parsed = MetricsConfig.fromBson(parser).tls().orElseThrow();
      assertEquals(Path.of("/etc/mongot/metrics-server.pem"), parsed.certificateKeyFile());
      assertEquals(
          Optional.of(Path.of("/etc/mongot/metrics.pem.pass")),
          parsed.certificateKeyFilePasswordFile());
      assertEquals(Path.of("/etc/mongot/prometheus-clients-ca.pem"), parsed.caFile());
    }

    MetricsConfig.MetricsConfigCliOptionRegistry registry =
        new MetricsConfig.MetricsConfigCliOptionRegistry();
    registry.tlsCliOptionRegistry.certificateKeyFile = Optional.of(Path.of("/etc/m.crt"));
    registry.tlsCliOptionRegistry.caFile = Optional.of(Path.of("/etc/ca.pem"));
    Map<String, BsonValue> overrides = registry.traverse();
    assertEquals(new BsonString("/etc/m.crt"), overrides.get("tls.certificateKeyFile"));
    assertEquals(new BsonString("/etc/ca.pem"), overrides.get("tls.caFile"));
  }

  @Test
  public void tlsMissingRequiredField_throws() {
    BsonDocument tls =
        new BsonDocument()
            .append("certificateKeyFile", new BsonString("/etc/mongot/metrics-server.pem"));
    BsonDocument doc = new BsonDocument().append("enabled", BsonBoolean.TRUE).append("tls", tls);

    assertThrows(
        BsonParseException.class,
        () -> {
          try (var parser =
              BsonDocumentParser.withContext(
                      PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY)),
                      doc)
                  .build()) {
            MetricsConfig.fromBson(parser);
          }
        });
  }
}
