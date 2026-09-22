package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.config.provider.community.TracingConfig.SamplingStrategy;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.Test;

public class TracingConfigTest {

  private static TracingConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return TracingConfig.fromBson(parser);
    }
  }

  @Test
  public void parse_fullBlock_preservesSettingsAndRoundTrips() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("enabled", new BsonBoolean(true))
            .append("searchSamplingStrategy", new BsonString("head"))
            .append("vectorSamplingStrategy", new BsonString("tail"))
            .append("searchTracingSamplerRate", new BsonDouble(0.25))
            .append("vectorTracingSamplerRate", new BsonDouble(0.5));

    TracingConfig config = parse(doc);

    assertThat(config.enabled()).isTrue();
    assertThat(config.searchSamplingStrategy()).isEqualTo(SamplingStrategy.HEAD);
    assertThat(config.vectorSamplingStrategy()).isEqualTo(SamplingStrategy.TAIL);
    assertThat(config.searchTracingSamplerRate()).isEqualTo(0.25);
    assertThat(config.vectorTracingSamplerRate()).isEqualTo(0.5);
    assertThat(parse(config.toBson())).isEqualTo(config);
  }

  @Test
  public void parse_emptyBlock_usesDisabledDefaults() throws BsonParseException {
    TracingConfig config = parse(new BsonDocument());

    assertThat(config.enabled()).isFalse();
    assertThat(config.searchSamplingStrategy()).isEqualTo(SamplingStrategy.TAIL);
    assertThat(config.vectorSamplingStrategy()).isEqualTo(SamplingStrategy.HEAD);
    assertThat(config.searchTracingSamplerRate()).isEqualTo(0.01);
    assertThat(config.vectorTracingSamplerRate()).isEqualTo(0.01);
  }

  @Test
  public void parse_invalidValues_areRejected() {
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("searchTracingSamplerRate", new BsonDouble(1.5))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("vectorTracingSamplerRate", new BsonDouble(-0.1))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("searchSamplingStrategy", new BsonString("nonsense"))));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("vectorSamplingStrategy", new BsonString("nonsense"))));
  }

  @Test
  public void otlp_parsesRoundTripsAndRejectsUnsafeConfig() throws BsonParseException {
    TracingConfig config =
        parse(
            otlp(
                new BsonDocument(
                    "endpoint", new BsonString("https://collector.example.com:4317"))));
    assertThat(config.otlp().orElseThrow().endpoint())
        .isEqualTo("https://collector.example.com:4317");
    assertThat(parse(config.toBson())).isEqualTo(config);
    assertThrows(BsonParseException.class, () -> parse(otlp(new BsonDocument())));

    BsonDocument unknown =
        new BsonDocument("endpoint", new BsonString("https://host:4317"))
            .append("bogus", new BsonBoolean(true));
    assertThrows(
        BsonParseException.class, () -> TracingConfig.validateStrictOtlp(tracing(otlp(unknown))));

    BsonDocument badCa =
        new BsonDocument("endpoint", new BsonString("https://host:4317"))
            .append("caCertificateFile", new BsonString("/definitely/not/a/real/ca.pem"));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("enabled", new BsonBoolean(false)).append("otlp", badCa)));
  }

  private static BsonDocument otlp(BsonDocument otlp) {
    return new BsonDocument("otlp", otlp);
  }

  private static BsonDocument tracing(BsonDocument otlp) {
    return new BsonDocument("tracing", otlp);
  }

  @Test
  public void otlp_derCertificate_isRejectedEvenWhenTracingDisabled() throws Exception {
    Path der = Files.createTempFile("ca", ".der");
    Files.write(der, selfSignedDer());
    assertThat(OtlpExporterFactory.validateCaCertificateFile(der)).isPresent();

    BsonDocument badCa =
        new BsonDocument("endpoint", new BsonString("https://host:4317"))
            .append("caCertificateFile", new BsonString(der.toString()));
    assertThrows(
        BsonParseException.class,
        () -> parse(new BsonDocument("enabled", new BsonBoolean(false)).append("otlp", badCa)));
  }

  private static byte[] selfSignedDer() throws Exception {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048);
    KeyPair keyPair = kpg.generateKeyPair();
    X500Name name = new X500Name("CN=der-test");
    Instant now = Instant.now();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            name,
            BigInteger.ONE,
            Date.from(now),
            Date.from(now.plus(1, ChronoUnit.DAYS)),
            name,
            keyPair.getPublic());
    ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());
    return new JcaX509CertificateConverter().getCertificate(builder.build(signer)).getEncoded();
  }
}
