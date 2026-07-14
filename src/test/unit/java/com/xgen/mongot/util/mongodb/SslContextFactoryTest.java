package com.xgen.mongot.util.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SslContextFactoryTest {

  private static Path caFile;
  private static Path clientCombinedFile;
  private static Path certOnlyFile;

  @BeforeClass
  public static void generateTestCerts() throws Exception {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048, new SecureRandom());

    KeyPair caKeyPair = kpg.generateKeyPair();
    X509Certificate caCert = buildSelfSignedCert(caKeyPair, "CN=Test CA");

    caFile = Files.createTempFile("ssl-ctx-test-ca", ".pem");
    Files.writeString(caFile, toPemCert(caCert));

    KeyPair clientKeyPair = kpg.generateKeyPair();
    X509Certificate clientCert =
        buildSignedCert(clientKeyPair, "CN=Test Client", caKeyPair, caCert);

    String combinedPem = toPemPrivateKey(clientKeyPair) + toPemCert(clientCert);
    clientCombinedFile = Files.createTempFile("ssl-ctx-test-client", ".pem");
    Files.writeString(clientCombinedFile, combinedPem);

    certOnlyFile = Files.createTempFile("ssl-ctx-test-cert-only", ".pem");
    Files.writeString(certOnlyFile, toPemCert(caCert));
  }

  @AfterClass
  public static void deleteTestCerts() throws Exception {
    if (caFile != null) {
      Files.deleteIfExists(caFile);
    }
    if (clientCombinedFile != null) {
      Files.deleteIfExists(clientCombinedFile);
    }
    if (certOnlyFile != null) {
      Files.deleteIfExists(certOnlyFile);
    }
  }

  // --- withCaFile ---

  @Test
  public void withCaFile_validCa_returnsSslContextFactory() {
    SslContextFactory factory = SslContextFactory.withCaFile(caFile);
    assertThat(factory).isNotNull();
  }

  @Test
  public void withCaFile_nonexistentFile_throwsRuntimeException() {
    Path missing = Path.of("/nonexistent/path/ca.pem");
    RuntimeException e =
        assertThrows(RuntimeException.class, () -> SslContextFactory.withCaFile(missing));
    assertThat(e).hasMessageThat().contains("Failed to build SslContextFactory from CA file");
  }

  // --- withCertKeyFile ---

  @Test
  public void withCertKeyFile_withCa_returnsSslContextFactory() {
    SslContextFactory factory =
        SslContextFactory.withCertKeyFile(
            Optional.of(caFile), clientCombinedFile, Optional.empty());
    assertThat(factory).isNotNull();
  }

  @Test
  public void withCertKeyFile_withoutCa_returnsSslContextFactory() {
    SslContextFactory factory =
        SslContextFactory.withCertKeyFile(Optional.empty(), clientCombinedFile, Optional.empty());
    assertThat(factory).isNotNull();
  }

  @Test
  public void withCertKeyFile_nonexistentCertFile_throwsRuntimeException() {
    Path missing = Path.of("/nonexistent/path/client.pem");
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> SslContextFactory.withCertKeyFile(Optional.empty(), missing, Optional.empty()));
    assertThat(e).hasMessageThat().contains("Failed to build SslContextFactory from cert/key file");
  }

  @Test
  public void withCertKeyFile_certOnlyFile_throwsRuntimeException() {
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                SslContextFactory.withCertKeyFile(
                    Optional.empty(), certOnlyFile, Optional.empty()));
    assertThat(e).hasMessageThat().contains("Failed to build SslContextFactory from cert/key file");
  }

  // --- getJavaContext ---

  @Test
  public void getJavaContext_fromWithCaFile_returnsTls13SslContext() {
    SSLContext ctx = SslContextFactory.withCaFile(caFile).getJavaContext();
    assertThat(ctx.getProtocol()).isEqualTo("TLSv1.3");
  }

  @Test
  public void getJavaContext_fromWithCertKeyFile_returnsTls13SslContext() {
    SSLContext ctx =
        SslContextFactory.withCertKeyFile(Optional.of(caFile), clientCombinedFile, Optional.empty())
            .getJavaContext();
    assertThat(ctx.getProtocol()).isEqualTo("TLSv1.3");
  }

  @Test
  public void getJavaContext_emptyFactory_returnsTls13SslContext() {
    SSLContext ctx = new SslContextFactory().getJavaContext();
    assertThat(ctx.getProtocol()).isEqualTo("TLSv1.3");
  }

  // --- PEM helpers ---

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
}
