package com.xgen.mongot.util.mongodb;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.SecretsParser;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/**
 * A class to assist with construction of SSLContexts.
 *
 * <p>Currently, services two disjoint use cases: Mongot Community, and OpenSSL dynamic linking.
 */
public class SslContextFactory {

  /**
   * Creates an SSLContext from a CA file containing multiple certificates.
   *
   * @param caFilePath Path to the CA file containing multiple certificates.
   * @return An initialized SSLContext.
   */
  public static SSLContext getWithCaFile(Path caFilePath) {
    try {
      TrustManagerFactory tmf = createTrustManagerFromCaFile(caFilePath);

      SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
      sslContext.init(null, tmf.getTrustManagers(), null);

      return sslContext;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SSL context from CA file", e);
    }
  }

  /**
   * Creates a TLS 1.3 {@link SSLContext} for mutual TLS (x509 client authentication).
   *
   * <p>Configures a key manager from the client certificate/key file, and optionally a trust
   * manager from the CA file. If {@code caFilePath} is empty, the JVM default trust store is used
   * to verify the server.
   *
   * @param caFilePath optional path to the CA file used to verify the server; absent means JVM
   *     default trust store
   * @param certKeyFilePath path to the combined PEM file (client private key and certificate(s))
   * @param certKeyFilePasswordPath optional path to the file containing the key passphrase
   * @return an initialized SSLContext for mTLS
   */
  public static SSLContext getWithCertKeyFile(
      Optional<Path> caFilePath, Path certKeyFilePath, Optional<Path> certKeyFilePasswordPath) {
    try {
      KeyManagerFactory kmf = createKmfFromCombinedPem(certKeyFilePath, certKeyFilePasswordPath);

      SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
      if (caFilePath.isPresent()) {
        TrustManagerFactory tmf = createTrustManagerFromCaFile(caFilePath.get());
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
      } else {
        sslContext.init(kmf.getKeyManagers(), null, new SecureRandom());
      }

      return sslContext;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds a {@link TrustManagerFactory} that trusts the X.509 certificates in the given CA file.
   *
   * <p>Reads all certificates from the file (e.g. a CA bundle), loads them into a key store, and
   * initializes the trust manager. Used to verify the peer (e.g. MongoDB server) during TLS
   * handshake.
   *
   * @param caFilePath path to a file containing one or more X.509 CA certificates
   * @return an initialized TrustManagerFactory
   */
  private static TrustManagerFactory createTrustManagerFromCaFile(Path caFilePath)
      throws Exception {
    Collection<? extends Certificate> caCertificates;
    try (InputStream caInput = new FileInputStream(caFilePath.toFile())) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      caCertificates = cf.generateCertificates(caInput);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read certificates from CA file: " + caFilePath, e);
    }

    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    @Var int certIndex = 1;
    for (var cert : caCertificates) {
      trustStore.setCertificateEntry("cert-" + certIndex, cert);
      certIndex++;
    }

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    return tmf;
  }

  /**
   * Builds a {@link KeyManagerFactory} from a combined PEM file for use with Netty's {@link
   * SslContextBuilder#forServer(KeyManagerFactory)}.
   *
   * <p>Supports unencrypted and password-protected PEM key pairs (traditional OpenSSL and PKCS8
   * formats). When the key is encrypted, {@code certKeyFilePasswordPath} must be present.
   *
   * @param certKeyFilePath path to the combined PEM file (private key and certificate(s))
   * @param certKeyFilePasswordPath optional path to a file containing the key passphrase
   * @return an initialized KeyManagerFactory
   */
  public static KeyManagerFactory buildKeyManagerFactory(
      Path certKeyFilePath, Optional<Path> certKeyFilePasswordPath) throws Exception {
    return createKmfFromCombinedPem(certKeyFilePath, certKeyFilePasswordPath);
  }

  /**
   * Builds a {@link KeyManagerFactory} from a combined PEM file containing a private key and X.509
   * certificate(s).
   *
   * <p>Supports PEM-encrypted key pairs and PKCS8-encrypted private key info. If the key is
   * encrypted, {@code certKeyFilePasswordPath} must be present.
   *
   * @param certKeyFilePath path to the PEM file (private key and certificate(s))
   * @param certKeyFilePasswordPath optional path to a file containing the key passphrase
   * @return an initialized KeyManagerFactory for use with TLS client authentication
   */
  private static KeyManagerFactory createKmfFromCombinedPem(
      Path certKeyFilePath, Optional<Path> certKeyFilePasswordPath) throws Exception {
    char[] password =
        certKeyFilePasswordPath
            .map(
                path ->
                    Crash.because("failed to read tls certificate key file password")
                        .ifThrows(() -> SecretsParser.readSecretFile(path)))
            .orElse("")
            .toCharArray();

    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);

    @Var Optional<PrivateKey> privateKey = Optional.empty();
    List<X509Certificate> certList = new ArrayList<>();

    JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter();
    JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter();

    try (PEMParser parser = new PEMParser(new FileReader(certKeyFilePath.toFile()))) {
      @Var Object obj;
      while ((obj = parser.readObject()) != null) {
        if (obj instanceof PEMEncryptedKeyPair encryptedKeyPair) {
          if (password.length == 0) {
            throw new IllegalArgumentException(
                "The certificate key-file at "
                    + certKeyFilePath
                    + " is password-protected, a key-file-password is required");
          }
          PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(password);
          PEMKeyPair keyPair = encryptedKeyPair.decryptKeyPair(decProv);
          privateKey = Optional.of(keyConverter.getKeyPair(keyPair).getPrivate());
        } else if (obj instanceof PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo) {
          if (password.length == 0) {
            throw new IllegalArgumentException(
                "The certificate key-file at "
                    + certKeyFilePath
                    + " is password-protected, a key-file-password is required");
          }
          InputDecryptorProvider decProv =
              new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password);
          PrivateKeyInfo keyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(decProv);
          privateKey = Optional.of(keyConverter.getPrivateKey(keyInfo));
        } else if (obj instanceof PrivateKeyInfo privateKeyInfo) {
          privateKey = Optional.of(keyConverter.getPrivateKey(privateKeyInfo));
        } else if (obj instanceof PEMKeyPair keyPair) {
          privateKey = Optional.of(keyConverter.getKeyPair(keyPair).getPrivate());
        } else if (obj instanceof X509CertificateHolder certificateHolder) {
          certList.add(certConverter.getCertificate(certificateHolder));
        }
      }
    }

    if (privateKey.isEmpty() || certList.isEmpty()) {
      throw new IllegalArgumentException(
          "Combined PEM must contain both a private key and an X.509 certificate");
    }

    Certificate[] certChain = certList.toArray(new Certificate[0]);
    keyStore.setKeyEntry("mongo-client", privateKey.get(), password, certChain);

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, password);
    return kmf;
  }

  SslContext get() throws SslDynamicLinkingException {
    try {
      // try to dynamically link to system version of OpenSSL to provide better throughput to
      // mongod.
      return SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).build();
    } catch (UnsatisfiedLinkError | SSLException e) {
      // OpenSSL provider may fail due to a linking problem. While this is not expected in
      // production, we failover to slower implementation if it does occur and will monitor
      // via log ingestion.
      throw new SslDynamicLinkingException(e);
    }
  }
}
