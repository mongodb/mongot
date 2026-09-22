package com.xgen.mongot.util.security;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.junit.Test;

public class PemCertificateBundleTest {

  private static final String PEM =
      """
      -----BEGIN CERTIFICATE-----
      MIIDBzCCAe+gAwIBAgIUCRVLamkxy9r8+fSmECQLgv1erhowDQYJKoZIhvcNAQEL
      BQAwEzERMA8GA1UEAwwIcGVtLXRlc3QwHhcNMjYwOTIyMTUyMjQzWhcNMzYwOTE5
      MTUyMjQzWjATMREwDwYDVQQDDAhwZW0tdGVzdDCCASIwDQYJKoZIhvcNAQEBBQAD
      ggEPADCCAQoCggEBAMJM/FuZf3gt+wyHK4Qr1/js763xu0kL9VADSgp7QPUSmF/l
      mqsJj8SjBxsKMFWpI1mjUEzY3TPPheajxCnV/nKACCPecy4wFREifgsiz4AFyjma
      mh7KuY6tZ7+lcbhqqgCeuyvk1IpH5bMq18ncgykanLEvNjfFM5IZoZj9CvAWQCMA
      SbcheWxSHFTs4HruI6sVj325nu4ar771jpFapRmSSs8E/mQwHN5cwwWmkaRhM/Gl
      ixQ9onrQDjmEPUSGWzPVLUkWkKCg97q32Avw9eQ0lp1fs/34vojc9/FoUHnnZH2P
      NQsAOM4BV6tzcWSentaESKcpqaRjjvbiSzrpq/UCAwEAAaNTMFEwHQYDVR0OBBYE
      FLA5EFrURKoSxYOTxp/AWXi4Li7uMB8GA1UdIwQYMBaAFLA5EFrURKoSxYOTxp/A
      WXi4Li7uMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAFro5Mov
      MHhKRFu1OtNedfL3S/QT0rtgbN3drO6KzSsExcjXphHNF6u/BUzQcaqC+vZthTdf
      Zinjdmxz9d09FOV1el574owVnkRJL4/CZcOZz161Qz6se2uzJpW1XdhJdvcTXnVA
      wENdgwkqwiiYRD+qxYzYsLPYhy5Js5GPaJbTDZccQa4BFQf7rNBDiK5jywJ9wVGB
      sJNCZcf7RDpd7hz9VYcQzYgd+2GqrBJeM/Ew0iX4plIyOIwwjZm3HrlPaM1+pZgF
      1GiX3Ha/N0Hk0KzZoYI+IQugsGyModiTAXQTpn6OS8YIRf98AHUNOUUpUAVlQx9l
      2Dh+SxcKb6CM7k0=
      -----END CERTIFICATE-----
      """;

  @Test
  public void readPem_validPem_returnsBundle() {
    byte[] bytes = PEM.getBytes(StandardCharsets.UTF_8);

    PemCertificateBundle bundle = PemCertificateBundle.readPem(bytes);

    assertThat(bundle.certificates()).hasSize(1);
    assertThat(bundle.pemBytes()).isEqualTo(bytes);
  }

  @Test
  public void readPem_defensivelyCopiesBytes() {
    byte[] input = PEM.getBytes(StandardCharsets.UTF_8);
    PemCertificateBundle bundle = PemCertificateBundle.readPem(input);
    byte[] expected = input.clone();

    // Mutating the caller's input array must not change the stored bytes.
    Arrays.fill(input, (byte) 0);
    assertThat(bundle.pemBytes()).isEqualTo(expected);

    // Mutating the returned array must not change the stored bytes either.
    Arrays.fill(bundle.pemBytes(), (byte) 0);
    assertThat(bundle.pemBytes()).isEqualTo(expected);
  }

  @Test
  public void readPem_der_isRejected() {
    assertThrows(
        PemCertificateBundle.PemParseException.class,
        () -> PemCertificateBundle.readPem(derFromPem()));
  }

  @Test
  public void readPem_pemWithSurroundingText_isRejected() {
    byte[] bytes = ("stray header\n" + PEM + "stray footer\n").getBytes(StandardCharsets.UTF_8);

    assertThrows(
        PemCertificateBundle.PemParseException.class, () -> PemCertificateBundle.readPem(bytes));
  }

  @Test
  public void readPem_emptyInput_isRejected() {
    assertThrows(
        PemCertificateBundle.PemParseException.class,
        () -> PemCertificateBundle.readPem(new byte[0]));
  }

  @Test
  public void readPem_malformedBlock_isRejected() {
    byte[] bytes =
        "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n"
            .getBytes(StandardCharsets.UTF_8);

    assertThrows(
        PemCertificateBundle.PemParseException.class, () -> PemCertificateBundle.readPem(bytes));
  }

  @Test
  public void readPem_multipleCertificates_returnsAll() {
    byte[] bytes = (PEM + PEM).getBytes(StandardCharsets.UTF_8);

    PemCertificateBundle bundle = PemCertificateBundle.readPem(bytes);

    assertThat(bundle.certificates()).hasSize(2);
  }

  private static byte[] derFromPem() {
    String base64 = PEM.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
    return Base64.getDecoder().decode(base64);
  }
}
