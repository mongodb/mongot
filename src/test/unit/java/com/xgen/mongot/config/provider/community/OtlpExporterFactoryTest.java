package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OtlpExporterFactoryTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void validateCaCertificateFile_missingOrMalformed_returnsError() throws Exception {
    assertThat(OtlpExporterFactory.validateCaCertificateFile(Path.of("/definitely/not/a/ca.pem")))
        .isPresent();

    File malformed = this.temporaryFolder.newFile("malformed.pem");
    Files.asCharSink(malformed, StandardCharsets.UTF_8)
        .write("-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n");
    assertThat(OtlpExporterFactory.validateCaCertificateFile(malformed.toPath())).isPresent();
  }
}
