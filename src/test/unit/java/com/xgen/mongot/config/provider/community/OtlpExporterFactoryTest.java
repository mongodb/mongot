package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.io.Files;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
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

  @Test
  public void createLogRecordExporter_validConfig_buildsAndShutsDown() {
    LogRecordExporter exporter =
        OtlpExporterFactory.createLogRecordExporter(
            new OtlpTracingConfig("https://collector.example.com:4317", Optional.empty()));

    assertThat(exporter).isInstanceOf(OtlpGrpcLogRecordExporter.class);
    exporter.shutdown();
  }

  @Test
  public void createLogRecordExporter_unreadableCa_throwsOtlpConfigException() {
    OtlpTracingConfig config =
        new OtlpTracingConfig(
            "https://collector.example.com:4317", Optional.of(Path.of("/not/a/real/ca.pem")));

    assertThrows(
        OtlpExporterFactory.OtlpConfigException.class,
        () -> OtlpExporterFactory.createLogRecordExporter(config));
  }
}
