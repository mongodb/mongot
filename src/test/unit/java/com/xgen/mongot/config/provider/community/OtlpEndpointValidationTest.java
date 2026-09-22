package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class OtlpEndpointValidationTest {

  @Parameters(name = "endpoint={0}, valid={1}")
  public static Collection<Object[]> endpoints() {
    return Arrays.asList(
        new Object[][] {
          {"https://collector.example.com:4317", true},
          {"https://127.0.0.1:1", true},
          {"https://[2001:db8::1]:4317", true},
          {"HTTPS://Host.Example.COM:4317", true},
          {"", false},
          {"collector.example.com:4317", false},
          {"http://host:4317", false},
          {"https://", false},
          {"https://:4317", false},
          {"https://host", false},
          {"https://host:0", false},
          {"https://host:65536", false},
          {"https://host:abc", false},
          {"https://host:4317/path", false},
          {"https://host:4317/", false},
          {"https://host:4317?x=1", false},
          {"https://host:4317#f", false},
          {"https://user:pass@host:4317", false},
          {" https://host:4317", false},
          {"https://host:4317 ", false},
          {"https://ho st:4317", false},
          {"https://2001:db8::1:4317", false},
          {"https://[2001:db8::1]", false},
        });
  }

  private final String endpoint;
  private final boolean expectedValid;

  public OtlpEndpointValidationTest(String endpoint, boolean expectedValid) {
    this.endpoint = endpoint;
    this.expectedValid = expectedValid;
  }

  @Test
  public void validate_returnsExpectedResult() {
    assertThat(OtlpExporterFactory.validateEndpoint(this.endpoint).isEmpty())
        .isEqualTo(this.expectedValid);
  }
}
