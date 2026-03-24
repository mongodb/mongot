package com.xgen.mongot.embedding.providers.config;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider.VOYAGE;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ErrorHandlingConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.TruncationOption;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.WorkloadParams;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      EmbeddingServiceConfigTest.DeserializationTest.class,
      EmbeddingServiceConfigTest.SerializationTest.class
    })
public class EmbeddingServiceConfigTest {
  static final ModelConfig MODEL_CONFIG =
      new VoyageModelConfig(
          Optional.of(512),
          Optional.of(TruncationOption.START),
          Optional.of(100),
          Optional.of(1000));
  static final ErrorHandlingConfig ERROR_HANDLING_CONFIG =
      new ErrorHandlingConfig(50, 50L, 10L, 0.1);
  static final EmbeddingServiceConfig.EmbeddingCredentials CREDENTIALS =
      new VoyageEmbeddingCredentials("token123", "2024-10-15T22:32:20.925Z");

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "embedding-service-config-deserialization";
    private static final BsonDeserializationTestSuite<EmbeddingServiceConfig> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/embedding/providers/config",
            SUITE_NAME,
            EmbeddingServiceConfig::fromBson);

    private final TestSpecWrapper<EmbeddingServiceConfig> testSpec;

    public DeserializationTest(TestSpecWrapper<EmbeddingServiceConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<EmbeddingServiceConfig>> data() {
      return TEST_SUITE.withExamples(
          fullConfig(),
          configsWithOptionalFields(),
          configWithDedicatedClusterFalse(),
          configWithUseFlexTierFalse());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddingServiceConfig> fullConfig() {
      // Create tenant workload credentials for the map
      Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> tenantCredsMap =
          new HashMap<>();
      tenantCredsMap.put(
          "tenant-1",
          new EmbeddingServiceConfig.TenantWorkloadCredentials(
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-query-token", "2024-10-15T22:32:20.925Z")),
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-changestream-token", "2024-10-15T22:32:20.925Z")),
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-collectionscan-token", "2024-10-15T22:32:20.925Z"))));
      // This shouldn't happen in practice, but test partial credentials
      tenantCredsMap.put(
          "tenant-2",
          new EmbeddingServiceConfig.TenantWorkloadCredentials(
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant2-query-token", "2024-10-15T22:32:20.925Z")),
              Optional.empty(),
              Optional.empty()));

      return BsonDeserializationTestSuite.TestSpec.valid(
          "voyage-3-large full config",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3-large",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.of("us-east-1"),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.of(
                              new VoyageEmbeddingCredentials(
                                  "tenant-query-token", "2024-10-15T22:32:20.925Z")))),
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.empty())),
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.empty())),
                  Optional.of(tenantCredsMap),
                  true,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddingServiceConfig>
        configsWithOptionalFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "voyage-3.5-lite with some optional fields",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5-lite",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  true,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddingServiceConfig>
        configWithDedicatedClusterFalse() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "voyage-3.5 with isDedicatedCluster false",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  false,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddingServiceConfig>
        configWithUseFlexTierFalse() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "voyage-3.5 with useFlexTier false",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  true,
                  Optional.empty(),
                  false,
                  Optional.empty())));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "embedding-service-config-serialization";
    private static final BsonSerializationTestSuite<EmbeddingServiceConfig> TEST_SUITE =
        fromEncodable("src/test/unit/resources/embedding/providers/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig>> data() {
      return List.of(
          sanitizedConfig(),
          fullConfig(),
          defaultConfig(),
          configWithDedicatedClusterFalse(),
          configWithUseFlexTierFalse());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig> sanitizedConfig() {
      // Create tenant workload credentials for the map
      Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> tenantCredsMap =
          new HashMap<>();
      tenantCredsMap.put(
          "tenant-1",
          new EmbeddingServiceConfig.TenantWorkloadCredentials(
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-query-token", "2024-10-15T22:32:20.925Z")),
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-changestream-token", "2024-10-15T22:32:20.925Z")),
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant1-collectionscan-token", "2024-10-15T22:32:20.925Z"))));
      tenantCredsMap.put(
          "tenant-2",
          new EmbeddingServiceConfig.TenantWorkloadCredentials(
              Optional.of(
                  new VoyageEmbeddingCredentials(
                      "tenant2-query-token", "2024-10-15T22:32:20.925Z")),
              Optional.empty(),
              Optional.empty()));

      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3-large sanitized secret",
          new EmbeddingServiceConfig(
                  VOYAGE,
                  "voyage-3-large",
                  EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
                  new EmbeddingConfig(
                      Optional.of("us-east-1"),
                      MODEL_CONFIG,
                      ERROR_HANDLING_CONFIG,
                      CREDENTIALS,
                      Optional.of(
                          new WorkloadParams(
                              Optional.of(MODEL_CONFIG),
                              Optional.of(ERROR_HANDLING_CONFIG),
                              Optional.of(CREDENTIALS),
                              Optional.of(
                                  new VoyageEmbeddingCredentials(
                                      "tenant-query-token", "2024-10-15T22:32:20.925Z")))),
                      Optional.of(
                          new WorkloadParams(
                              Optional.of(MODEL_CONFIG),
                              Optional.of(ERROR_HANDLING_CONFIG),
                              Optional.of(CREDENTIALS),
                              Optional.empty())),
                      Optional.of(
                          new WorkloadParams(
                              Optional.of(MODEL_CONFIG),
                              Optional.of(ERROR_HANDLING_CONFIG),
                              Optional.of(CREDENTIALS),
                              Optional.empty())),
                      Optional.of(tenantCredsMap),
                      true,
                      Optional.empty(),
                      true,
                      Optional.empty()))
              .copySanitized("xxx-sanitized-xxx"));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig> fullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3.5-lite full config",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5-lite",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.empty())),
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.empty())),
                  Optional.of(
                      new WorkloadParams(
                          Optional.of(MODEL_CONFIG),
                          Optional.of(ERROR_HANDLING_CONFIG),
                          Optional.of(CREDENTIALS),
                          Optional.empty())),
                  Optional.empty(),
                  true,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig> defaultConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3.5 default config",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  true,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig>
        configWithDedicatedClusterFalse() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3.5 with isDedicatedCluster false",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  false,
                  Optional.empty(),
                  true,
                  Optional.empty())));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddingServiceConfig>
        configWithUseFlexTierFalse() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3.5 with useFlexTier false",
          new EmbeddingServiceConfig(
              VOYAGE,
              "voyage-3.5",
              EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
              new EmbeddingConfig(
                  Optional.empty(),
                  MODEL_CONFIG,
                  ERROR_HANDLING_CONFIG,
                  CREDENTIALS,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  true,
                  Optional.empty(),
                  false,
                  Optional.empty())));
    }
  }
}
