package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Supplier;
import com.xgen.mongot.config.provider.MongotConfigs;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.testing.TestUtils;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for CommunityMongotBootstrapper embedding initialization logic.
 *
 * <p>Tests the initializeEmbeddingService() and applyEndpointOverride() methods.
 */
public class CommunityMongotBootstrapperTest {

  private static final String CUSTOM_ENDPOINT = "https://custom-api.example.com/v1/embeddings";
  private static final String TEST_QUERY_KEY = "test-query-key";
  private static final String TEST_INDEXING_KEY = "test-indexing-key";

  private SimpleMeterRegistry meterRegistry;
  private MongotConfigs mongotConfigs;
  private Path queryKeyFile;
  private Path indexingKeyFile;

  @Before
  public void setUp() throws Exception {
    this.meterRegistry = new SimpleMeterRegistry();
    this.mongotConfigs = MongotConfigs.getDefault(Path.of("test-data"));

    // Create temporary credential files with proper permissions
    this.queryKeyFile = Files.createTempFile("voyage-api-query-key", ".txt");
    this.indexingKeyFile = Files.createTempFile("voyage-api-indexing-key", ".txt");

    Files.writeString(this.queryKeyFile, TEST_QUERY_KEY);
    Files.writeString(this.indexingKeyFile, TEST_INDEXING_KEY);

    // Set restrictive permissions (readable only by owner)
    try {
      Files.setPosixFilePermissions(
          this.queryKeyFile, PosixFilePermissions.fromString("r--------"));
      Files.setPosixFilePermissions(
          this.indexingKeyFile, PosixFilePermissions.fromString("r--------"));
    } catch (UnsupportedOperationException ignored) {
      // POSIX permissions not supported on this filesystem (e.g., Windows)
    }
  }

  @After
  public void tearDown() throws Exception {
    // Clean up temporary files
    if (this.queryKeyFile != null) {
      Files.deleteIfExists(this.queryKeyFile);
    }
    if (this.indexingKeyFile != null) {
      Files.deleteIfExists(this.indexingKeyFile);
    }
  }

  @Test
  public void initializeEmbeddingService_noCredentials_returnsNoOpSupplier() {
    // Record catalog size before initialization
    int catalogSizeBefore = EmbeddingModelCatalog.getAllSupportedModels().size();

    // Create config without embedding config
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.empty());

    Supplier<EmbeddingServiceManager> supplier =
        CommunityMongotBootstrapper.initializeEmbeddingService(
            config, this.mongotConfigs, this.meterRegistry, null);

    assertNotNull("Supplier should not be null", supplier);
    assertNotNull("Manager should not be null", supplier.get());

    // Verify no new models were added when credentials are not set
    assertEquals(
        "Model catalog should not add models when credentials are not set",
        catalogSizeBefore,
        EmbeddingModelCatalog.getAllSupportedModels().size());
  }

  @Test
  public void initializeEmbeddingService_withCredentials_populatesCatalog() {
    // Create config with credential files but no endpoint override
    EmbeddingConfig embeddingConfig =
        new EmbeddingConfig(
            Optional.empty(),
            Optional.of(this.queryKeyFile),
            Optional.of(this.indexingKeyFile),
            Optional.empty(),
            false);
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.of(embeddingConfig));

    Supplier<EmbeddingServiceManager> supplier =
        CommunityMongotBootstrapper.initializeEmbeddingService(
            config, this.mongotConfigs, this.meterRegistry, null);

    assertNotNull("Supplier should not be null", supplier);
    assertNotNull("Manager should not be null", supplier.get());

    // Verify the expected models are present (they may have been registered by other tests too)
    Set<String> supportedModels = EmbeddingModelCatalog.getAllSupportedModels();
    assertFalse("Model catalog should contain at least 1 model", supportedModels.isEmpty());

    // Verify the expected models are present
    assertTrue("Missing voyage-4-lite", supportedModels.contains("voyage-4-lite"));
    assertTrue("Missing voyage-4", supportedModels.contains("voyage-4"));
    assertTrue("Missing voyage-4-large", supportedModels.contains("voyage-4-large"));
    assertTrue("Missing voyage-code-3", supportedModels.contains("voyage-code-3"));

    // Confirm default endpoint from internal config for the expected models
    for (String modelName : supportedModels) {
      var modelConfig = EmbeddingModelCatalog.getModelConfig(modelName);
      assertNotNull("Model config should not be null for " + modelName, modelConfig);

      // All workloads should have empty provider endpoint (no override)
      assertEquals(
          "Query workload should not have endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.query().providerEndpoint());
      assertEquals(
          "Change stream workload should not have endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.changeStream().providerEndpoint());
      assertEquals(
          "Collection scan workload should not have endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.collectionScan().providerEndpoint());
    }
  }

  @Test
  public void initializeEmbeddingService_withEndpointOverride_appliesOverride() {
    // Create config with embedding endpoint override
    EmbeddingConfig embeddingConfig =
        new EmbeddingConfig(
            Optional.of(CUSTOM_ENDPOINT),
            Optional.of(this.queryKeyFile),
            Optional.of(this.indexingKeyFile),
            Optional.empty(),
            false);
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.of(embeddingConfig));

    Supplier<EmbeddingServiceManager> supplier =
        CommunityMongotBootstrapper.initializeEmbeddingService(
            config, this.mongotConfigs, this.meterRegistry, null);

    assertNotNull("Supplier should not be null", supplier);
    assertNotNull("Manager should not be null", supplier.get());

    // Verify the expected models are present (they may have been registered by other tests too)
    Set<String> supportedModels = EmbeddingModelCatalog.getAllSupportedModels();
    assertFalse("Model catalog should contain at least 1 model", supportedModels.isEmpty());
    Set<String> queryCompatibleModels = EmbeddingModelCatalog.getAllowedQueryModels();
    assertFalse(
        "Query compatible models should contain at least 1 after endpoint overrides",
        queryCompatibleModels.isEmpty());

    // Verify that the endpoint override was applied to the expected models
    for (String modelName : supportedModels) {
      var modelConfig = EmbeddingModelCatalog.getModelConfig(modelName);
      assertNotNull("Model config should not be null for " + modelName, modelConfig);

      // Verify all workloads have the custom endpoint
      assertEquals(
          "Query workload should have custom endpoint for " + modelName,
          CUSTOM_ENDPOINT,
          modelConfig.query().providerEndpoint().orElse(null));
      assertEquals(
          "Change stream workload should have custom endpoint for " + modelName,
          CUSTOM_ENDPOINT,
          modelConfig.changeStream().providerEndpoint().orElse(null));
      assertEquals(
          "Collection scan workload should have custom endpoint for " + modelName,
          CUSTOM_ENDPOINT,
          modelConfig.collectionScan().providerEndpoint().orElse(null));

      // Verify rpsPerProvider is preserved through endpoint override
      assertEquals(
          "rpsPerProvider should be empty (default) after endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.query().rpsPerProvider());
      assertEquals(
          "rpsPerProvider should be empty (default) after endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.changeStream().rpsPerProvider());
      assertEquals(
          "rpsPerProvider should be empty (default) after endpoint override for " + modelName,
          Optional.empty(),
          modelConfig.collectionScan().rpsPerProvider());
    }
  }

  @Test
  public void maybeCreateFtdcReporter_FtdcDisabled_DoesNotCreateFtdcReporter() throws IOException {
    CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
    SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
    Optional<CommunityMongotBootstrapper.MetricsLifecycles> metricsLifecycles =
        CommunityMongotBootstrapper.maybeCreateFtdcReporter(
            compositeRegistry,
            simpleRegistry,
            this.createMinimalConfigWithFtdc(
                TestUtils.getTempFolderPath().toString(),
                Optional.of(new FtdcCommunityConfig(false, 100, 10, 1000))),
            MongotConfigs.getDefault(TestUtils.getTempFolderPath()),
            "1.0.0");
    assertTrue(metricsLifecycles.isEmpty());
  }

  @Test
  public void maybeCreateFtdcReporter_FailureInitializing_Throws() throws IOException {
    TemporaryFolder folder = TestUtils.getTempFolder();
    try {
      folder.create();
      File file = folder.newFile();

      // Verify system crash when FTDC reporter can't create the diagnostic.data directory
      // because the path provided is a file and not a directory.
      CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
      SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
      TestUtils.assertThrows(
          "Test code should never call System.exit()",
          SecurityException.class,
          () ->
              CommunityMongotBootstrapper.maybeCreateFtdcReporter(
                  compositeRegistry,
                  simpleRegistry,
                  this.createMinimalConfigWithFtdc(
                      file.getPath(), Optional.of(new FtdcCommunityConfig(true, 100, 10, 1000))),
                  MongotConfigs.getDefault(Path.of(file.getPath())),
                  "1.0.0"));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void maybeCreateFtdcReporter_SuccessCase() throws Exception {
    TemporaryFolder folder = TestUtils.getTempFolder();
    Path folderPath = folder.newFolder().toPath();
    try {
      CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
      SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
      Optional<CommunityMongotBootstrapper.MetricsLifecycles> metricsLifecycles =
          CommunityMongotBootstrapper.maybeCreateFtdcReporter(
              compositeRegistry,
              simpleRegistry,
              this.createMinimalConfigWithFtdc(
                  folderPath.toString(), Optional.of(new FtdcCommunityConfig(true, 100, 10, 100))),
              MongotConfigs.getDefault(folderPath),
              "1.0.0");
      assertFalse(metricsLifecycles.isEmpty());

      metricsLifecycles.get().start().run();

      Thread.sleep(500); // Wait for a couple runs of the FTDC reporter

      Path start = folderPath.resolve("diagnostic.data");
      try (Stream<Path> stream = Files.walk(start)) {
        List<Path> results = stream.filter(Files::isRegularFile).toList();
        assertFalse(results.isEmpty());
      }
      metricsLifecycles.get().stop().run();
    } finally {
      folder.delete();
    }
  }

  private CommunityConfig createMinimalConfigWithEmbedding(
      Optional<EmbeddingConfig> embeddingConfig) {
    // Create minimal valid config for testing embeddings
    return new CommunityConfig(
        this.createMinimalSyncSourceConfig(),
        new StorageConfig(Path.of("test-data")),
        this.createMinimalServerConfig(),
        FtdcCommunityConfig.getDefault(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        embeddingConfig);
  }

  private CommunityConfig createMinimalConfigWithFtdc(
      String storagePath, Optional<FtdcCommunityConfig> ftdcConfig) {
    return new CommunityConfig(
        this.createMinimalSyncSourceConfig(),
        new StorageConfig(Path.of(storagePath)),
        this.createMinimalServerConfig(),
        ftdcConfig.orElse(FtdcCommunityConfig.getDefault()),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private SyncSourceConfig createMinimalSyncSourceConfig() {
    return new SyncSourceConfig(
        new ReplicaSetConfig(
            List.of(com.google.common.net.HostAndPort.fromParts("localhost", 27017)),
            Optional.of("user"),
            Optional.of(Path.of("/tmp/test.passwd")),
            Databases.ADMIN,
            false,
            MongoReadPreferenceName.PRIMARY,
            Optional.empty()),
        Optional.empty(),
        Optional.empty());
  }

  private ServerConfig createMinimalServerConfig() {
    return new ServerConfig(
        new ServerConfig.GrpcServerConfig("127.0.0.1:27028", Optional.empty()), Optional.empty());
  }
}
