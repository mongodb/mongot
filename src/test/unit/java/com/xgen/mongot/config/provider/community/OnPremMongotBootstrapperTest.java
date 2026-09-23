package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Supplier;
import com.mongodb.MongoException;
import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.config.provider.MongotConfigs;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.trace.Slf4jExporter;
import com.xgen.mongot.trace.SpanGuard;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.testing.TestUtils;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for OnPremMongotBootstrapper embedding initialization logic.
 *
 * <p>Tests the initializeEmbeddingService() and applyEndpointOverride() methods.
 */
public class OnPremMongotBootstrapperTest {

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

    // Reset sampling strategies so tracing tests are order-independent.
    Tracing.updateStrategyConfig(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  public void initializeEmbeddingService_noCredentials_returnsNoOpSupplier() {
    // Record catalog size before initialization
    int catalogSizeBefore = EmbeddingModelCatalog.getAllSupportedModels().size();

    // Create config without embedding config
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.empty());

    Supplier<EmbeddingServiceManager> supplier =
        OnPremMongotBootstrapper.initializeEmbeddingService(
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
            false);
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.of(embeddingConfig));

    Supplier<EmbeddingServiceManager> supplier =
        OnPremMongotBootstrapper.initializeEmbeddingService(
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
            false);
    CommunityConfig config = this.createMinimalConfigWithEmbedding(Optional.of(embeddingConfig));

    Supplier<EmbeddingServiceManager> supplier =
        OnPremMongotBootstrapper.initializeEmbeddingService(
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
  public void failFastOnTopologyMismatch_passesWhenTopologyMatches() throws Exception {
    var guard = mock(CatalogAccessGuard.class);
    doNothing().when(guard).requireTopologyMatch();

    OnPremMongotBootstrapper.failFastOnTopologyMismatch(guard);
  }

  @Test
  @Crash.TestOnlyHaltHandler
  public void failFastOnTopologyMismatch_crashesWhenMismatched() throws Exception {
    var guard = mock(CatalogAccessGuard.class);
    doThrow(new TopologyMismatchException("mismatch for test")).when(guard).requireTopologyMatch();

    AtomicInteger exitCode = new AtomicInteger(-1);
    Crash.setHaltHandlerForTesting(
        code -> {
          exitCode.set(code);
          throw new HaltError();
        });

    try {
      assertThrows(
          HaltError.class, () -> OnPremMongotBootstrapper.failFastOnTopologyMismatch(guard));
      assertEquals("expected fail-exit code 1", 1, exitCode.get());
    } finally {
      Crash.clearHaltHandlerForTesting();
    }
  }

  @Test
  public void failFastOnTopologyMismatch_continuesWhenTopologyUndetermined() throws Exception {
    var guard = mock(CatalogAccessGuard.class);
    doThrow(new CheckedMongoException(new MongoException("topology cache is unavailable")))
        .when(guard)
        .requireTopologyMatch();

    OnPremMongotBootstrapper.failFastOnTopologyMismatch(guard);
  }

  @Test
  public void maybeCreateFtdcReporter_FtdcDisabled_DoesNotCreateFtdcReporter() throws IOException {
    CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
    SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
    Optional<OnPremMongotBootstrapper.MetricsLifecycles> metricsLifecycles =
        OnPremMongotBootstrapper.maybeCreateFtdcReporter(
            compositeRegistry,
            simpleRegistry,
            this.createMinimalConfigWithFtdc(
                TestUtils.getTempFolderPath().toString(),
                Optional.of(new FtdcCommunityConfig(false, 100, 10, 1000))),
            MongotConfigs.getDefault(TestUtils.getTempFolderPath()),
            "1.0.0",
            new ThreadPoolResourceMetrics.Bindings());
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
              OnPremMongotBootstrapper.maybeCreateFtdcReporter(
                  compositeRegistry,
                  simpleRegistry,
                  this.createMinimalConfigWithFtdc(
                      file.getPath(), Optional.of(new FtdcCommunityConfig(true, 100, 10, 1000))),
                  MongotConfigs.getDefault(Path.of(file.getPath())),
                  "1.0.0",
                  new ThreadPoolResourceMetrics.Bindings()));
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
      Optional<OnPremMongotBootstrapper.MetricsLifecycles> metricsLifecycles =
          OnPremMongotBootstrapper.maybeCreateFtdcReporter(
              compositeRegistry,
              simpleRegistry,
              this.createMinimalConfigWithFtdc(
                  folderPath.toString(), Optional.of(new FtdcCommunityConfig(true, 100, 10, 100))),
              MongotConfigs.getDefault(folderPath),
              "1.0.0",
              new ThreadPoolResourceMetrics.Bindings());
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
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        embeddingConfig,
        Optional.empty(),
        Optional.empty());
  }

  private CommunityConfig createMinimalConfigWithFtdc(
      String storagePath, Optional<FtdcCommunityConfig> ftdcConfig) {
    return new CommunityConfig(
        this.createMinimalSyncSourceConfig(),
        new StorageConfig(Path.of(storagePath)),
        this.createMinimalServerConfig(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(
            new AdvancedConfigs(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ftdcConfig)),
        Optional.empty());
  }

  private SyncSourceConfig createMinimalSyncSourceConfig() {
    return new SyncSourceConfig(
        new ReplicaSetConfig(
            List.of(com.google.common.net.HostAndPort.fromParts("localhost", 27017)),
            Optional.empty(),
            Optional.empty(),
            Optional.of(
                new ScramConfig(
                    Databases.ADMIN,
                    "user",
                    Path.of("/tmp/test.passwd"),
                    new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty())))),
        Optional.empty(),
        Optional.empty());
  }

  private ServerConfig createMinimalServerConfig() {
    return new ServerConfig(
        new ServerConfig.GrpcServerConfig("127.0.0.1:27028", Optional.empty()), Optional.empty());
  }

  @Test
  public void configureTracing_enabled_wiresAndEmitsOtlpJson() throws Exception {
    Logger exporterLogger = TestUtils.getClassLogger(Slf4jExporter.class);
    List<ILoggingEvent> events = TestUtils.getLogEvents(exporterLogger);

    // Drive the path from the actual tracing-enabled YAML file, not a hand-built config.
    Path configPath =
        Path.of("src/test/unit/resources/config/provider/community/communityConfigTracing.yaml");
    TracingConfig config =
        CommunityConfig.readFromFile(configPath).config().tracingConfig().orElseThrow();

    Optional<SpanProcessor> processor =
        OnPremMongotBootstrapper.configureTracing(Optional.of(config), this.meterRegistry);
    assertTrue(processor.isPresent());
    assertEquals(1.0, this.meterRegistry.get("tracing.enabled").gauge().value(), 0.0);

    SpanProcessor installed = processor.get();
    try {
      // The configured head strategy at rate 1.0 must apply to both search and vector spans.
      try (SpanGuard searchGuard = Tracing.searchSpanGuard("test.search")) {
        assertTrue(searchGuard.getSpan().isRecording());
      }
      try (SpanGuard vectorGuard = Tracing.vectorSearchSpanGuard("test.vectorSearch")) {
        assertTrue(vectorGuard.getSpan().isRecording());
      }
    } finally {
      // The shutdown helper must flush the buffered traces before stopping the processor; if the
      // order were reversed, shutdown would clear the buffer and no log would be emitted.
      OnPremMongotBootstrapper.flushAndShutdownTracing(installed);
    }

    assertTrue(
        "expected an OTLP-JSON span log",
        events.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .anyMatch(msg -> msg.contains("scopeSpans")));
    assertTrue(
        "expected the search span in the emitted traces",
        events.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .anyMatch(msg -> msg.contains("test.search")));
    assertTrue(
        "expected the vector span in the emitted traces",
        events.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .anyMatch(msg -> msg.contains("test.vectorSearch")));
  }

  @Test
  public void configureTracing_enabled_appliesStrategiesAndRatesIndependently() {
    // Rates are per-signal: head sampling at 1.0 records; at 0.0 it does not.
    Optional<SpanProcessor> byRate =
        OnPremMongotBootstrapper.configureTracing(
            Optional.of(
                new TracingConfig(
                    true,
                    TracingConfig.SamplingStrategy.HEAD,
                    TracingConfig.SamplingStrategy.HEAD,
                    1.0,
                    0.0,
                    Optional.empty())),
            this.meterRegistry);
    assertTrue(byRate.isPresent());
    try {
      try (SpanGuard searchGuard = Tracing.searchSpanGuard("rate.search")) {
        assertTrue(searchGuard.getSpan().isRecording());
      }
      try (SpanGuard vectorGuard = Tracing.vectorSearchSpanGuard("rate.vectorSearch")) {
        assertFalse(vectorGuard.getSpan().isRecording());
      }
    } finally {
      byRate.get().shutdown();
    }

    // Strategies are per-signal: tail records regardless of rate; head at 0.0 does not.
    Optional<SpanProcessor> byStrategy =
        OnPremMongotBootstrapper.configureTracing(
            Optional.of(
                new TracingConfig(
                    true,
                    TracingConfig.SamplingStrategy.TAIL,
                    TracingConfig.SamplingStrategy.HEAD,
                    0.0,
                    0.0,
                    Optional.empty())),
            this.meterRegistry);
    assertTrue(byStrategy.isPresent());
    try {
      try (SpanGuard searchGuard = Tracing.searchSpanGuard("strategy.search")) {
        assertTrue(searchGuard.getSpan().isRecording());
      }
      try (SpanGuard vectorGuard = Tracing.vectorSearchSpanGuard("strategy.vectorSearch")) {
        assertFalse(vectorGuard.getSpan().isRecording());
      }
    } finally {
      byStrategy.get().shutdown();
    }
  }

  @Test
  public void configureTracing_absentOrDisabled_recordsNoSpansAndReportsGaugeZero() {
    // `searchSamplingStrategy: tail` in the disabled config guards against a bug where
    // updateStrategyConfig runs before the !enabled check and disabled tracing records every
    // $search query.
    for (Optional<TracingConfig> config :
        List.of(Optional.<TracingConfig>empty(), Optional.of(disabledTracingConfig()))) {
      SimpleMeterRegistry registry = new SimpleMeterRegistry();

      Optional<SpanProcessor> processor =
          OnPremMongotBootstrapper.configureTracing(config, registry);

      assertTrue(processor.isEmpty());
      assertEquals(0.0, registry.get("tracing.enabled").gauge().value(), 0.0);
      try (SpanGuard searchGuard = Tracing.searchSpanGuard("test.search")) {
        assertFalse(searchGuard.getSpan().isRecording());
      }
      try (SpanGuard vectorGuard = Tracing.vectorSearchSpanGuard("test.vectorSearch")) {
        assertFalse(vectorGuard.getSpan().isRecording());
      }
    }
  }

  @Test
  public void composeWithOtlp_invokesLogSinkBeforeOtlp() {
    AtomicBoolean logCalled = new AtomicBoolean(false);
    SpanExporter logSink = mock(SpanExporter.class);
    when(logSink.export(any()))
        .thenAnswer(
            invocation -> {
              logCalled.set(true);
              return CompletableResultCode.ofSuccess();
            });
    SpanExporter otlp = mock(SpanExporter.class);
    when(otlp.export(any()))
        .thenAnswer(
            invocation -> {
              assertTrue("log sink must be invoked before OTLP", logCalled.get());
              return CompletableResultCode.ofSuccess();
            });

    OnPremMongotBootstrapper.composeWithOtlp(logSink, otlp).export(List.of());

    assertTrue(logCalled.get());
  }

  private static TracingConfig disabledTracingConfig() {
    return new TracingConfig(
        false,
        TracingConfig.SamplingStrategy.TAIL,
        TracingConfig.SamplingStrategy.HEAD,
        0.01,
        0.01,
        Optional.empty());
  }

  private static final class HaltError extends Error {
    private HaltError() {
      super("test halt");
    }
  }
}
