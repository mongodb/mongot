package com.xgen.mongot.config.provider.community;

import static com.xgen.mongot.cursor.CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT;
import static com.xgen.mongot.cursor.CursorConfig.DEFAULT_MESSAGE_SIZE_LIMIT;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.mongodb.ReadPreference;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.DefaultMetadataService;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MongodTopologyMonitor;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.config.provider.CommonUtils;
import com.xgen.mongot.config.provider.MongotConfigs;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingServiceManagerConfig;
import com.xgen.mongot.config.provider.monitor.PeriodicConfigMonitor;
import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.DefaultInternalDatabaseResolver;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.index.lucene.LuceneGlobalSettings;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.logging.Logging;
import com.xgen.mongot.metrics.BuildInfoMetrics;
import com.xgen.mongot.metrics.Instrumentation;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.metrics.cache.ScrapeCacheConfig;
import com.xgen.mongot.metrics.ftdc.Ftdc;
import com.xgen.mongot.metrics.ftdc.FtdcConfig;
import com.xgen.mongot.metrics.ftdc.FtdcMetadata;
import com.xgen.mongot.metrics.ftdc.FtdcScheduledReporter;
import com.xgen.mongot.metrics.prometheus.PrometheusServer;
import com.xgen.mongot.metrics.system.SystemMetricsInstrumentation;
import com.xgen.mongot.monitor.DiskMonitor;
import com.xgen.mongot.monitor.PeriodicDiskMonitor;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
import com.xgen.mongot.replication.mongodb.autoembedding.AutoEmbeddingMaterializedViewManagerFactory;
import com.xgen.mongot.server.CommandServer;
import com.xgen.mongot.server.auth.SecurityConfig;
import com.xgen.mongot.server.command.search.SearchCommandsRegister;
import com.xgen.mongot.server.command.search.SearchCommandsRegister.BootstrapperMetadata;
import com.xgen.mongot.server.executors.ExecutorManager;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import com.xgen.mongot.server.grpc.GrpcStreamingServer;
import com.xgen.mongot.server.grpc.HealthManager;
import com.xgen.mongot.server.http.HealthCheckServer;
import com.xgen.mongot.server.http.ReadinessChecker;
import com.xgen.mongot.trace.AllowlistSpanExporter;
import com.xgen.mongot.trace.LatencySamplingSpanProcessor;
import com.xgen.mongot.trace.Slf4jExporter;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.GlobalMetricFactory;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.MongotVersionResolver;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.SecretsParser;
import com.xgen.mongot.util.Shutdown;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.SslContextFactory;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnPremMongotBootstrapper {

  public static final Logger LOG = LoggerFactory.getLogger(OnPremMongotBootstrapper.class);

  private static final Duration DEFAULT_CONFIG_UPDATE_PERIOD = Duration.ofSeconds(1);

  // Fixed cadence for refreshing system metrics (disk/memory/netstat/process) gauges.
  private static final Duration SYSTEM_METRICS_UPDATE_INTERVAL = Duration.ofSeconds(5);

  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  private final FeatureFlags featureFlags;

  private final DeploymentEnvironment edition;

  /**
   * Builds the driver token-acquisition callback when the config carries an {@code oidc} block.
   * Supplied by the enterprise entry point; community uses {@link
   * OidcCallbackFactory#unsupported()} (the enterprise-gated {@code oidc} field is rejected at
   * parse time).
   */
  private final OidcCallbackFactory oidcCallbackFactory;

  /**
   * Creates a bootstrapper for an on-prem mongot edition.
   *
   * @param dynamicFeatureFlagRegistry the registry wired into all subsystems and installed as the
   *     process-wide instance; create it only once per process
   * @param featureFlags the edition's feature-flag defaults, e.g. {@link
   *     FeatureFlags#communityDefaults()} or {@link FeatureFlags#enterpriseDefaults()}
   * @param edition the on-prem edition being bootstrapped
   */
  public OnPremMongotBootstrapper(
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      FeatureFlags featureFlags,
      DeploymentEnvironment edition) {
    this(dynamicFeatureFlagRegistry, featureFlags, edition, OidcCallbackFactory.unsupported());
  }

  /**
   * Same as {@link #OnPremMongotBootstrapper(DynamicFeatureFlagRegistry, FeatureFlags,
   * DeploymentEnvironment)}, with the {@link OidcCallbackFactory} used when the config carries an
   * {@code oidc} block.
   */
  public OnPremMongotBootstrapper(
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      FeatureFlags featureFlags,
      DeploymentEnvironment edition,
      OidcCallbackFactory oidcCallbackFactory) {
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.featureFlags = featureFlags;
    this.edition = edition;
    this.oidcCallbackFactory = oidcCallbackFactory;
  }

  /**
   * Bootstraps the mongot using the config file from the supplied configPath, or an empty config
   * when no config path is given.
   *
   * <p>This method does not return until all mongot subsystems have been configured and started.
   */
  public void bootstrap(Optional<Path> configPath, boolean internalListAllIndexesForTesting) {
    bootstrap(configPath, internalListAllIndexesForTesting, Map.of());
  }

  /**
   * Bootstraps the mongot using the config file from the supplied configPath, or an empty config
   * when no config path is given, merging any CLI-supplied config values into it.
   *
   * <p>This method does not return until all mongot subsystems have been configured and started.
   *
   * @param configPath the path of the YAML config file, or empty for an empty config.
   * @param cliValues the CLI-supplied config values keyed by dot-delimited YAML path.
   */
  public void bootstrap(
      Optional<Path> configPath,
      boolean internalListAllIndexesForTesting,
      Map<String, BsonValue> cliValues) {

    CommunityConfig.ParsedCommunityConfig parsedConfig =
        Crash.because("failed to parse config file")
            .ifThrowsExceptionOrError(
                () ->
                    CommunityConfig.readFromFile(configPath, cliValues, Optional.of(this.edition)));
    CommunityConfig config = parsedConfig.config();

    config
        .loggingConfig()
        .ifPresent(
            loggingConfig -> {
              Logging.setRootLevel(loggingConfig.verbosity());
              loggingConfig
                  .logPath()
                  .ifPresent(
                      logPath -> {
                        LOG.atInfo()
                            .addKeyValue("logPath", logPath)
                            .log("Configuring logging to file");
                        Logging.enableFileAppender(logPath);
                      });
              // Attached after enableFileAppender: that call detaches every root appender and
              // expects the async console appender to be first, so the OTLP sink must come last.
              loggingConfig
                  .otlp()
                  .ifPresent(
                      otlp -> {
                        Logging.attachRootAppender(logAppender(otlp));
                        LOG.atInfo()
                            .addKeyValue("endpoint", otlp.endpoint())
                            .log("Shipping logs to OTLP collector");
                      });
            });

    // Unlike production mode, we expect that the path to the data dir may not exist yet
    Crash.because("failed to create data directory")
        .ifThrowsExceptionOrError(() -> Files.createDirectories(config.storageConfig().dataPath()));

    // The server-id is stored to disk so we need the datapath to exist before fetching it.
    var serverInfo = new CommunityServerInfo(config);
    LOG.atInfo().addKeyValue("serverInfo", serverInfo).log("Starting server with server info");

    var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithCompositeMeterRegistry();
    var meterRegistry = meterAndFtdcRegistry.getCompositeMeterRegistry();

    // Log any BsonParseExceptions from unknown fields when parsing the mongot config.
    reportUnknownFields(parsedConfig.unknownFieldExceptions());

    // InitialSyncHostProvider starts replica-set discovery when it's first instantiated. Initiate
    // it early in the bootstrap process so as a best-effort it can be ready before first use.
    var initialSyncHostProvider =
        new InitialSyncHostProvider(
            config.syncSourceConfig(), meterRegistry, this.oidcCallbackFactory);

    var indexCatalog = new DefaultIndexCatalog();
    // Shared with the ConfigManager, which registers the journal entries it cannot parse.
    var incompatibleIndexes = new IncompatibleIndexes(meterRegistry);
    var initializedIndexCatalog = new InitializedIndexCatalog();

    var mongotVersion = MongotVersionResolver.create().getVersion();
    var mongotConfigs = this.getMongotConfigs(config);

    // Initialize static global feature flags for quantization or codec objects
    FeatureFlags.initializeProcessInstance(mongotConfigs.featureFlags);

    SearchAggregationBootstrap.installRegistryIfEnabled(mongotConfigs.featureFlags);

    // Initialize global feature flags for utility classes
    LoggableIdUtils.initialize(mongotConfigs.featureFlags.isEnabled(Feature.LOGGABLE_DOCUMENT_ID));

    var prometheusServerOptional =
        config
            .metricsConfig()
            .flatMap(mc -> maybeStartPrometheusServer(mc, mongotConfigs.featureFlags));
    prometheusServerOptional.ifPresent(
        (promServer) -> meterRegistry.add(promServer.getPrometheusMeterRegistry()));

    meterRegistry.add(meterAndFtdcRegistry.ftdcRegistry());
    var ftdcReporterLifecycle =
        maybeCreateFtdcReporter(
            meterRegistry,
            meterAndFtdcRegistry.ftdcRegistry(),
            config,
            mongotConfigs,
            mongotVersion,
            meterAndFtdcRegistry.threadPoolResourceBindings());

    // Initialize global metrics for community
    GlobalMetricFactory.initialize(meterRegistry);
    BuildInfoMetrics.register(
        mongotVersion,
        mongotConfigs.featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR),
        meterRegistry);
    var systemMetricsInstrumentation = maybeStartSystemMetrics(config, meterRegistry);
    var tracingProcessor = configureTracing(config.tracingConfig(), meterRegistry);

    var cursorManager =
        MongotCursorManagerImpl.fromConfig(
            mongotConfigs.cursorConfig, meterRegistry, indexCatalog, initializedIndexCatalog);

    var diskMonitorConfig = config.diskMonitorConfig();
    var diskMonitor =
        PeriodicDiskMonitor.createAndStart(
            config.storageConfig().dataPath(), diskMonitorConfig.crashThreshold(), meterRegistry);
    var replicationGateConfig =
        new HysteresisConfig(
            diskMonitorConfig.resumeReplicationThreshold(),
            diskMonitorConfig.pauseReplicationThreshold());
    var initialSyncGateConfig =
        Optional.of(
            new HysteresisConfig(
                diskMonitorConfig.resumeInitialSyncThreshold(),
                diskMonitorConfig.pauseInitialSyncThreshold()));
    var replicationStateMonitor =
        ReplicationStateMonitor.diskBased(
            replicationGateConfig, initialSyncGateConfig, diskMonitor);

    var syncSourceConfig =
        syncSourceConfig(
            config.syncSourceConfig(), initialSyncHostProvider, this.oidcCallbackFactory);
    var mongoDbMetadataClient =
        new MongoDbMetadataClient(Optional.of(syncSourceConfig), meterRegistry);
    mongoDbMetadataClient.refreshServerInfo();

    var mongodTopologyMonitor = new MongodTopologyMonitor(syncSourceConfig, meterRegistry);
    mongodTopologyMonitor.start();
    var clusterTopologyGuard =
        new CatalogAccessGuard(
            mongodTopologyMonitor, config.syncSourceConfig().router().isPresent());

    // Crash early if mongod is reachable and the topology contradicts syncSource.router.
    // If mongod is unreachable, continue.
    failFastOnTopologyMismatch(clusterTopologyGuard);

    // Initialize global DynamicFeatureFlagRegistry for non-DI paths
    DynamicFeatureFlagRegistry.initializeProcessInstance(this.dynamicFeatureFlagRegistry);

    var commandRegisterMetadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            mongotVersion,
            this.edition.deploymentEnvironmentName(),
            mongoDbMetadataClient,
            mongotConfigs.featureFlags,
            this.dynamicFeatureFlagRegistry);

    // Initialize the community metadata service
    var metadataService = initializeMetadataService(syncSourceConfig, meterRegistry);

    // Initialize embedding service if configured
    Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier =
        initializeEmbeddingService(config, mongotConfigs, meterRegistry, commandRegisterMetadata);

    // Create the ConfigManager and initialize it. This initializes all the indexes and begins
    // replication.
    boolean isAutoEmbeddingViewWriter =
        config.embeddingConfig().map(ec -> ec.isAutoEmbeddingViewWriter()).orElse(false);

    var configManager =
        configManager(
            config.storageConfig().dataPath(),
            mongotConfigs,
            indexCatalog,
            initializedIndexCatalog,
            incompatibleIndexes,
            cursorManager,
            syncSourceConfig,
            meterAndFtdcRegistry,
            diskMonitor,
            replicationStateMonitor,
            mongotConfigs.featureFlags,
            this.dynamicFeatureFlagRegistry,
            embeddingServiceManagerSupplier,
            isAutoEmbeddingViewWriter);

    var metadataUpdater =
        metadataUpdater(
            serverInfo,
            metadataService,
            configManager,
            clusterTopologyGuard,
            mongoDbMetadataClient,
            meterRegistry,
            internalListAllIndexesForTesting);

    var healthManager = new HealthManager(configManager, meterRegistry);
    var serverLifecycles =
        servers(
            config,
            cursorManager,
            configManager,
            indexCatalog,
            initializedIndexCatalog,
            incompatibleIndexes,
            commandRegisterMetadata,
            mongotConfigs.regularBlockingRequestSettings,
            meterRegistry,
            healthManager,
            metadataService,
            clusterTopologyGuard,
            internalListAllIndexesForTesting,
            mongotConfigs.indexDefinitionConfig,
            embeddingServiceManagerSupplier,
            mongotConfigs.featureFlags,
            this.dynamicFeatureFlagRegistry,
            meterAndFtdcRegistry.threadPoolResourceBindings());

    var readinessChecker =
        new CommunityReadinessChecker(
            serverInfo,
            configManager,
            configManager,
            metadataService,
            clusterTopologyGuard,
            serverLifecycles.servers);
    var healthCheckServer =
        createHealthCheckServer(config, meterRegistry, healthManager, readinessChecker);

    // Create a PeriodicConfigMonitor that will regularly check the authoritative catalog for
    // updates, but do not start it until the shutdown hook is registered.
    var configMonitor =
        configMonitor(
            metadataService.getAuthoritativeIndexCatalog(),
            mongoDbMetadataClient,
            configManager,
            mongotConfigs.featureFlags,
            meterRegistry,
            syncSourceConfig,
            initialSyncHostProvider,
            clusterTopologyGuard);

    // Register shutdown hook prior to starting the server.
    // Close the server to cleanly finish any in-flight requests.
    // Close the logger last to flush any final logs from the server.
    Shutdown.registerHook(
        healthManager::enterTerminalState,
        serverLifecycles.stop,
        () -> systemMetricsInstrumentation.ifPresent(SystemMetricsInstrumentation::stop),
        () -> prometheusServerOptional.ifPresent(PrometheusServer::shutdown),
        () -> ftdcReporterLifecycle.ifPresent(f -> f.stop.run()),
        metadataUpdater::stop,
        configManager::close,
        healthCheckServer::stop,
        diskMonitor::stop,
        metadataService::close,
        initialSyncHostProvider::close,
        mongodTopologyMonitor::close,
        // Flush and stop tracing before the logger is closed so buffered traces are not lost.
        () -> tracingProcessor.ifPresent(OnPremMongotBootstrapper::flushAndShutdownTracing),
        Logging::shutdown);

    // Start our background processes.
    configMonitor.start();
    metadataUpdater.start();
    serverLifecycles.start.run();
    ftdcReporterLifecycle.ifPresent(f -> f.start.run());
    // Begin periodically refreshing system metrics (disk/memory/netstat/process gauges). Without
    // this the gauges (e.g. system.disk.space.data.path.free) stay frozen at their startup
    // snapshot.
    systemMetricsInstrumentation.ifPresent(
        instrumentation ->
            instrumentation.start(SYSTEM_METRICS_UPDATE_INTERVAL.getSeconds(), TimeUnit.SECONDS));

    // Health check server should be started last, to minimize the chance that we service health
    // check requests on startup that immediately return unhealthy.
    healthCheckServer.start();
  }

  private static SyncSourceConfig syncSourceConfig(
      com.xgen.mongot.config.provider.community.SyncSourceConfig communitySyncSourceConfig,
      InitialSyncHostProvider initialSyncHostProvider,
      OidcCallbackFactory oidcCallbackFactory) {

    var replicaSet = communitySyncSourceConfig.replicaSet();
    var replicationReadPreference = communitySyncSourceConfig.getReplicationReaderReadPreference();

    var mongodClusterReplicationUri =
        ConnectionInfoFactory.getClusterConnectionInfo(
            replicaSet, replicationReadPreference, oidcCallbackFactory);

    // Default the read-preference to secondary-preferred. Callers should override to primary where
    // applicable.
    var mongodClusterReadWriteUri =
        ConnectionInfoFactory.getClusterConnectionInfo(
            replicaSet, ReadPreference.secondaryPreferred(), oidcCallbackFactory);

    var mongosClusterReadWriteUri =
        communitySyncSourceConfig
            .router()
            .map(
                router ->
                    ConnectionInfoFactory.getClusterConnectionInfo(
                        router, ReadPreference.secondaryPreferred(), oidcCallbackFactory));

    // The InitialSyncHostProvider may not have finished replica-set discovery and is not 'ready'
    // yet. If that's the case the SyncSourceConfig will be configured with empty single host
    // replication URIs and we will start up the NoOpReplicationManager. By the first run of the
    // CommunityConfigUpdater the InitialSyncHostProvider will be ready and start up the real
    // ReplicationManager.
    return SyncSourceConfig.builder()
        .mongodSingleHostReplicationUri(initialSyncHostProvider.getMongodInitialSyncConnection())
        .mongodClusterReplicationUri(mongodClusterReplicationUri)
        .mongodClusterReadWriteUri(mongodClusterReadWriteUri)
        .mongosSingleHostReplicationUri(initialSyncHostProvider.getMongosInitialSyncConnection())
        .mongosClusterReadWriteUri(mongosClusterReadWriteUri)
        .isSharded(communitySyncSourceConfig.router().isPresent())
        .build();
  }

  @VisibleForTesting
  static Optional<PrometheusServer> maybeStartPrometheusServer(
      MetricsConfig metricsConfig, FeatureFlags featureFlags) {
    if (!metricsConfig.enabled()) {
      LOG.info("Not starting Prometheus endpoint since it was explicitly disabled");
      return Optional.empty();
    }

    var address = OnPremMongotBootstrapper.parseInetSocketAddress(metricsConfig.address());
    // Build the server TLS context before binding so invalid TLS material fails startup with an
    // actionable configuration error rather than at the first scrape.
    Optional<SSLContext> serverSslContext =
        metricsConfig
            .tls()
            .map(
                tls ->
                    SslContextFactory.withCertKeyFile(
                            Optional.of(tls.caFile()),
                            tls.certificateKeyFile(),
                            tls.certificateKeyFilePasswordFile())
                        .getJavaContext());
    LOG.atInfo()
        .addKeyValue("address", address)
        .addKeyValue("mtlsEnabled", serverSslContext.isPresent())
        .log(
            serverSslContext.isPresent()
                ? "Starting Prometheus endpoint over mTLS"
                : "Starting Prometheus endpoint over HTTP");
    return Optional.of(
        PrometheusServer.start(
            address,
            List.of(),
            List.of(),
            featureFlags,
            ScrapeCacheConfig.getDefault(),
            Optional.empty(),
            serverSslContext));
  }

  private static Optional<SystemMetricsInstrumentation> maybeStartSystemMetrics(
      CommunityConfig config, MeterRegistry meterRegistry) {
    if (!config.metricsConfig().map(MetricsConfig::enabled).orElse(false)) {
      return Optional.empty();
    }
    Instrumentation.instrumentJvmMetrics(meterRegistry);
    return Optional.of(
        SystemMetricsInstrumentation.create(meterRegistry, config.storageConfig().dataPath()));
  }

  /**
   * Configures mongot-local OpenTelemetry query tracing when the self-managed {@code tracing} block
   * is enabled. Spans are always exported as OTLP-JSON through mongot's structured logs; an
   * optional {@code tracing.otlp} block adds an OTLP/gRPC collector as an additional sink.
   *
   * <p>Tracing is disabled when the block is absent or {@code enabled: false}, and a disabled
   * deployment neither constructs an OTLP exporter nor opens an outbound connection. The {@code
   * otlp} block and its CA file are still validated while the config is read.
   *
   * @return the installed processor when tracing is enabled, or empty when it is disabled
   */
  @VisibleForTesting
  static Optional<SpanProcessor> configureTracing(
      Optional<TracingConfig> tracingConfig, MeterRegistry meterRegistry) {
    boolean enabled = tracingConfig.map(TracingConfig::enabled).orElse(false);
    Gauge.builder("tracing.enabled", () -> enabled ? 1.0 : 0.0)
        .strongReference(true)
        .register(meterRegistry);

    if (!enabled) {
      // Install an explicit no-op provider so a later getTracer() cannot lazily create the default
      // processor (and its scheduler/exporter) on a disabled deployment. Tracing is startup-only.
      Tracing.initializeTracer(Optional.empty(), SpanProcessor.composite(List.of()));
      LOG.atDebug().log("OTEL tracing is disabled.");
      return Optional.empty();
    }

    TracingConfig config = tracingConfig.get();
    Tracing.updateStrategyConfig(
        Optional.of(config.searchSamplingStrategy().name()),
        Optional.of(config.vectorSamplingStrategy().name()),
        Optional.of(config.searchTracingSamplerRate()),
        Optional.of(config.vectorTracingSamplerRate()));

    SpanProcessor processor =
        LatencySamplingSpanProcessor.builder(tracingExporter(config.otlp())).build();
    Tracing.initializeTracer(Optional.empty(), processor);

    LOG.atInfo()
        .addKeyValue("searchSamplingStrategy", config.searchSamplingStrategy())
        .addKeyValue("vectorSamplingStrategy", config.vectorSamplingStrategy())
        .addKeyValue("searchTracingSamplerRate", config.searchTracingSamplerRate())
        .addKeyValue("vectorTracingSamplerRate", config.vectorTracingSamplerRate())
        .addKeyValue("otlpSink", config.otlp().isPresent())
        .log(
            config.otlp().isPresent()
                ? "OTEL tracing enabled; traces export as OTLP-JSON through mongot logs and to the "
                    + "configured OTLP collector."
                : "OTEL tracing enabled; traces export as OTLP-JSON through mongot logs.");
    return Optional.of(processor);
  }

  /**
   * Builds the exporter for enabled tracing: the log sink, plus the allowlist, plus any OTLP sink.
   */
  @VisibleForTesting
  static SpanExporter tracingExporter(Optional<OtlpTracingConfig> otlpConfig) {
    return otlpConfig
        .map(config -> composeWithOtlp(Slf4jExporter.create(), OtlpExporterFactory.create(config)))
        .orElseGet(() -> new AllowlistSpanExporter(Slf4jExporter.create()));
  }

  /**
   * Composes the log sink first with an OTLP sink behind the attribute allowlist, so a blocked
   * collector cannot starve local log delivery.
   */
  @VisibleForTesting
  static SpanExporter composeWithOtlp(SpanExporter logExporter, SpanExporter otlpExporter) {
    return new AllowlistSpanExporter(SpanExporter.composite(logExporter, otlpExporter));
  }

  /**
   * Builds the OTLP log appender for a {@code logging.otlp} block, reusing the same exporter
   * settings as the trace sink. The caller attaches it to the root logger.
   */
  @VisibleForTesting
  static Appender<ILoggingEvent> logAppender(OtlpTracingConfig otlp) {
    return OtlpLogAppender.create(OtlpExporterFactory.createLogRecordExporter(otlp));
  }

  /**
   * Synchronously flushes buffered traces and then stops the tracing processor during shutdown, so
   * traces retained since the last sampling window are exported before the logging pipeline closes.
   * The flush is bounded; if it does not complete in time the processor is still shut down.
   */
  @VisibleForTesting
  static void flushAndShutdownTracing(SpanProcessor processor) {
    processor.forceFlush().join(5, TimeUnit.SECONDS);
    processor.shutdown();
  }

  static Optional<MetricsLifecycles> maybeCreateFtdcReporter(
      CompositeMeterRegistry executorRegistry,
      MeterRegistry reportingRegistry,
      CommunityConfig communityConfig,
      MongotConfigs mongotConfigs,
      String mongotVersion,
      ThreadPoolResourceMetrics.Bindings threadPoolResourceBindings) {

    FtdcCommunityConfig ftdcCommunityConfig = communityConfig.ftdcConfig();
    if (!ftdcCommunityConfig.enabled()) {
      LOG.info("FTDC reporter not enabled");
      return Optional.empty();
    }

    var ftdcConfig =
        FtdcConfig.create(
            communityConfig.storageConfig().dataPath().resolve("diagnostic.data"),
            Optional.of(Bytes.ofMebi(ftdcCommunityConfig.directorySizeMB())),
            Optional.of(Bytes.ofMebi(ftdcCommunityConfig.fileSizeMB())),
            // Use defaults for the remaining configs
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    var metadata =
        new FtdcMetadata.Builder()
            .addStaticInfo("configuration", communityConfig.toBson())
            .addStaticInfo("internalConfiguration", mongotConfigs.toBson())
            .addStaticInfo("hostInfo", serializeHostInfo())
            .addStaticInfo(
                "buildInfo",
                serializeBuildInfo(communityConfig.storageConfig().dataPath(), mongotVersion))
            .build();

    var ftdc =
        Crash.because("failed to initialize ftdc")
            .ifThrowsExceptionOrError(() -> Ftdc.initialize(ftdcConfig, metadata));
    // If feature flag is enabled, use CompositeMeterRegistry so executor metrics are exported to
    // both FTDC itself and Prometheus. Otherwise, use ftdcRegistry to keep executor metrics only
    // in FTDC.
    boolean useCombinedRegistryForExecutorMetrics =
        mongotConfigs.featureFlags.isEnabled(Feature.FTDC_EXECUTOR_METRICS_TO_PROMETHEUS);
    var reporter =
        FtdcScheduledReporter.create(
            reportingRegistry,
            executorRegistry,
            ftdc,
            useCombinedRegistryForExecutorMetrics,
            ftdcConfig.maxMeterCount(),
            threadPoolResourceBindings);

    return Optional.of(
        new MetricsLifecycles(
            () ->
                reporter.start(
                    communityConfig.ftdcConfig().collectionPeriodMillis(), TimeUnit.MILLISECONDS),
            reporter::stop));
  }

  private static HealthCheckServer createHealthCheckServer(
      CommunityConfig config,
      MeterRegistry meterRegistry,
      HealthManager healthManager,
      ReadinessChecker readinessChecker) {
    InetSocketAddress address =
        config
            .healthCheckConfig()
            .map(c -> parseInetSocketAddress(c.address()))
            .orElse(new InetSocketAddress("localhost", 8080));
    return HealthCheckServer.create(address, meterRegistry, healthManager, readinessChecker);
  }

  private static SecurityConfig createGrpcSecurityConfig(ServerConfig config) {
    return SecurityConfig.createAuthDisabled(
        config.getGrpcTlsMode(),
        config.getGrpcCertificateKeyFile(),
        config.getGrpcCertificateKeyFilePasswordFile(),
        config.getGrpcCaFile());
  }

  private static InetSocketAddress parseInetSocketAddress(String address) {
    HostAndPort hostAndPort = HostAndPort.fromString(address);
    InetSocketAddress serverAddress =
        new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
    if (serverAddress.isUnresolved()) {
      throw new IllegalArgumentException(
          String.format("could not resolve address: %s", serverAddress));
    }
    return serverAddress;
  }

  private static MetadataService initializeMetadataService(
      SyncSourceConfig syncSourceConfig, MeterRegistry meterRegistry) {
    return DefaultMetadataService.create(syncSourceConfig, meterRegistry);
  }

  /**
   * Bootstrap-time topology mismatch check. Calls {@link CatalogAccessGuard#requireTopologyMatch()}
   * once and:
   *
   * <ul>
   *   <li>On {@link TopologyMismatchException}: crashes the process. The mongot's {@code
   *       syncSource.router} setting does not match the actual cluster topology.
   *   <li>On {@link CheckedMongoException}: logs and continues. The cluster topology could not be
   *       determined (mongod might not be available yet).
   * </ul>
   */
  @VisibleForTesting
  static void failFastOnTopologyMismatch(CatalogAccessGuard catalogAccessGuard) {
    try {
      catalogAccessGuard.requireTopologyMatch();
      LOG.info("Bootstrap topology check passed: syncSource.router matches cluster topology.");
    } catch (TopologyMismatchException e) {
      Crash.because(
              "Bootstrap topology check failed: syncSource.router does not match the actual"
                  + " cluster topology; update the mongot config and restart.")
          .withThrowable(e)
          .now();
    } catch (CheckedMongoException e) {
      LOG.info(
          "Bootstrap topology check skipped. mongod might not be available yet."
              + "Continuing bootstrap.");
    }
  }

  /**
   * Initializes the embedding service by loading internal configuration and applying any user
   * overrides.
   *
   * @param config the community configuration
   * @param mongotConfigs the mongot configs
   * @param meterRegistry the meter registry
   * @return a memoized supplier for the EmbeddingServiceManager
   */
  static Supplier<EmbeddingServiceManager> initializeEmbeddingService(
      CommunityConfig config,
      MongotConfigs mongotConfigs,
      MeterRegistry meterRegistry,
      SearchCommandsRegister.BootstrapperMetadata bootstrapperMetadata) {

    // Load credentials from files if configured
    Optional<EmbeddingServiceManagerConfig.VoyageCredentials> credentials =
        loadVoyageCredentials(config.embeddingConfig());

    // Load internal embedding configuration
    Optional<EmbeddingServiceManagerConfig> embeddingConfigOpt =
        EmbeddingServiceManagerConfig.loadEmbeddingServiceConfig(credentials);

    if (embeddingConfigOpt.isEmpty()) {
      LOG.info(
          "Auto-embedding functionality is disabled. To enable, configure queryKeyFile and "
              + "indexingKeyFile in the embedding section of your community config.");
      // Return no-op supplier - embedding service manager with empty configs
      return Suppliers.memoize(
          () ->
              new EmbeddingServiceManager(
                  List.of(),
                  new EmbeddingClientFactory(meterRegistry, DeploymentEnvironment.COMMUNITY),
                  Executors.fixedSizeThreadScheduledExecutor(
                      "embedding-providers", 1, meterRegistry),
                  meterRegistry,
                  mongotConfigs.autoEmbeddingMaterializedViewConfig.congestionControl));
    }

    // Get user's endpoint override from config
    Optional<String> providerEndpointOverride =
        config.embeddingConfig().flatMap(EmbeddingConfig::providerEndpoint);

    // Get embedding configs and optionally apply endpoint override
    List<EmbeddingServiceConfig> embeddingServiceConfigs =
        providerEndpointOverride.isPresent()
            ? applyEndpointOverride(
                embeddingConfigOpt.get().configs(), providerEndpointOverride.get())
            : embeddingConfigOpt.get().configs();

    // Update embedding model catalog
    EmbeddingModelCatalog.updateModelConfigs(embeddingServiceConfigs);
    // Enable materialized view support for community (default for community deployments)
    EmbeddingModelCatalog.enableMatView(true);

    LOG.info("Initialized auto-embedding with {} model(s)", embeddingServiceConfigs.size());

    // Return memoized supplier
    return Suppliers.memoize(
        () ->
            new EmbeddingServiceManager(
                embeddingServiceConfigs,
                new EmbeddingClientFactory(meterRegistry, DeploymentEnvironment.COMMUNITY),
                Executors.fixedSizeThreadScheduledExecutor(
                    "embedding-providers",
                    mongotConfigs.autoEmbeddingMaterializedViewConfig.numEmbeddingThreads,
                    meterRegistry),
                meterRegistry,
                mongotConfigs.autoEmbeddingMaterializedViewConfig.congestionControl,
                mongotConfigs.autoEmbeddingMaterializedViewConfig.embeddingProviderRpsLimit));
  }

  /** Loads Voyage API credential secrets from files specified in embedding config. */
  private static Optional<EmbeddingServiceManagerConfig.VoyageCredentials> loadVoyageCredentials(
      Optional<EmbeddingConfig> embeddingConfig) {
    if (embeddingConfig.isEmpty()) {
      return Optional.empty();
    }

    EmbeddingConfig config = embeddingConfig.get();
    if (config.queryKeyFile().isEmpty() || config.indexingKeyFile().isEmpty()) {
      LOG.warn(
          "Voyage API credential files not configured. Auto-embedding functionality "
              + "will be disabled. To enable auto-embedding indexes, specify both queryKeyFile "
              + "and indexingKeyFile in the embedding configuration.");
      return Optional.empty();
    }

    try {
      String queryKey = SecretsParser.readSecretFile(config.queryKeyFile().get());
      String indexingKey = SecretsParser.readSecretFile(config.indexingKeyFile().get());

      LOG.info(
          "Loaded Voyage credentials from files: {} and {}",
          config.queryKeyFile().get(),
          config.indexingKeyFile().get());

      return Optional.of(
          new EmbeddingServiceManagerConfig.VoyageCredentials(
              new VoyageEmbeddingCredentials(queryKey),
              new VoyageEmbeddingCredentials(indexingKey)));
    } catch (IOException | IllegalArgumentException e) {
      LOG.warn("Failed to read Voyage API credential files. Auto-embedding disabled.", e);
      return Optional.empty();
    }
  }

  /** Applies provider endpoint override to all embedding service configs. */
  static List<EmbeddingServiceConfig> applyEndpointOverride(
      List<EmbeddingServiceConfig> configs, String endpoint) {
    LOG.info("Overriding embedding provider endpoint with user-provided value: {}", endpoint);
    return configs.stream()
        .map(
            serviceConfig ->
                new EmbeddingServiceConfig(
                    serviceConfig.embeddingProvider,
                    serviceConfig.modelName,
                    serviceConfig.rpsPerProvider,
                    new EmbeddingServiceConfig.EmbeddingConfig(
                        serviceConfig.embeddingConfig.region,
                        serviceConfig.embeddingConfig.getModelConfigBase(),
                        serviceConfig.embeddingConfig.getErrorHandlingConfigBase(),
                        serviceConfig.embeddingConfig.getCredentialsBase(),
                        serviceConfig.embeddingConfig.getQueryParams(),
                        serviceConfig.embeddingConfig.getCollectionScanParams(),
                        serviceConfig.embeddingConfig.getChangeStreamParams(),
                        serviceConfig.embeddingConfig.getTenantCredentials(),
                        serviceConfig.embeddingConfig.isDedicatedCluster,
                        Optional.of(endpoint),
                        serviceConfig.embeddingConfig.useFlexTier,
                        serviceConfig.embeddingConfig.rpsPerProvider),
                    serviceConfig.compatibleModels))
        .toList();
  }

  private static CommunityMetadataUpdater metadataUpdater(
      CommunityServerInfo serverInfo,
      MetadataService metadataService,
      ConfigManager configManager,
      CatalogAccessGuard catalogAccessGuard,
      MongoDbMetadataClient mongoDbMetadataClient,
      MeterRegistry meterRegistry,
      boolean internalListAllIndexesForTesting) {
    Duration runFrequency =
        internalListAllIndexesForTesting ? Duration.ofSeconds(2) : Duration.ofSeconds(30);

    if (internalListAllIndexesForTesting) {
      LOG.atInfo()
          .addKeyValue("runFrequency", runFrequency)
          .log(
              "Starting up with 'internalListAllIndexesForTesting' flag, "
                  + "increasing run frequency of community metadata updater.");
    }

    return new CommunityMetadataUpdater(
        serverInfo,
        metadataService,
        configManager,
        catalogAccessGuard,
        mongoDbMetadataClient,
        meterRegistry,
        runFrequency);
  }

  private static DefaultConfigManager configManager(
      Path dataPath,
      MongotConfigs mongotConfigs,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      IncompatibleIndexes incompatibleIndexes,
      MongotCursorManager cursorManager,
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      DiskMonitor diskMonitor,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      boolean isAutoEmbeddingViewWriter) {
    LuceneGlobalSettings.apply(mongotConfigs.luceneConfig);
    var analyzerRegistryFactory =
        Crash.because("failed to get AnalyzerRegistry.Factory instance")
            .ifThrowsExceptionOrError(AnalyzerRegistry::factory);
    var indexFactory =
        Crash.because("failed to get LuceneIndexFactory instance")
            .ifThrowsExceptionOrError(
                () ->
                    LuceneIndexFactory.fromConfig(
                        mongotConfigs.luceneConfig,
                        mongotConfigs.featureFlags,
                        dynamicFeatureFlagRegistry,
                        mongotConfigs.environmentVariantPerfConfig,
                        meterAndFtdcRegistry,
                        Optional.empty(),
                        analyzerRegistryFactory,
                        diskMonitor,
                        mongotConfigs.initialSyncConfig.enableNaturalOrderScan()));

    var mvMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    var autoEmbeddingMongoClient =
        new AutoEmbeddingMongoClient(
            Optional.of(syncSourceConfig),
            meterAndFtdcRegistry.meterRegistry(),
            mongotConfigs.autoEmbeddingMaterializedViewConfig);
    var dbResolver = new DefaultInternalDatabaseResolver();
    var leaseManager =
        CommonUtils.getLeaseManager(
            dbResolver,
            autoEmbeddingMongoClient,
            meterAndFtdcRegistry,
            isAutoEmbeddingViewWriter,
            mvMetadataCatalog);
    var mvCollectionResolver =
        CommonUtils.getMaterializedViewCollectionResolver(
            dbResolver,
            autoEmbeddingMongoClient,
            mvMetadataCatalog,
            leaseManager,
            mongotConfigs.autoEmbeddingMaterializedViewConfig,
            meterAndFtdcRegistry.meterRegistry());
    var materializedViewIndexFactory =
        CommonUtils.getMaterializedViewIndexFactory(
            autoEmbeddingMongoClient,
            featureFlags,
            meterAndFtdcRegistry,
            leaseManager,
            mvCollectionResolver,
            dbResolver,
            mongotConfigs.autoEmbeddingMaterializedViewConfig);
    var replicationManagerFactory =
        CommonUtils.getReplicationManagerFactory(
            dataPath,
            mongotConfigs.replicationConfig,
            mongotConfigs.initialSyncConfig,
            mongotConfigs.durabilityConfig,
            mongotConfigs.featureFlags,
            cursorManager,
            indexCatalog,
            initializedIndexCatalog,
            meterAndFtdcRegistry,
            DefaultConfigManager.ReplicationMode.DISK_UTILIZATION_BASED,
            replicationStateMonitor,
            Optional.of(embeddingServiceManagerSupplier),
            Optional.empty());

    AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMaterializedViewManagerFactory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            dataPath,
            mongotConfigs.autoEmbeddingMaterializedViewConfig,
            mongotConfigs.initialSyncConfig,
            mongotConfigs.featureFlags,
            cursorManager,
            meterAndFtdcRegistry,
            DefaultConfigManager.ReplicationMode.ENABLE,
            Optional.of(embeddingServiceManagerSupplier),
            leaseManager,
            indexCatalog,
            mvMetadataCatalog,
            autoEmbeddingMongoClient,
            dynamicFeatureFlagRegistry);

    var lifecycleManager =
        CommonUtils.getLifecycleManager(
            mongotConfigs.lifecycleConfig,
            Optional.of(syncSourceConfig),
            replicationManagerFactory,
            initializedIndexCatalog,
            indexFactory,
            Optional.empty(),
            autoEmbeddingMaterializedViewManagerFactory,
            meterAndFtdcRegistry.meterRegistry(),
            replicationStateMonitor.getReplicationGate(),
            mongotConfigs.featureFlags,
            meterAndFtdcRegistry.threadPoolResourceBindings());

    return DefaultConfigManager.initialize(
        indexCatalog,
        Optional.empty(),
        initializedIndexCatalog,
        incompatibleIndexes,
        cursorManager,
        indexFactory,
        Optional.of(materializedViewIndexFactory),
        dataPath.resolve("configJournal.json"),
        meterAndFtdcRegistry.meterRegistry(),
        lifecycleManager,
        replicationStateMonitor,
        featureFlags,
        dataPath.resolve("dynamicFeatureFlagsJournal.json"),
        dynamicFeatureFlagRegistry);
  }

  private static ServerLifecycles servers(
      CommunityConfig config,
      MongotCursorManager cursorManager,
      ConfigManager configManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      QuarantinedIndexLookup quarantinedIndexLookup,
      BootstrapperMetadata metadata,
      RegularBlockingRequestSettings regularBlockingRequestSettings,
      MeterRegistry meterRegistry,
      HealthManager healthManager,
      MetadataService metadataService,
      CatalogAccessGuard catalogAccessGuard,
      boolean internalListAllIndexesForTesting,
      IndexDefinitionConfig indexDefinitionConfig,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      ThreadPoolResourceMetrics.Bindings threadPoolResourceBindings) {
    boolean shouldRetryViaEnvoy =
        SearchQueryAdmissionControlConfigMapper.shouldRetryViaLoadBalancer(
            config.advancedConfigs().flatMap(AdvancedConfigs::regularBlockingRequestConfig));
    // Create the ExecutorManager that can serve requests for the tcpServer (and the grpcServer).
    // noinspection resource
    var executorManager =
        new ExecutorManager(
            meterRegistry,
            regularBlockingRequestSettings,
            featureFlags,
            dynamicFeatureFlagRegistry,
            Optional.of(threadPoolResourceBindings),
            shouldRetryViaEnvoy);

    var grpcServer =
        GrpcStreamingServer.createCommunity(
            parseInetSocketAddress(config.serverConfig().grpc().address()),
            createGrpcSecurityConfig(config.serverConfig()),
            cursorManager,
            configManager,
            meterRegistry,
            executorManager,
            indexCatalog,
            initializedIndexCatalog,
            quarantinedIndexLookup,
            metadata,
            healthManager,
            DEFAULT_BSON_SIZE_SOFT_LIMIT,
            DEFAULT_MESSAGE_SIZE_LIMIT,
            metadataService,
            catalogAccessGuard,
            internalListAllIndexesForTesting,
            indexDefinitionConfig,
            embeddingServiceManagerSupplier);

    return new ServerLifecycles(
        grpcServer::start,
        () -> {
          grpcServer.close();

          // Close the executors after the servers have finished closing.
          executorManager.close();
        },
        List.of(grpcServer));
  }

  private record ServerLifecycles(Runnable start, Runnable stop, List<CommandServer> servers) {}

  record MetricsLifecycles(Runnable start, Runnable stop) {}

  private static PeriodicConfigMonitor configMonitor(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      MongoDbMetadataClient mongoDbMetadataClient,
      ConfigManager configManager,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry,
      SyncSourceConfig initialSyncSourceConfig,
      InitialSyncHostProvider initialSyncHostProvider,
      CatalogAccessGuard catalogAccessGuard) {
    var configUpdater =
        new CommunityConfigUpdater(
            authoritativeIndexCatalog,
            mongoDbMetadataClient,
            configManager,
            featureFlags,
            initialSyncSourceConfig,
            initialSyncHostProvider,
            catalogAccessGuard);
    return PeriodicConfigMonitor.create(configUpdater, DEFAULT_CONFIG_UPDATE_PERIOD, meterRegistry);
  }

  private static BsonValue serializeHostInfo() {
    return new BsonDocument()
        .append("hostName", new BsonString(getLocalHostName()))
        .append("numCpus", new BsonInt32(Runtime.INSTANCE.getNumCpus()));
  }

  private static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "unknown";
    }
  }

  private static BsonValue serializeBuildInfo(Path configPath, String mongotVersion) {
    List<String> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
    List<BsonString> bsonJvmArgs =
        jvmArgs.stream().map(BsonString::new).collect(Collectors.toList());

    return new BsonDocument()
        .append("configPath", new BsonString(configPath.toString()))
        .append("mongotVersion", new BsonString(mongotVersion))
        .append("jvmArgs", new BsonArray(bsonJvmArgs));
  }

  private static void reportUnknownFields(List<BsonParseException> unknownFieldExceptions) {
    if (unknownFieldExceptions.isEmpty()) {
      return;
    }
    String unknownFields =
        unknownFieldExceptions.stream()
            .map(Throwable::getMessage)
            .collect(Collectors.joining("; "));
    LOG.atWarn().log("Ignoring unrecognized field(s) in mongot config file: {}", unknownFields);
  }

  private MongotConfigs getMongotConfigs(CommunityConfig config) {
    var luceneConfig =
        LuceneConfigMapper.toLuceneConfig(
            config,
            Runtime.INSTANCE,
            Optional.of(
                new HysteresisConfig(
                    config.diskMonitorConfig().resumeReplicationThreshold(),
                    config.diskMonitorConfig().pauseReplicationThreshold())));

    var globalReplicationConfig =
        MongoDbReplicationConfigMapper.toGlobalReplicationConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    var replicationConfig =
        MongoDbReplicationConfigMapper.toMongoDbReplicationConfig(
            globalReplicationConfig,
            Runtime.INSTANCE,
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    var initialSyncConfig =
        InitialSyncConfigMapper.toInitialSyncConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    var durabilityConfig =
        DurabilityConfig.create(
            Optional.empty(),
            config
                .durabilityConfig()
                .flatMap(CommunityDurabilityConfig::commitIntervalMs)
                .map(Duration::ofMillis));

    var cursorConfig =
        CursorConfigMapper.toCursorConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::cursorConfig));

    var indexDefinitionConfig =
        IndexDefinitionConfig.create(
            config
                .advancedConfigs()
                .flatMap(AdvancedConfigs::indexingConfig)
                .flatMap(CommunityIndexingConfig::definitionConfig)
                .flatMap(d -> d.maxEmbeddedDocumentsNestingLevel()));
    var lifecycleConfig = LifecycleConfig.getDefault();
    var environmentVariantPerfConfig = EnvironmentVariantPerfConfig.getDefault();
    var regularBlockingRequestSettings =
        SearchQueryAdmissionControlConfigMapper.createRegularBlockingRequestSettings(
            config.advancedConfigs().flatMap(AdvancedConfigs::regularBlockingRequestConfig));

    var autoEmbeddingMaterializedViewConfig =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            globalReplicationConfig, config);
    return new MongotConfigs(
        luceneConfig,
        replicationConfig,
        initialSyncConfig,
        durabilityConfig,
        cursorConfig,
        indexDefinitionConfig,
        lifecycleConfig,
        this.featureFlags,
        environmentVariantPerfConfig,
        regularBlockingRequestSettings,
        autoEmbeddingMaterializedViewConfig);
  }
}
