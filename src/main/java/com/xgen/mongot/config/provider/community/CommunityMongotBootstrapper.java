package com.xgen.mongot.config.provider.community;

import static com.xgen.mongot.cursor.CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT;
import static com.xgen.mongot.cursor.CursorConfig.DEFAULT_MESSAGE_SIZE_LIMIT;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.mongodb.ConnectionString;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.DefaultMetadataService;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.provider.CommonUtils;
import com.xgen.mongot.config.provider.MongotConfigs;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingServiceManagerConfig;
import com.xgen.mongot.config.provider.monitor.PeriodicConfigMonitor;
import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.logging.Logging;
import com.xgen.mongot.metrics.Instrumentation;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
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
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.server.auth.SecurityConfig;
import com.xgen.mongot.server.command.search.SearchCommandsRegister;
import com.xgen.mongot.server.command.search.SearchCommandsRegister.BootstrapperMetadata;
import com.xgen.mongot.server.executors.ExecutorManager;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import com.xgen.mongot.server.grpc.GrpcStreamingServer;
import com.xgen.mongot.server.grpc.HealthManager;
import com.xgen.mongot.server.http.HealthCheckServer;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.GlobalMetricFactory;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.MongotVersionResolver;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.Shutdown;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.mongodb.ConnectionStringBuilder;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.SslContextFactory;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.List;
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

public class CommunityMongotBootstrapper {

  public static final Logger LOG = LoggerFactory.getLogger(CommunityMongotBootstrapper.class);

  private static final List<PosixFilePermission> POSIX_OWNER_PERMISSIONS =
      List.of(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);

  private static final double DISK_USAGE_PAUSE_REPLICATION_THRESHOLD = 0.9;
  private static final double DISK_USAGE_RESUME_REPLICATION_THRESHOLD = 0.85;
  // Never crash in community.
  private static final double CRASH_THRESHOLD = 1.01;

  private static final Duration DEFAULT_CONFIG_UPDATE_PERIOD = Duration.ofSeconds(1);

  /**
   * Bootstraps the mongot using the config file from the supplied configPath.
   *
   * <p>This method does not return until all mongot subsystems have been configured and started.
   */
  public static void bootstrap(Path configPath, boolean internalListAllIndexesForTesting) {

    CommunityConfig config =
        Crash.because("failed to parse config file")
            .ifThrows(() -> CommunityConfig.readFromFile(configPath));

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
            });

    // Unlike production mode, we expect that the path to the data dir may not exist yet
    Crash.because("failed to create data directory")
        .ifThrows(() -> Files.createDirectories(config.storageConfig().dataPath()));

    // The server-id is stored to disk so we need the datapath to exist before fetching it.
    var serverInfo = new CommunityServerInfo(config);
    LOG.atInfo().addKeyValue("serverInfo", serverInfo).log("Starting server with server info");

    var syncSourceConfig = syncSourceConfig(config.syncSourceConfig());
    var indexCatalog = new DefaultIndexCatalog();
    var initializedIndexCatalog = new InitializedIndexCatalog();

    var mongotVersion = MongotVersionResolver.create().getVersion();
    var mongotConfigs = getMongotConfigs(config.storageConfig().dataPath());

    // Initialize global feature flags for utility classes
    LoggableIdUtils.initialize(
        mongotConfigs.featureFlags.isEnabled(Feature.LOGGABLE_DOCUMENT_ID));

    // metrics

    var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithCompositeMeterRegistry();
    var meterRegistry = meterAndFtdcRegistry.getCompositeMeterRegistry();

    var prometheusServerOptional =
        config.metricsConfig().flatMap(CommunityMongotBootstrapper::maybeStartPrometheusServer);
    prometheusServerOptional.ifPresent(
        (promServer) -> meterRegistry.add(promServer.getPrometheusMeterRegistry()));

    meterRegistry.add(meterAndFtdcRegistry.ftdcRegistry());
    var ftdcReporterLifecycle =
        maybeCreateFtdcReporter(
            meterRegistry,
            meterAndFtdcRegistry.ftdcRegistry(),
            config,
            mongotConfigs,
            mongotVersion);

    // Initialize global metrics for community
    GlobalMetricFactory.initialize(meterRegistry);
    var systemMetricsInstrumentation = maybeStartSystemMetrics(config, meterRegistry);

    var cursorManager =
        MongotCursorManagerImpl.fromConfig(
            CursorConfig.getDefault(), meterRegistry, indexCatalog, initializedIndexCatalog);

    var diskMonitor =
        PeriodicDiskMonitor.createAndStart(
            config.storageConfig().dataPath(), CRASH_THRESHOLD, meterRegistry);
    var replicationGateConfig =
        new HysteresisConfig(
            DISK_USAGE_RESUME_REPLICATION_THRESHOLD, DISK_USAGE_PAUSE_REPLICATION_THRESHOLD);
    // Never pause initial syncs in community.
    Optional<HysteresisConfig> initialSyncConfig = Optional.empty();
    var replicationStateMonitor =
        ReplicationStateMonitor.diskBased(replicationGateConfig, initialSyncConfig, diskMonitor);

    var mongoDbMetadataClient =
        new MongoDbMetadataClient(Optional.of(syncSourceConfig), meterRegistry);
    var commandRegisterMetadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            mongotVersion, "mongot-community", mongoDbMetadataClient, mongotConfigs.featureFlags);

    // Initialize the community metadata service
    var metadataService = initializeMetadataService(syncSourceConfig, meterRegistry);

    // Initialize embedding service if configured
    Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier =
        initializeEmbeddingService(config, mongotConfigs, meterRegistry, commandRegisterMetadata);

    // Create the ConfigManager and initialize it. This initializes all the indexes and begins
    // replication.
    boolean isAutoEmbeddingViewWriter =
        config.embeddingConfig().map(ec -> ec.isAutoEmbeddingViewWriter()).orElse(false);
    // Community Edition doesn't have dynamic feature flags from a conf call, so use empty defaults
    var dynamicFeatureFlagRegistry =
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    var configManager =
        configManager(
            config.storageConfig().dataPath(),
            mongotConfigs,
            indexCatalog,
            initializedIndexCatalog,
            cursorManager,
            syncSourceConfig,
            meterAndFtdcRegistry,
            diskMonitor,
            replicationStateMonitor,
            mongotConfigs.featureFlags,
            dynamicFeatureFlagRegistry,
            embeddingServiceManagerSupplier,
            isAutoEmbeddingViewWriter);

    // Create different servers that will service $search requests from the mongod, as
    // well as some other requests (e.g. some from the proxy), but do not start them yet.
    var healthManager = new HealthManager(configManager, meterRegistry);
    var healthCheckServer = createHealthCheckServer(config, healthManager);
    var serverLifecycles =
        servers(
            config,
            cursorManager,
            configManager,
            indexCatalog,
            initializedIndexCatalog,
            commandRegisterMetadata,
            mongotConfigs.regularBlockingRequestSettings,
            meterRegistry,
            healthManager,
            metadataService.getAuthoritativeIndexCatalog(),
            internalListAllIndexesForTesting,
            embeddingServiceManagerSupplier);

    // Create a PeriodicConfigMonitor that will regularly check the authoritative catalog for
    // updates, but do not start it until the shutdown hook is registered.
    var configMonitor =
        configMonitor(
            metadataService.getAuthoritativeIndexCatalog(),
            mongoDbMetadataClient,
            configManager,
            mongotConfigs.featureFlags,
            meterRegistry);

    // Register shutdown hook prior to starting the server.
    // Close the server to cleanly finish any in-flight requests.
    // Close the logger last to flush any final logs from the server.
    Shutdown.registerHook(
        healthManager::enterTerminalState,
        serverLifecycles.stop,
        () -> systemMetricsInstrumentation.ifPresent(SystemMetricsInstrumentation::stop),
        () -> prometheusServerOptional.ifPresent(PrometheusServer::shutdown),
        () -> ftdcReporterLifecycle.ifPresent(f -> f.stop.run()),
        configManager::close,
        healthCheckServer::stop,
        diskMonitor::stop,
        metadataService::close,
        Logging::shutdown);

    // Start our background processes.
    configMonitor.start();
    serverLifecycles.start.run();
    ftdcReporterLifecycle.ifPresent(f -> f.start.run());

    // Health check server should be started last, to minimize the chance that we service health
    // check requests on startup that immediately return unhealthy.
    healthCheckServer.start();
  }

  private static SyncSourceConfig syncSourceConfig(
      com.xgen.mongot.config.provider.community.SyncSourceConfig communitySyncSourceConfig) {
    String replicaSetPassword =
        Crash.because("failed to read replicaSet.passwordFile")
            .ifThrows(() -> readSecretFile(communitySyncSourceConfig.replicaSet().passwordFile()));

    var replicaSetConfig = communitySyncSourceConfig.replicaSet();
    ConnectionString replicaSetConnectionString =
        Crash.because("failed to construct replicaSet connection string")
            .ifThrows(
                () ->
                    ConnectionStringBuilder.standard()
                        .withHostAndPorts(replicaSetConfig.hostandPorts())
                        .withAuthenticationCredentials(
                            replicaSetConfig.username(), replicaSetPassword)
                        .withAuthenticationDatabase(replicaSetConfig.authSource())
                        .withOption("tls", Boolean.toString(replicaSetConfig.tls()))
                        .withOption(
                            "readPreference",
                            replicaSetConfig.readPreference().asReadPreference().getName())
                        // Per spec, this should be false by default, however, it is not: JAVA-4257
                        .withOption("directConnection", "false")
                        .build());

    Optional<ConnectionString> routerConnectionString =
        communitySyncSourceConfig
            .router()
            .map(
                routerConfig -> {
                  String routerPassword =
                      Crash.because("failed to read router.passwordFile")
                          .ifThrows(() -> readSecretFile(routerConfig.passwordFile()));

                  return Crash.because("failed to construct router connection string")
                      .ifThrows(
                          () ->
                              ConnectionStringBuilder.standard()
                                  .withHostAndPorts(routerConfig.hostandPorts())
                                  .withAuthenticationCredentials(
                                      routerConfig.username(), routerPassword)
                                  .withAuthenticationDatabase(routerConfig.authSource())
                                  .withOption("tls", Boolean.toString(routerConfig.tls()))
                                  .withOption(
                                      "readPreference",
                                      routerConfig.readPreference().asReadPreference().getName())
                                  .withOption("directConnection", "false")
                                  .build());
                });

    Optional<SSLContext> sslContext =
        communitySyncSourceConfig
            .caFile()
            .flatMap(caFile -> Optional.of(SslContextFactory.getWithCaFile(caFile)));
    return new SyncSourceConfig(
        replicaSetConnectionString, routerConnectionString, replicaSetConnectionString, sslContext);
  }

  private static String readSecretFile(Path filePath) throws IOException {
    Check.checkArg(filePath.toFile().exists(), "Secret file %s does not exist", filePath);
    Check.checkArg(filePath.toFile().isFile(), "Secret file %s is not a file", filePath);

    try {
      var filePermissions = Files.getPosixFilePermissions(filePath);

      POSIX_OWNER_PERMISSIONS.forEach(filePermissions::remove);
      Check.checkArg(
          filePermissions.isEmpty(),
          "Secret file %s permissions are too permissive (must only be readable by owner)",
          filePath);
    } catch (UnsupportedOperationException ignored) {
      // Not all filesystems support POSIX permissions, we won't verify file permissions on those
      // filesystems as the number of edge cases increase significantly.
    }

    return Files.readString(filePath);
  }

  private static Optional<PrometheusServer> maybeStartPrometheusServer(
      MetricsConfig metricsConfig) {
    if (!metricsConfig.enabled()) {
      LOG.info("Not starting Prometheus endpoint since it was explicitly disabled");
      return Optional.empty();
    }

    var address = CommunityMongotBootstrapper.parseInetSocketAddress(metricsConfig.address());
    return Optional.of(PrometheusServer.start(address, List.of(), List.of()));
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

  static Optional<MetricsLifecycles> maybeCreateFtdcReporter(
      CompositeMeterRegistry executorRegistry,
      MeterRegistry reportingRegistry,
      CommunityConfig communityConfig,
      MongotConfigs mongotConfigs,
      String mongotVersion) {

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
            .ifThrows(() -> Ftdc.initialize(ftdcConfig, metadata));
    // If feature flag is enabled, use CompositeMeterRegistry so executor metrics are exported to
    // both FTDC itself and Prometheus. Otherwise, use ftdcRegistry to keep executor metrics only
    // in FTDC.
    boolean useCombinedRegistryForExecutorMetrics =
        mongotConfigs.featureFlags.isEnabled(Feature.FTDC_EXECUTOR_METRICS_TO_PROMETHEUS);
    var reporter =
        FtdcScheduledReporter.create(
            reportingRegistry, executorRegistry, ftdc, useCombinedRegistryForExecutorMetrics);

    return Optional.of(
        new MetricsLifecycles(
            () ->
                reporter.start(
                    communityConfig.ftdcConfig().collectionPeriodMillis(), TimeUnit.MILLISECONDS),
            reporter::stop));
  }

  private static HealthCheckServer createHealthCheckServer(
      CommunityConfig config, HealthManager healthManager) {
    InetSocketAddress address =
        config
            .healthCheckConfig()
            .map(c -> parseInetSocketAddress(c.address()))
            .orElse(new InetSocketAddress("localhost", 8080));
    return HealthCheckServer.create(address, healthManager);
  }

  private static SecurityConfig createGrpcSecurityConfig(ServerConfig config) {
    return SecurityConfig.createAuthDisabled(
        config.getGrpcTlsMode(), config.getGrpcCertificateKeyFile(), config.getGrpcCaFile());
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
                  new EmbeddingClientFactory(meterRegistry),
                  Executors.fixedSizeThreadScheduledExecutor(
                      "embedding-providers", 1, meterRegistry),
                  meterRegistry));
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
                new EmbeddingClientFactory(meterRegistry),
                Executors.fixedSizeThreadScheduledExecutor(
                    "embedding-providers",
                    mongotConfigs.autoEmbeddingMaterializedViewConfig.numIndexingThreads * 2,
                    meterRegistry),
                meterRegistry));
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
      String queryKey = readSecretFile(config.queryKeyFile().get()).trim();
      String indexingKey = readSecretFile(config.indexingKeyFile().get()).trim();

      LOG.info(
          "Loaded Voyage credentials from files: {} and {}",
          config.queryKeyFile().get(),
          config.indexingKeyFile().get());

      return Optional.of(
          new EmbeddingServiceManagerConfig.VoyageCredentials(
              new VoyageEmbeddingCredentials(queryKey),
              new VoyageEmbeddingCredentials(indexingKey)));
    } catch (IOException e) {
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
                        Optional.of(endpoint)),
                    serviceConfig.compatibleModels))
        .toList();
  }

  private static DefaultConfigManager configManager(
      Path dataPath,
      MongotConfigs mongotConfigs,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MongotCursorManager cursorManager,
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      DiskMonitor diskMonitor,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      boolean isAutoEmbeddingViewWriter) {
    var indexFactory =
        CommonUtils.getIndexFactory(
            mongotConfigs.luceneConfig,
            mongotConfigs.featureFlags,
            dynamicFeatureFlagRegistry,
            mongotConfigs.environmentVariantPerfConfig,
            meterAndFtdcRegistry,
            Optional.empty(),
            diskMonitor);

    var leaseManager =
        CommonUtils.getLeaseManager(
            syncSourceConfig, meterAndFtdcRegistry, isAutoEmbeddingViewWriter);
    var materializedViewIndexFactory =
        CommonUtils.getMaterializedViewIndexFactory(
            syncSourceConfig, featureFlags, meterAndFtdcRegistry, leaseManager);
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
            Optional.of(embeddingServiceManagerSupplier));

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
            isAutoEmbeddingViewWriter,
            leaseManager);

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
            replicationStateMonitor.getReplicationGate());

    return DefaultConfigManager.initialize(
        indexCatalog,
        Optional.empty(),
        initializedIndexCatalog,
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
      BootstrapperMetadata metadata,
      RegularBlockingRequestSettings regularBlockingRequestSettings,
      MeterRegistry meterRegistry,
      HealthManager healthManager,
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      boolean internalListAllIndexesForTesting,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    // Create the ExecutorManager that can serve requests for the tcpServer (and the grpcServer).
    // noinspection resource
    var executorManager = new ExecutorManager(meterRegistry, regularBlockingRequestSettings);

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
            metadata,
            healthManager,
            DEFAULT_BSON_SIZE_SOFT_LIMIT,
            DEFAULT_MESSAGE_SIZE_LIMIT,
            authoritativeIndexCatalog,
            internalListAllIndexesForTesting,
            embeddingServiceManagerSupplier);

    return new ServerLifecycles(
        grpcServer::start,
        () -> {
          grpcServer.close();

          // Close the executors after the servers have finished closing.
          executorManager.close();
        });
  }

  private record ServerLifecycles(Runnable start, Runnable stop) {}

  record MetricsLifecycles(Runnable start, Runnable stop) {}

  private static PeriodicConfigMonitor configMonitor(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      MongoDbMetadataClient mongoDbMetadataClient,
      ConfigManager configManager,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry) {
    var configUpdater =
        new CommunityConfigUpdater(
            authoritativeIndexCatalog, mongoDbMetadataClient, configManager, featureFlags);
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

  private static MongotConfigs getMongotConfigs(Path dataPath) {
    var luceneConfig =
        LuceneConfig.create(
            dataPath,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(
                new HysteresisConfig(
                    DISK_USAGE_PAUSE_REPLICATION_THRESHOLD,
                    DISK_USAGE_PAUSE_REPLICATION_THRESHOLD)));

    var replicationConfig = MongoDbReplicationConfig.getDefault();

    var initialSyncConfig = new InitialSyncConfig();

    var durabilityConfig = DurabilityConfig.create(Optional.empty(), Optional.empty());

    var cursorConfig = CursorConfig.getDefault();

    var indexDefinitionConfig = IndexDefinitionConfig.getDefault();
    var lifecycleConfig = LifecycleConfig.getDefault();
    var featureFlags = FeatureFlags.withQueryFeaturesEnabled();
    var environmentVariantPerfConfig = EnvironmentVariantPerfConfig.getDefault();
    var regularBlockingRequestSettings = RegularBlockingRequestSettings.defaults();
    var autoEmbeddingMaterializedViewConfig = AutoEmbeddingMaterializedViewConfig.getDefault();
    return new MongotConfigs(
        luceneConfig,
        replicationConfig,
        initialSyncConfig,
        durabilityConfig,
        cursorConfig,
        indexDefinitionConfig,
        lifecycleConfig,
        featureFlags,
        environmentVariantPerfConfig,
        regularBlockingRequestSettings,
        autoEmbeddingMaterializedViewConfig);
  }
}
