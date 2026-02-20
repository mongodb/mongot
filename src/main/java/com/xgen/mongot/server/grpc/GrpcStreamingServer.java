package com.xgen.mongot.server.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.server.CommandServer;
import com.xgen.mongot.server.auth.SecurityConfig;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.command.search.SearchCommandsRegister;
import com.xgen.mongot.server.command.search.SearchCommandsRegister.RegistrationMode;
import com.xgen.mongot.server.executors.ExecutorManager;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.util.NettyUtil;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Crash;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import java.io.File;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcStreamingServer implements CommandServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcStreamingServer.class);

  private final SocketAddress address;
  private final Server server;
  private final HealthManager healthManager;
  private volatile ServerStatus status;

  /** GRPC Streaming server class for Community. */
  public static GrpcStreamingServer createCommunity(
      SocketAddress address,
      SecurityConfig securityConfig,
      MongotCursorManager cursorManager,
      ConfigManager configManager,
      MeterRegistry meterRegistry,
      ExecutorManager executorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      HealthManager healthManager,
      Bytes bsonSizeSoftLimit,
      Bytes inboundMessageSizeLimit,
      MetadataService metadataService,
      boolean internalListAllIndexesForTesting,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    CommandRegistry commandRegistry =
        registerCommands(
            meterRegistry,
            cursorManager,
            configManager,
            indexCatalog,
            initializedIndexCatalog,
            metadata,
            bsonSizeSoftLimit,
            embeddingServiceManagerSupplier);

    SearchCommandsRegister.registerIndexManagementCommands(
        RegistrationMode.SECURE,
        commandRegistry,
        metadataService,
        configManager,
        internalListAllIndexesForTesting);

    return create(
        address,
        Optional.of(securityConfig),
        cursorManager,
        executorManager,
        commandRegistry,
        healthManager,
        inboundMessageSizeLimit);
  }

  /** GRPC Streaming server class for mms. Used for testing, too. */
  public static GrpcStreamingServer create(
      SocketAddress address,
      Optional<SecurityConfig> securityConfig,
      MongotCursorManager cursorManager,
      ConfigManager configManager,
      MeterRegistry meterRegistry,
      ExecutorManager executorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Bytes bsonSizeSoftLimit,
      Bytes inboundMessageSizeLimit,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    CommandRegistry commandRegistry =
        registerCommands(
            meterRegistry,
            cursorManager,
            configManager,
            indexCatalog,
            initializedIndexCatalog,
            metadata,
            bsonSizeSoftLimit,
            embeddingServiceManagerSupplier);

    // Initialize the `HealthStatusManager`. It will mark the state as serving after the
    // `configManager` is initialized.
    HealthManager healthManager = new HealthManager(configManager, meterRegistry);
    return create(
        address,
        securityConfig,
        cursorManager,
        executorManager,
        commandRegistry,
        healthManager,
        inboundMessageSizeLimit);
  }

  /** Create a gRPC Streaming server class according to the {@link CommandRegistry}. */
  @VisibleForTesting
  public static GrpcStreamingServer create(
      SocketAddress address,
      Optional<SecurityConfig> securityConfigOpt,
      MongotCursorManager cursorManager,
      ExecutorManager executorManager,
      CommandRegistry commandRegistry,
      HealthManager healthManager,
      Bytes inboundMessageSizeLimit) {
    NettyUtil.SocketType socketType = NettyUtil.getSocketType(address);

    var wireMessageCallHandler =
        ServerCalls.<MessageMessage, MessageMessage>asyncBidiStreamingCall(
            responseStream -> {
              // If we receive a gRPC stream when unhealthy, throws an
              // UNAVAILABLE error so that envoy can retry.
              if (!healthManager.isHealthy()) {
                throw new StatusRuntimeException(Status.UNAVAILABLE);
              }
              return new WireMessageCallHandler(
                  commandRegistry,
                  executorManager.commandExecutor,
                  cursorManager,
                  GrpcContext.SEARCH_ENVOY_METADATA_KEY.get(),
                  responseStream);
            });

    var bsonMessageCallHandler =
        ServerCalls.<RawBsonDocument, RawBsonDocument>asyncBidiStreamingCall(
            responseStream -> {
              if (!healthManager.isHealthy()) {
                throw new StatusRuntimeException(Status.UNAVAILABLE);
              }
              return new BsonMessageCallHandler(
                  commandRegistry,
                  executorManager.commandExecutor,
                  cursorManager,
                  GrpcContext.SEARCH_ENVOY_METADATA_KEY.get(),
                  responseStream);
            });

    TlsMode tlsMode =
        securityConfigOpt.isPresent() ? securityConfigOpt.get().tlsMode() : TlsMode.DISABLED;
    LOG.atInfo().addKeyValue("TlsMode", tlsMode).log("initializing gRPC server");
    NettyServerBuilder serverBuilder =
        NettyServerBuilder.forAddress(address)
            .addService(
                () ->
                    ServerServiceDefinition.builder(CommandStreamMethods.MONGODB_WIRE_SERVICE_NAME)
                        .addMethod(
                            CommandStreamMethods.authenticatedCommandStream, wireMessageCallHandler)
                        .addMethod(
                            CommandStreamMethods.unauthenticatedCommandStream,
                            wireMessageCallHandler)
                        .build())
            .addService(
                () ->
                    ServerServiceDefinition.builder(CommandStreamMethods.MONGODB_BSON_SERVICE_NAME)
                        .addMethod(CommandStreamMethods.searchCommandStream, bsonMessageCallHandler)
                        .addMethod(
                            CommandStreamMethods.vectorSearchCommandStream, bsonMessageCallHandler)
                        .build())
            .addService(healthManager.getHealthService())
            .maxInboundMessageSize((int) inboundMessageSizeLimit.toBytes())
            .intercept(new SearchEnvoyMetadataInterceptor())
            .intercept(new MongoDbGrpcProtocolInterceptor())
            .workerEventLoopGroup(
                executorManager.getEventLoopGroup(
                    socketType, ExecutorManager.EventLoopGroupType.WORKER))
            .bossEventLoopGroup(
                executorManager.getEventLoopGroup(
                    socketType, ExecutorManager.EventLoopGroupType.BOSS))
            .channelType(NettyUtil.getServerChannelType(socketType));
    switch (socketType) {
      case TCP -> {
        serverBuilder.withChildOption(ChannelOption.SO_KEEPALIVE, true);
        // Create the SSL context if TLS is enabled.
        if (tlsMode != TlsMode.DISABLED) {
          serverBuilder.sslContext(createSslContext(securityConfigOpt));
        }
      }
      case UNIX_DOMAIN -> {
        // No additional options when the socket type is UNIX_DOMAIN.
      }
    }
    return new GrpcStreamingServer(address, serverBuilder.build(), healthManager);
  }

  private static SslContext createSslContext(Optional<SecurityConfig> securityConfigOpt) {
    if (securityConfigOpt.isEmpty()) {
      throw new IllegalArgumentException("SecurityConfig is not set, cannot create SSL context");
    }
    SecurityConfig securityConfig = securityConfigOpt.get();
    if (securityConfig.tlsMode() == TlsMode.DISABLED) {
      throw new IllegalArgumentException("TLS mode is disabled, should not create SSL context");
    }
    Optional<Path> certKeyFilePath = securityConfig.certKeyFilePath();
    var sslContextBuilder =
        SslContextBuilder.forServer(getCertFile(certKeyFilePath), getCertFile(certKeyFilePath))
            .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
            .applicationProtocolConfig(
                new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2,
                    ApplicationProtocolNames.HTTP_1_1));
    if (securityConfig.tlsMode() == TlsMode.MTLS) {
      Optional<Path> caFilePath = securityConfig.certAuthFilePath();
      sslContextBuilder.trustManager(getCertFile(caFilePath)).clientAuth(ClientAuth.REQUIRE);
    }
    return Crash.because("Failed to initialize SSL context")
        .ifThrows(() -> sslContextBuilder.build());
  }

  private static File getCertFile(Optional<Path> certFilePath) {
    if (certFilePath.isEmpty()) {
      throw new IllegalArgumentException("certificate file is not set");
    }
    return certFilePath.get().toFile();
  }

  public static CommandRegistry registerCommands(
      MeterRegistry meterRegistry,
      MongotCursorManager cursorManager,
      ConfigManager configManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Bytes bsonSizeSoftLimit,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    CommandRegistry commandRegistry = CommandRegistry.create(meterRegistry);

    SearchCommandsRegister.register(
        RegistrationMode.SECURE,
        commandRegistry,
        cursorManager,
        meterRegistry,
        indexCatalog,
        initializedIndexCatalog,
        metadata,
        bsonSizeSoftLimit,
        embeddingServiceManagerSupplier);

    return commandRegistry;
  }

  private GrpcStreamingServer(SocketAddress address, Server server, HealthManager healthManager) {
    this.address = address;
    this.server = server;
    this.healthManager = healthManager;
    this.status = ServerStatus.NOT_STARTED;
  }

  public void start() {
    LOG.atInfo().addKeyValue("address", this.address).log("Starting gRPC server");
    Crash.because("failed to start gRPC server").ifThrows(this.server::start);
    this.status = ServerStatus.STARTED;
  }

  @Override
  public void close() {
    LOG.info("Shutting down gRPC server.");
    this.healthManager.enterTerminalState();
    Crash.because("failed to shut down gRPC server")
        .ifThrows(
            () -> {
              this.server.shutdown().awaitTermination(60, TimeUnit.SECONDS);
            });
  }

  @Override
  public ServerStatus getServerStatus() {
    return this.status;
  }

  /** Returns the bound address if the server has started. */
  public Optional<SocketAddress> getAddress() {
    var list = this.server.getListenSockets();
    if (list.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(list.getFirst());
    }
  }
}
