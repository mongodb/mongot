package com.xgen.testing.mongot.server.grpc;

import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.auth.SecurityConfig;
import com.xgen.mongot.server.command.search.SearchCommandsRegister;
import com.xgen.mongot.server.command.search.definition.request.GetMoreCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.KillCursorsCommandDefinition;
import com.xgen.mongot.server.executors.ExecutorManager;
import com.xgen.mongot.server.grpc.GrpcStreamingServer;
import com.xgen.mongot.server.message.MessageHeader;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.MessageSection;
import com.xgen.mongot.server.message.MessageSectionBody;
import com.xgen.mongot.server.message.OpCode;
import com.xgen.mongot.server.util.NettyUtil;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.server.ClientAndServer;
import com.xgen.testing.mongot.server.Mocks;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcClientAndServer implements ClientAndServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcClientAndServer.class);
  private final NettyUtil.SocketType socketType;
  private final TlsMode tlsMode;
  private final Optional<SecurityConfig> securityConfig;
  private final StreamCommand streamCommand;
  private GrpcStreamingServer server;
  private GrpcStreamingClient client;
  private ExecutorManager executorManager;

  public enum ServiceMethod {
    AUTHENTICATED_COMMAND,
    UNAUTHENTICATED_COMMAND,
    BSON_SEARCH,
    BSON_VECTOR_SEARCH
  }

  public GrpcClientAndServer(
      NettyUtil.SocketType socketType,
      Optional<SecurityConfig> securityConfig,
      ServiceMethod method) {
    this.socketType = socketType;
    this.securityConfig = securityConfig;
    if (securityConfig.isPresent()) {
      this.tlsMode = securityConfig.get().tlsMode();
    } else {
      this.tlsMode = TlsMode.DISABLED;
    }
    this.streamCommand =
        method == ServiceMethod.BSON_SEARCH || method == ServiceMethod.BSON_VECTOR_SEARCH
            ? new GrpcClientAndServer.BsonGrpcStreamCommand(method)
            : new GrpcClientAndServer.MessageMessageGrpcStreamCommand(method);
  }

  @Override
  public void start(Mocks mocks) {
    startWithoutHealthCheck(mocks, Optional.empty());
    waitUntilServerHealthy();
  }

  @Override
  public BsonDocument runCommand(BsonDocument command) {
    BsonDocument reply = this.streamCommand.run(command, this.client);
    if (reply.get("ok").asNumber().intValue() == 0) {
      throw new RuntimeException(reply.get("errmsg").asString().getValue());
    }
    return reply;
  }

  @Override
  public void shutdownClient() {
    this.client.close();
  }

  @Override
  public void shutdownServer() {
    this.server.close();
    this.executorManager.close();
  }

  public HealthCheckResponse.ServingStatus checkHealth() {
    return this.client.checkHealth();
  }

  public SocketAddress getServerAddress() {
    Optional<SocketAddress> address = this.server.getAddress();
    if (!address.isPresent()) {
      throw new RuntimeException("Server address is not set. Did you start the server?");
    }
    return address.get();
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public void waitUntilServerHealthy() {
    while (true) {
      try {
        if (this.client.checkHealth().equals(HealthCheckResponse.ServingStatus.SERVING)) {
          break;
        }
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.UNAVAILABLE) {
          throw e;
        }
        LOG.warn("gRPC server is not available, retrying ...", e);
      } catch (Exception e) {
        LOG.warn("gRPC server is not healthy yet, retrying ...", e);
      }

      LOG.info("Waiting for gRPC server healthy ...");
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public void startWithoutHealthCheck(
      Mocks mocks, Optional<SearchEnvoyMetadata> searchEnvoyMetadata) {
    SocketAddress socketAddress = createSocketAddress(mocks);
    this.executorManager = new ExecutorManager(mocks.meterRegistry);
    this.server =
        GrpcStreamingServer.create(
            socketAddress,
            this.securityConfig,
            mocks.cursorManager,
            mocks.configManager,
            mocks.meterRegistry,
            this.executorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "testVersion",
                "localhost",
                () -> MongoDbServerInfo.EMPTY,
                FeatureFlags.getDefault(),
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            CursorConfig.DEFAULT_MESSAGE_SIZE_LIMIT,
            mocks.embeddingServiceManagerSupplier);
    this.server.start();
    this.client =
        GrpcStreamingClient.create(
            getServerAddress(), searchEnvoyMetadata, this.tlsMode, Optional.empty());
  }

  // NB: when using TCP sockets this will use port 0 to auto-assign the bind port; callers will
  // need to obtain the address from the server to get the actual port.
  private SocketAddress createSocketAddress(Mocks mocks) {
    switch (this.socketType) {
      case TCP -> {
        return new InetSocketAddress("127.0.0.1", 0);
      }
      case UNIX_DOMAIN -> {
        try {
          return new DomainSocketAddress(mocks.temporaryFolder.newFile("grpc.sock"));
        } catch (IOException e) {
          throw new RuntimeException("failed to create tmp file", e);
        }
      }
    }
    return Check.unreachable();
  }

  private interface StreamCommand {
    BsonDocument run(BsonDocument command, GrpcStreamingClient client);
  }

  private static class MessageMessageGrpcStreamCommand implements StreamCommand {
    private Optional<GrpcStreamingClient.Stream<MessageMessage>> grpcStream;
    private final ServiceMethod method;

    public MessageMessageGrpcStreamCommand(GrpcClientAndServer.ServiceMethod method) {
      this.method = method;
      this.grpcStream = Optional.empty();
    }

    @Override
    public BsonDocument run(BsonDocument command, GrpcStreamingClient client) {

      if (this.grpcStream.isEmpty()
          || (!command.getFirstKey().equals(GetMoreCommandDefinition.NAME)
              && !command.getFirstKey().equals(KillCursorsCommandDefinition.NAME))) {
        this.grpcStream.ifPresent(stream -> stream.close());
        this.grpcStream =
            Optional.of(
                switch (this.method) {
                  case AUTHENTICATED_COMMAND -> client.startAuthenticatedCommandStream();
                  case UNAUTHENTICATED_COMMAND -> client.startUnauthenticatedCommandStream();
                  default -> {
                    throw new IllegalStateException();
                  }
                });
      }

      List<MessageSection> sections = new ArrayList<>();
      MessageSectionBody section = new MessageSectionBody(command);
      sections.add(section);
      MessageMessage opMsg =
          new MessageMessage(new MessageHeader(0, 10001, 0, OpCode.MSG), 0, sections);
      MessageMessage replyMsg = this.grpcStream.get().handleMessage(opMsg);
      Assert.assertEquals(replyMsg.getHeader().responseTo(), opMsg.getHeader().requestId());
      return ((MessageSectionBody) replyMsg.sections().get(0)).body;
    }
  }

  private static class BsonGrpcStreamCommand implements StreamCommand {
    private final ServiceMethod method;
    private Optional<GrpcStreamingClient.Stream<RawBsonDocument>> grpcStream;

    public BsonGrpcStreamCommand(GrpcClientAndServer.ServiceMethod method) {
      this.method = method;
      this.grpcStream = Optional.empty();
    }

    @Override
    public BsonDocument run(BsonDocument command, GrpcStreamingClient client) {
      if (this.grpcStream.isEmpty()
          || (!command.getFirstKey().equals(GetMoreCommandDefinition.NAME)
              && !command.getFirstKey().equals(KillCursorsCommandDefinition.NAME))) {
        this.grpcStream.ifPresent(stream -> stream.close());
        this.grpcStream =
            switch (this.method) {
              case BSON_SEARCH -> Optional.of(client.startBsonSearchCommandStream());
              case BSON_VECTOR_SEARCH -> Optional.of(client.startBsonVectorSearchCommandStream());
              default -> throw new IllegalStateException();
            };
      }

      RawBsonDocument reply =
          this.grpcStream
              .get()
              .handleMessage(new RawBsonDocument(command, new BsonDocumentCodec()));

      return reply;
    }
  }
}
