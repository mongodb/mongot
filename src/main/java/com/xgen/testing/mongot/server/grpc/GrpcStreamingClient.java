package com.xgen.testing.mongot.server.grpc;

import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.grpc.CommandStreamMethods;
import com.xgen.mongot.server.grpc.GrpcMetadata;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.util.NettyUtil;
import com.xgen.mongot.util.Crash;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import java.io.Closeable;
import java.io.File;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import org.bson.RawBsonDocument;

public class GrpcStreamingClient implements Closeable {
  private final ManagedChannel channel;
  private final EventLoopGroup eventLoopGroup;

  public static final int MONGODB_WIRE_VERSION = 18;

  private static final CallOptions.Key<AtomicReference<Integer>>
      MONGODB_MAX_WIRE_VERSION_OBSERVER_KEY =
          CallOptions.Key.create("mongodbMaxWireVersionObserver");

  /** A util method to append an ascii header. */
  public static void appendAsciiHeader(Metadata metadata, String key, String value) {
    var asciiKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(asciiKey, value);
  }

  /**
   * Create a gRPC streaming client according to the {@link SearchEnvoyMetadata}.
   *
   * <p>Headers for MongoDB gRPC protocol will be added automatically.
   */
  public static GrpcStreamingClient create(
      SocketAddress address,
      Optional<SearchEnvoyMetadata> searchEnvoyMetadata,
      TlsMode tlsMode,
      Optional<Path> certificatePath) {
    Metadata requestHeaders = new Metadata();
    requestHeaders.put(GrpcMetadata.MONGODB_WIRE_VERSION_METADATA_KEY, MONGODB_WIRE_VERSION);
    searchEnvoyMetadata.ifPresent(
        envoyMetadata -> requestHeaders.put(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY, envoyMetadata));
    return create(address, requestHeaders, tlsMode, certificatePath);
  }

  /** Create a gRPC streaming client according to the {@link Metadata}. */
  public static GrpcStreamingClient create(
      SocketAddress address,
      Metadata requestHeaders,
      TlsMode tlsMode,
      Optional<Path> certificatePath) {
    NettyUtil.SocketType socketType = NettyUtil.getSocketType(address);
    EventLoopGroup eventLoopGroup = NettyUtil.createEventLoopGroup(socketType);
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(address)
            .eventLoopGroup(eventLoopGroup)
            .channelType(NettyUtil.getClientChannelType(socketType));
    channelBuilder.negotiationType(
        tlsMode == TlsMode.DISABLED ? NegotiationType.PLAINTEXT : NegotiationType.TLS);
    if (tlsMode != TlsMode.DISABLED) {
      channelBuilder.sslContext(createSslContext(certificatePath));
    }

    channelBuilder.intercept(
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {
              @Override
              public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.merge(requestHeaders);
                super.start(
                    new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                        responseListener) {
                      @Override
                      public void onHeaders(Metadata headers) {
                        Optional.ofNullable(
                                callOptions.getOption(MONGODB_MAX_WIRE_VERSION_OBSERVER_KEY))
                            .ifPresent(
                                versionReference ->
                                    versionReference.set(
                                        headers.get(
                                            GrpcMetadata.MONGODB_MAX_WIRE_VERSION_METADATA_KEY)));
                        super.onHeaders(headers);
                      }
                    },
                    headers);
              }
            };
          }
        });

    return new GrpcStreamingClient(channelBuilder.build(), eventLoopGroup);
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static SslContext createSslContext(Optional<Path> certificatePath) {
    SslContext sslContext;
    try {
      var sslContextBuilder =
          SslContextBuilder.forClient()
              .endpointIdentificationAlgorithm(null)
              // Configure ALPN for HTTP/2 and HTTP/1.1
              .applicationProtocolConfig(
                  new ApplicationProtocolConfig(
                      ApplicationProtocolConfig.Protocol.ALPN, // Use ALPN
                      ApplicationProtocolConfig.SelectorFailureBehavior
                          .NO_ADVERTISE, // Selector behavior
                      ApplicationProtocolConfig.SelectedListenerFailureBehavior
                          .ACCEPT, // Listener behavior
                      ApplicationProtocolNames.HTTP_2, // Preferred HTTP/2
                      ApplicationProtocolNames.HTTP_1_1 // Fallback to HTTP/1.1
                      ))
              // Set the cipher suites (optional, as Netty uses reasonable defaults for clients)
              .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
              .trustManager(getCaCertChainStream());
      sslContextBuilder.keyManager(getClientCert(certificatePath), getClientCert(certificatePath));
      sslContext = sslContextBuilder.build();
    } catch (SSLException e) {
      throw new RuntimeException("SSL Exception creating gRPC client: ", e);
    }

    return sslContext;
  }

  public static File getClientCert(Optional<Path> certificatePath) {
    if (certificatePath.isPresent()) {
      return certificatePath.get().toFile();
    }
    return new File("src/test/integration/resources/server/mongodb-combined.pem");
  }

  public static File getCaCertChainStream() {
    return new File("src/test/integration/resources/server/ca.pem");
  }

  private GrpcStreamingClient(ManagedChannel channel, EventLoopGroup eventLoopGroup) {
    this.channel = channel;
    this.eventLoopGroup = eventLoopGroup;
  }

  public static class Stream<T> implements Closeable {
    private CompletableFuture<T> lastResponseMsg;
    private final CompletableFuture<Status> streamTerminated;
    private final StreamObserver<T> requestObserver;
    private final AtomicReference<Integer> mongodbMaxWireVersionObserver;

    private Stream(ManagedChannel channel, MethodDescriptor<T, T> methodDescriptor) {
      this.streamTerminated = new CompletableFuture<Status>();
      this.mongodbMaxWireVersionObserver = new AtomicReference<>();
      var responseObserver =
          new StreamObserver<T>() {
            @Override
            public void onNext(T message) {
              Stream.this.lastResponseMsg.complete(message);
            }

            @Override
            public void onError(Throwable t) {
              Stream.this.streamTerminated.complete(Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {}
          };
      this.requestObserver =
          ClientCalls.asyncBidiStreamingCall(
              channel.newCall(
                  methodDescriptor,
                  CallOptions.DEFAULT.withOption(
                      MONGODB_MAX_WIRE_VERSION_OBSERVER_KEY, this.mongodbMaxWireVersionObserver)),
              responseObserver);
    }

    public Optional<Integer> getMongodbMaxWireVersionObserver() {
      return Optional.ofNullable(this.mongodbMaxWireVersionObserver.get());
    }

    public T handleMessage(T msg) throws StatusRuntimeException {
      Stream.this.lastResponseMsg = new CompletableFuture<T>();
      this.requestObserver.onNext(msg);
      CompletableFuture.anyOf(Stream.this.lastResponseMsg, Stream.this.streamTerminated).join();
      if (Stream.this.lastResponseMsg.isDone()) {
        return Stream.this.lastResponseMsg.join();
      } else {
        throw Stream.this.streamTerminated.join().asRuntimeException();
      }
    }

    @Override
    public void close() {
      this.requestObserver.onCompleted();
    }
  }

  public Stream<MessageMessage> startAuthenticatedCommandStream() {
    return new Stream<MessageMessage>(
        this.channel, CommandStreamMethods.authenticatedCommandStream);
  }

  public Stream<MessageMessage> startUnauthenticatedCommandStream() {
    return new Stream<MessageMessage>(
        this.channel, CommandStreamMethods.unauthenticatedCommandStream);
  }

  public Stream<RawBsonDocument> startBsonSearchCommandStream() {
    return new Stream<RawBsonDocument>(this.channel, CommandStreamMethods.searchCommandStream);
  }

  public Stream<RawBsonDocument> startBsonVectorSearchCommandStream() {
    return new Stream<RawBsonDocument>(
        this.channel, CommandStreamMethods.vectorSearchCommandStream);
  }

  // We cannot reference this method directly because we don't have bazel visibility of
  // `@io_grpc_grpc_java//services:_health_java_grpc`.
  private static final MethodDescriptor<HealthCheckRequest, HealthCheckResponse>
      HEALTH_CHECK_METHOD_DESCRIPTOR =
          MethodDescriptor.<HealthCheckRequest, HealthCheckResponse>newBuilder()
              .setType(MethodDescriptor.MethodType.UNARY)
              .setRequestMarshaller(ProtoUtils.marshaller(HealthCheckRequest.getDefaultInstance()))
              .setResponseMarshaller(
                  ProtoUtils.marshaller(HealthCheckResponse.getDefaultInstance()))
              .setFullMethodName("grpc.health.v1.Health/Check")
              .build();

  public HealthCheckResponse.ServingStatus checkHealth() {
    return ClientCalls.blockingUnaryCall(
            this.channel.newCall(HEALTH_CHECK_METHOD_DESCRIPTOR, CallOptions.DEFAULT),
            HealthCheckRequest.getDefaultInstance())
        .getStatus();
  }

  /** Calls the unary {@code mongodb.BsonCommandService/manageSearchIndex} method. */
  public RawBsonDocument callManageSearchIndexUnary(RawBsonDocument request) {
    return ClientCalls.blockingUnaryCall(
        this.channel.newCall(CommandStreamMethods.manageSearchIndexUnary, CallOptions.DEFAULT),
        request);
  }

  @Override
  public void close() {
    Crash.because("failed to shut down gRPC client")
        .ifThrows(
            () -> {
              this.channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
              this.eventLoopGroup.shutdownGracefully().await(30, TimeUnit.SECONDS);
            });
  }
}
