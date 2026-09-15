package com.xgen.mongot.util.mongodb;

import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.RequestContext;
import com.mongodb.client.ClientSession;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.client.internal.ClientSessionBinding;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ServerAddressHelper;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryHelper;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.session.ClientSessionContext;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A custom MongoDB client enabling efficient reads of paged results with a cursor by letting the
 * caller define on result type. The client supports any {@link ReadOperation} type which returns a
 * {@link BatchCursor}.
 *
 * <p>This client was created to enable search efficient reads, selecting a replica to read from as
 * specified by {@link com.mongodb.ReadPreference} in {@link MongoClientSettings}, and exposing
 * shard migrations with `showMigrationEvents` option on change-stream pipeline.
 */
public class BatchMongoClient implements AutoCloseable {

  private final Cluster cluster;
  private final MongoClientSettings settings;
  private final MongoClientImpl internalMongoClient;
  private final AtomicBoolean closed;

  public BatchMongoClient(MongoClientSettings settings) {
    Check.argNotNull(settings, "settings");
    MongoDriverInformation information = createMongoDriverInformation();
    Cluster dbCluster = createCluster(settings, information);

    this.settings = settings;
    this.cluster = dbCluster;
    this.internalMongoClient = createInternalMongoClient(dbCluster, information, settings);
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Creates a cursor for given read operation.
   *
   * @param readOperation A read operation to execute
   * @param session An open session. Must not be null.
   * @return A {@link BatchCursor} of given type
   */
  public <T> BatchCursor<T> openCursor(
      ReadOperation<BatchCursor<T>, ?> readOperation, ClientSession session) {
    return openCursor(readOperation, session, Optional.empty());
  }

  /**
   * Opens a cursor for the given read operation, optionally carrying a per-operation {@code
   * maxTimeMS}.
   *
   * <p>Driver 5.x removed the per-operation {@code AggregateOperation.maxTime()} setter; the max
   * time is now applied centrally from the {@link TimeoutContext} (see {@code
   * CommandHelper.applyMaxTimeMS}). When {@code maxTimeMs} is present we inject it via {@link
   * TimeoutSettings#withMaxTimeMS(long)} so the operation's command carries {@code maxTimeMS} as it
   * did in driver 4.x. When absent, the default {@code TimeoutSettings} is used (its default {@code
   * maxTimeMS} of 0 means "no server-side limit", equivalent to omitting the field).
   *
   * <p>The override only takes effect for callers that pass a non-empty {@code maxTimeMs};
   * currently that is the change-stream cursor path ({@code ChangeStreamMongoCursorClient}).
   * Callers using the two-arg overload are unaffected.
   *
   * @param readOperation A read operation to execute
   * @param session An open session. Must not be null.
   * @param maxTimeMs optional per-operation maxTimeMS to apply to the command
   * @return A {@link BatchCursor} of given type
   */
  public <T> BatchCursor<T> openCursor(
      ReadOperation<BatchCursor<T>, ?> readOperation,
      ClientSession session,
      Optional<Integer> maxTimeMs) {
    TimeoutSettings timeoutSettings = TimeoutSettings.create(this.settings);
    TimeoutSettings effectiveTimeoutSettings =
        maxTimeMs.map(timeoutSettings::withMaxTimeMS).orElse(timeoutSettings);
    OperationContext operationContext =
        new OperationContext(
            resolveRequestContext(),
            new BatchSessionContext(session, this.settings.getReadConcern()),
            new TimeoutContext(effectiveTimeoutSettings),
            this.settings.getServerApi());
    return readOperation.execute(createReadBinding(session), operationContext);
  }

  /**
   * Resolves the {@link RequestContext} from the settings' {@code contextProvider} if one is set,
   * falling back to {@link IgnorableRequestContext#INSTANCE} otherwise. This mirrors the driver 4.x
   * behavior of honoring a configured {@link SynchronousContextProvider}.
   */
  private RequestContext resolveRequestContext() {
    return Optional.ofNullable(this.settings.getContextProvider())
        .map(
            provider -> {
              Check.instanceOf(provider, SynchronousContextProvider.class);
              return ((SynchronousContextProvider) provider).getContext();
            })
        .orElse(IgnorableRequestContext.INSTANCE);
  }

  /** Create a new client session with given options. */
  public ClientSession openSession(ClientSessionOptions options) {
    return this.internalMongoClient.startSession(options);
  }

  /** Returns the {@link MongoClientSettings} used by this client. */
  public MongoClientSettings getSettings() {
    return this.settings;
  }

  @Override
  public void close() {
    if (!this.closed.getAndSet(true)) {
      this.internalMongoClient.close();
      this.cluster.close();
    }
  }

  private MongoDriverInformation createMongoDriverInformation() {
    return MongoDriverInformation.builder().driverName("mongot-sync").build();
  }

  private Cluster createCluster(
      MongoClientSettings settings, MongoDriverInformation driverInformation) {

    return new DefaultClusterFactory()
        .createCluster(
            settings.getClusterSettings(),
            settings.getServerSettings(),
            settings.getConnectionPoolSettings(),
            InternalConnectionPoolSettings.builder().build(),
            TimeoutSettings.create(settings),
            getStreamFactory(settings, false),
            TimeoutSettings.createHeartbeatSettings(settings),
            getStreamFactory(settings, true),
            settings.getCredential(),
            settings.getLoggerSettings(),
            getCommandListener(settings.getCommandListeners()),
            settings.getApplicationName(),
            driverInformation,
            settings.getCompressorList(),
            settings.getServerApi(),
            settings.getDnsClient());
  }

  private StreamFactory getStreamFactory(MongoClientSettings settings, boolean isHeartbeat) {
    SocketSettings socketSettings =
        isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();

    return StreamFactoryHelper.getSyncStreamFactoryFactory(
            settings.getTransportSettings(), ServerAddressHelper.getInetAddressResolver(settings))
        .create(socketSettings, settings.getSslSettings());
  }

  private MongoClientImpl createInternalMongoClient(
      Cluster cluster, MongoDriverInformation driverInformation, MongoClientSettings settings) {

    return new MongoClientImpl(cluster, settings, driverInformation, null);
  }

  /**
   * A {@link ClientSessionContext} backed by the provided {@link ClientSession}, used to ensure
   * that {@code lsid} is attached to all commands (including {@code getMore}) so the session
   * participates in {@code $clusterTime} gossip and causal consistency.
   *
   * <p>In driver 5.7+, the {@link OperationContext} passed to {@code ReadOperation.execute()} is
   * threaded through to all subsequent getMore commands. A NoOp session context here would omit
   * {@code lsid}, which would stop the session's gossiped cluster-time from advancing -- and that
   * cluster-time is what {@code ChangeStreamMongoCursorClient} uses as the replication-lag "now"
   * reference.
   *
   * <p>Note: the server does <em>not</em> return a top-level {@code operationTime} on these
   * change-stream {@code getMore} responses regardless of the session, which is why the lag "now"
   * is taken from {@code $clusterTime} rather than {@code operationTime}.
   */
  private static final class BatchSessionContext extends ClientSessionContext {
    private final ClientSession syncSession;
    private final ReadConcern readConcern;

    BatchSessionContext(ClientSession session, ReadConcern readConcern) {
      super(session);
      this.syncSession = session;
      this.readConcern = readConcern;
    }

    @Override
    public boolean isImplicitSession() {
      return false;
    }

    @Override
    public boolean notifyMessageSent() {
      return this.syncSession.notifyMessageSent();
    }

    @Override
    public boolean hasActiveTransaction() {
      return this.syncSession.hasActiveTransaction();
    }

    @Override
    public ReadConcern getReadConcern() {
      return this.readConcern;
    }
  }

  private ReadBinding createReadBinding(ClientSession session) {
    Check.isNotNull(session, "session");
    ClusterBinding clusterBinding =
        new ClusterBinding(this.cluster, this.settings.getReadPreference());
    return new ClientSessionBinding(session, false /* ownsSession */, clusterBinding);
  }
}
