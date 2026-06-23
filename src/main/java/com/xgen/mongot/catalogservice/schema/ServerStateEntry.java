package com.xgen.mongot.catalogservice.schema;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record ServerStateEntry(
    ObjectId serverId, String serverName, Instant lastHeartbeatTs, boolean ready, boolean shutdown)
    implements DocumentEncodable {

  private static final Duration EXPIRED_SERVER_DURATION = Duration.ofHours(2);
  private static final Duration READINESS_STATE_EXPIRE_TIME = Duration.ofMinutes(15);

  public ServerStateEntry(ObjectId serverId, String serverName, Instant lastHeartbeatTs) {
    this(serverId, serverName, lastHeartbeatTs, false /* ready */, false /* shutdown */);
  }

  public ServerStateEntry {
    Objects.requireNonNull(serverId);
    Objects.requireNonNull(serverName);
    Objects.requireNonNull(lastHeartbeatTs);
  }

  abstract static class Fields {
    static final Field.Required<ObjectId> ID = Field.builder("_id").objectIdField().required();

    static final Field.Required<String> SERVER_NAME =
        Field.builder("serverName").stringField().required();

    static final Field.Required<BsonDateTime> LAST_HEARTBEAT_TS =
        Field.builder("lastHeartbeatTs").bsonDateTimeField().required();

    static final Field.WithDefault<Boolean> READY =
        Field.builder("ready").booleanField().optional().withDefault(false);

    static final Field.WithDefault<Boolean> SHUTDOWN =
        Field.builder("shutdown").booleanField().optional().withDefault(false);
  }

  /**
   * Generates a doc to filter ServerStateEntries based on the unique server-id for the current
   * server.
   */
  public static BsonDocument serverIdFilter(ObjectId serverId) {
    return Filters.eq(Fields.ID.getName(), serverId).toBsonDocument();
  }

  /**
   * Filters for active servers defined by having heartbeated since the {@link
   * #EXPIRED_SERVER_DURATION} and not having the shutdown field set.
   */
  public static BsonDocument activeServersFilter() {
    return Filters.and(
            Filters.gt(
                Fields.LAST_HEARTBEAT_TS.getName(), Instant.now().minus(EXPIRED_SERVER_DURATION)),
            Filters.or(
                Filters.eq(Fields.SHUTDOWN.getName(), false),
                Filters.eq(Fields.SHUTDOWN.getName(), null)))
        .toBsonDocument();
  }

  /**
   * Filters for stale servers defined by having not heartbeated since the {@link
   * #EXPIRED_SERVER_DURATION}.
   */
  public static BsonDocument staleServerFilter() {
    return Filters.lt(
            Fields.LAST_HEARTBEAT_TS.getName(), Instant.now().minus(EXPIRED_SERVER_DURATION))
        .toBsonDocument();
  }

  /** Creates an update command setting the lastHeartbeatTs to the current time. */
  public static BsonDocument updateHeartbeatTs() {
    return Updates.set(
            Fields.LAST_HEARTBEAT_TS.getName(), new BsonDateTime(Instant.now().toEpochMilli()))
        .toBsonDocument();
  }

  /** Creates an update command setting the shutdown field to the provided status. */
  public static BsonDocument updateShutdownStatus(boolean status) {
    return Updates.set(Fields.SHUTDOWN.getName(), status).toBsonDocument();
  }

  /** Creates an update command setting the ready flag to the provided status. */
  public static BsonDocument updateReadinessStatus(boolean status) {
    return Updates.set(Fields.READY.getName(), status).toBsonDocument();
  }

  /**
   * We maintain the readiness state if the server was previously marked as ready and the last
   * heartbeat was within {@link #READINESS_STATE_EXPIRE_TIME} (i.e. the readiness state has not
   * expired).
   *
   * <p>If a server was down for an extended period of time (indicated by lack of heartbeats) we
   * reset the readiness probe to avoid adding this server to the LB serving stale data before
   * refreshing the indexes.
   */
  public boolean shouldMaintainReadinessState() {
    return this.ready && !isReadinessStateExpired();
  }

  /**
   * Returns true if no heartbeat has been recorded within {@link #READINESS_STATE_EXPIRE_TIME},
   * i.e. the stored readiness state should be treated as expired.
   */
  public boolean isReadinessStateExpired() {
    return this.lastHeartbeatTs.isBefore(Instant.now().minus(READINESS_STATE_EXPIRE_TIME));
  }

  public static ServerStateEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new ServerStateEntry(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.SERVER_NAME).unwrap(),
        Instant.ofEpochMilli(parser.getField(Fields.LAST_HEARTBEAT_TS).unwrap().getValue()),
        parser.getField(Fields.READY).unwrap(),
        parser.getField(Fields.SHUTDOWN).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.serverId)
        .field(Fields.SERVER_NAME, this.serverName)
        .field(Fields.LAST_HEARTBEAT_TS, new BsonDateTime(this.lastHeartbeatTs.toEpochMilli()))
        .field(Fields.READY, this.ready)
        .field(Fields.SHUTDOWN, this.shutdown)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    ServerStateEntry entry = (ServerStateEntry) o;

    return Objects.equals(entry.serverId, this.serverId)
        && Objects.equals(entry.serverName, this.serverName)
        && Objects.equals(entry.lastHeartbeatTs.toEpochMilli(), this.lastHeartbeatTs.toEpochMilli())
        && Objects.equals(entry.ready, this.ready)
        && Objects.equals(entry.shutdown, this.shutdown);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.serverId,
        this.serverName,
        this.lastHeartbeatTs.toEpochMilli(),
        this.ready,
        this.shutdown);
  }
}
