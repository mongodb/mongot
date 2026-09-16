package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import com.xgen.mongot.util.mongodb.FindCommandResponse;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

/**
 * Issues the source-side reads used by initial sync partition planning, which decides whether a
 * collection scan can be split across several concurrent {@code $natural} cursors.
 *
 * <p>Planning is the best effort, so a read here reports an absent result rather than failing and
 * the planner falls back to a single cursor scan.
 */
public class PartitionPlanningMongoClient {

  private static final String INITIAL_SYNC_ID_FIELD = "$initialSyncId";

  private final MongoClient mongoClient;

  public PartitionPlanningMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  /**
   * Reads the sync source's {@code $initialSyncId}, which identifies the node's copy of the data
   * that a partition's {@code $recordId} bounds are only valid against.
   *
   * <p>The id is scoped to the node, not the collection: every collection on a node reports the
   * same one, and it moves when the node itself is resynced, which is the event that rewrites every
   * {@code $recordId}. A collection dropped and recreated keeps the same id, and is caught instead
   * by the collection UUID that namespace resolution matches on.
   *
   * <p>The server reports it on a resume token rather than through a command of its own, so this
   * runs a one document natural-order find that requests one. The {@code limit} closes the cursor
   * server-side, so no cursor is left open.
   *
   * @param namespace the collection to read
   * @param afterClusterTime the cluster time the read must not precede
   * @return the id, or empty if the sync source reports none. An empty collection returns no resume
   *     token at all, and some storage configurations either omit the field or report it as
   *     BsonNull.
   */
  public Optional<UUID> captureInitialSyncId(
      MongoNamespace namespace, BsonTimestamp afterClusterTime) {
    var command =
        new CollectionScanFindCommand.Builder(namespace.getCollectionName())
            .hint(CollectionScanFindCommand.NATURAL_ORDER)
            .limit(1)
            .requestResumeToken(BsonBoolean.TRUE)
            // Local is safe here because the later collection scan reads at majority,
            // (CollectionScanCursorFactory): a rolled back id fails its check and resyncs,
            // so the cost is a wasted resync rather than a wrong index.
            // TODO(CLOUDP-417359): measure what majority costs under replication lag.
            .readConcern(ReadConcern.LOCAL, afterClusterTime)
            .build();

    var proxy =
        this.mongoClient
            .getDatabase(namespace.getDatabaseName())
            .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
            .runCommand(command.toProxy(), FindCommandResponseProxy.class);

    return FindCommandResponse.fromProxy(proxy)
        .getPostBatchResumeToken()
        .flatMap(PartitionPlanningMongoClient::readInitialSyncId);
  }

  /**
   * Extracts the {@code $initialSyncId} from a post batch resume token, returning empty for any
   * shape that is not a UUID.
   *
   * <p>{@link CollectionScanCommandMongoClient} reads the same field but raises on a BsonNull,
   * which is correct once a scan is underway and wrong here.
   */
  @VisibleForTesting
  static Optional<UUID> readInitialSyncId(BsonDocument postBatchResumeToken) {
    BsonValue initialSyncId = postBatchResumeToken.get(INITIAL_SYNC_ID_FIELD);
    if (initialSyncId == null || !initialSyncId.isBinary()) {
      return Optional.empty();
    }

    BsonBinary binary = initialSyncId.asBinary();
    if (binary.getType() != BsonBinarySubType.UUID_STANDARD.getValue()) {
      return Optional.empty();
    }
    return Optional.of(binary.asUuid());
  }
}
