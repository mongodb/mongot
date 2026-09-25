package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Projections;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import com.xgen.mongot.util.mongodb.FindCommandResponse;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

/**
 * Issues the source-side reads used by initial sync partition planning, which decides whether a
 * collection scan can be split across several concurrent {@code $natural} cursors.
 *
 * <p>Planning is the best effort, so a read here reports an absent result rather than failing and
 * the planner falls back to a single cursor scan.
 */
public class PartitionPlanningMongoClient {

  private static final String INITIAL_SYNC_ID_FIELD = "$initialSyncId";
  private static final String RECORD_ID_FIELD = "$recordId";
  private static final String ID_FIELD = "_id";

  /**
   * Request budget for one resolve. {@code _id} has no size limit of its own. This ensure that
   * request is below the command's 16MB.
   */
  private static final Bytes RESOLVE_PAYLOAD_BUDGET = Bytes.ofMebi(5);

  /**
   * Batch size and limit for one resolve, which has to be at least the caller's sample size for a
   * request to come back in a single batch. A caller that samples more than this leaves a cursor
   * open, which the check below turns into a fallback rather than a short sample.
   */
  private static final int RESOLVE_BATCH_SIZE = 1000;

  /** Returns the {@code $recordId} that {@code showRecordId} adds, plus a constant {@code _id}. */
  private static final Bson RECORD_ID_PROJECTION =
      Projections.computed(ID_FIELD, new BsonDocument("$literal", new BsonInt32(1)));

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
            // TODO(CLOUDP-417359): measure what majority costs under replication lag.
            .readConcern(ReadConcern.MAJORITY, afterClusterTime)
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
   * Resolves sampled {@code _id}s to the {@code $recordId}s the partition boundaries are cut on.
   *
   * @param namespace the collection to read
   * @param ids the sampled {@code _id}s
   * @param afterClusterTime the cluster time the read must not precede
   * @return the resolved {@code $recordId}s, in no particular order
   */
  public List<Long> resolveRecordIds(
      MongoNamespace namespace, List<BsonValue> ids, BsonTimestamp afterClusterTime) {
    List<Long> recordIds = new ArrayList<>(ids.size());

    for (List<BsonValue> chunk : chunkForResolve(ids)) {
      var command =
          new CollectionScanFindCommand.Builder(namespace.getCollectionName())
              .filter(new BsonDocument(ID_FIELD, new BsonDocument("$in", new BsonArray(chunk))))
              .projection(RECORD_ID_PROJECTION)
              .showRecordId(BsonBoolean.TRUE)
              // _id is unique, so a chunk matches at most one document per id. Asking for the
              // whole sample leaves the server nothing to hold a cursor open for.
              .limit(RESOLVE_BATCH_SIZE)
              .batchSize(RESOLVE_BATCH_SIZE)
              .readConcern(ReadConcern.MAJORITY, afterClusterTime)
              .build();

      var response =
          FindCommandResponse.fromProxy(
              this.mongoClient
                  .getDatabase(namespace.getDatabaseName())
                  .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                  .runCommand(command.toProxy(), FindCommandResponseProxy.class));

      // The batch holds every document the chunk can match, so a live cursor means ids were
      // silently dropped and we would cut on a partial sample.
      checkState(response.getId() == 0, "resolve left a cursor open, chunk was not complete");

      for (RawBsonDocument document : response.getBatch()) {
        BsonValue recordId = document.get(RECORD_ID_FIELD);
        // Clustered collections report BinData here, but they already fell back at pre-flight,
        // so reaching this means the collection changed underneath the plan.
        checkState(recordId != null && recordId.isInt64(), "$recordId is missing or not an int64");
        recordIds.add(recordId.asInt64().getValue());
      }
    }
    return recordIds;
  }

  /**
   * Splits the ids so each request fits {@link #RESOLVE_PAYLOAD_BUDGET}. An id larger than the
   * whole budget goes out on its own rather than being dropped; BSON caps a document at 16MB, so
   * the command still fits.
   */
  private static List<List<BsonValue>> chunkForResolve(List<BsonValue> ids) {
    List<List<BsonValue>> chunks = new ArrayList<>();
    @Var List<BsonValue> current = new ArrayList<>();
    @Var Bytes payload = Bytes.ofBytes(0);

    for (BsonValue id : ids) {
      Bytes size = BsonUtils.bsonValueSerializedBytes(id);
      if (!current.isEmpty() && payload.add(size).compareTo(RESOLVE_PAYLOAD_BUDGET) > 0) {
        chunks.add(current);
        current = new ArrayList<>();
        payload = Bytes.ofBytes(0);
      }
      current.add(id);
      payload = payload.add(size);
    }
    if (!current.isEmpty()) {
      chunks.add(current);
    }
    return chunks;
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
