package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.junit.Test;

public class PartitionPlanningMongoClientTest {

  private static final String RECORD_ID_FIELD = "$recordId";
  private static final String INITIAL_SYNC_ID_FIELD = "$initialSyncId";

  private static final UUID INITIAL_SYNC_ID = UUID.randomUUID();

  /** A token carrying a scan position, which every shape below shares. */
  private static BsonDocument token() {
    return new BsonDocument(RECORD_ID_FIELD, new BsonInt64(42L));
  }

  @Test
  public void readInitialSyncId_uuidPresent_returnsIt() {
    BsonDocument resumeToken =
        token().append(INITIAL_SYNC_ID_FIELD, new BsonBinary(INITIAL_SYNC_ID));

    assertThat(PartitionPlanningMongoClient.readInitialSyncId(resumeToken))
        .hasValue(INITIAL_SYNC_ID);
  }

  @Test
  public void readInitialSyncId_fieldAbsent_returnsEmpty() {
    // A sync source without local collections, such as a disaggregated storage cluster, reports a
    // resume token with no id on it at all.
    assertThat(PartitionPlanningMongoClient.readInitialSyncId(token())).isEmpty();
  }

  @Test
  public void readInitialSyncId_fieldIsNull_returnsEmpty() {
    // Some storage configurations report the field but leave it null. Read rather than raised,
    // unlike the coherence check on the scan path.
    BsonDocument resumeToken = token().append(INITIAL_SYNC_ID_FIELD, BsonNull.VALUE);

    assertThat(PartitionPlanningMongoClient.readInitialSyncId(resumeToken)).isEmpty();
  }

  @Test
  public void readInitialSyncId_binaryIsNotAUuidSubtype_returnsEmpty() {
    // asUuid raises on a non-UUID subtype, so the subtype is checked rather than assumed.
    BsonDocument resumeToken =
        token()
            .append(
                INITIAL_SYNC_ID_FIELD,
                new BsonBinary(BsonBinarySubType.BINARY, new byte[] {1, 2, 3, 4}));

    assertThat(PartitionPlanningMongoClient.readInitialSyncId(resumeToken)).isEmpty();
  }

  @Test
  public void readInitialSyncId_fieldIsNotBinary_returnsEmpty() {
    BsonDocument resumeToken = token().append(INITIAL_SYNC_ID_FIELD, new BsonInt64(7L));

    assertThat(PartitionPlanningMongoClient.readInitialSyncId(resumeToken))
        .isEqualTo(Optional.empty());
  }
}
