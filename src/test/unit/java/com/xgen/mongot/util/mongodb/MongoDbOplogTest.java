package com.xgen.mongot.util.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class MongoDbOplogTest {

  @Test
  public void getOldestTimestamp_oldestEntryPresent_returnsTs() {
    BsonTimestamp expected = new BsonTimestamp(1_700_000_000, 1);
    BsonDocument oldest = new BsonDocument("ts", expected);
    MongoClient client = clientReturningOldest(oldest);

    BsonTimestamp result = MongoDbOplog.getOldestTimestamp(client);

    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void getOldestTimestamp_emptyOplog_throwsIllegalState() {
    MongoClient client = clientReturningOldest(null);

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> MongoDbOplog.getOldestTimestamp(client));

    assertThat(thrown).hasMessageThat().isEqualTo("local.oplog.rs is empty");
  }

  @Test
  public void getOldestTimestamp_missingTsField_throwsIllegalState() {
    MongoClient client = clientReturningOldest(new BsonDocument("op", new BsonString("n")));

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> MongoDbOplog.getOldestTimestamp(client));

    assertThat(thrown).hasMessageThat().isEqualTo("oldest oplog entry did not include ts field");
  }

  @Test
  public void getOldestTimestamp_tsNotTimestamp_throwsIllegalState() {
    MongoClient client = clientReturningOldest(new BsonDocument("ts", new BsonInt32(1)));

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> MongoDbOplog.getOldestTimestamp(client));

    assertThat(thrown)
        .hasMessageThat()
        .isEqualTo("oldest oplog entry ts field was not a timestamp");
  }

  @Test
  public void getOldestTimestamp_nullClient_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> MongoDbOplog.getOldestTimestamp(null));
  }

  @SuppressWarnings("unchecked")
  private static MongoClient clientReturningOldest(BsonDocument oldest) {
    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(client.getDatabase(Databases.LOCAL)).thenReturn(database);
    when(database.getCollection(eq("oplog.rs"), eq(BsonDocument.class))).thenReturn(collection);
    when(collection.find()).thenReturn(findIterable);
    when(findIterable.projection(any())).thenReturn(findIterable);
    when(findIterable.sort(any())).thenReturn(findIterable);
    when(findIterable.limit(1)).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(oldest);
    return client;
  }
}
