package com.xgen.mongot.util.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.mongodb.ReadConcern;
import com.mongodb.client.model.Indexes;
import org.bson.BsonBoolean;
import org.junit.Test;

public class CollectionScanFindCommandTest {

  private static final String COLLECTION_NAME = "collectionScanFindCommandTest";

  @Test
  public void build_resumeTokenWithNaturalOrderHint_succeeds() {
    new CollectionScanFindCommand.Builder(COLLECTION_NAME)
        .hint(CollectionScanFindCommand.NATURAL_ORDER)
        .requestResumeToken(BsonBoolean.TRUE)
        .readConcern(ReadConcern.MAJORITY)
        .build();
  }

  @Test
  public void build_resumeTokenWithoutHint_throws() {
    CollectionScanFindCommand.Builder builder =
        new CollectionScanFindCommand.Builder(COLLECTION_NAME)
            .requestResumeToken(BsonBoolean.TRUE)
            .readConcern(ReadConcern.MAJORITY);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("requires hint {$natural: 1}");
  }

  @Test
  public void build_resumeTokenWithNonNaturalOrderHint_throws() {
    CollectionScanFindCommand.Builder builder =
        new CollectionScanFindCommand.Builder(COLLECTION_NAME)
            .hint(Indexes.ascending("_id"))
            .requestResumeToken(BsonBoolean.TRUE)
            .readConcern(ReadConcern.MAJORITY);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("requires hint {$natural: 1}");
  }

  @Test
  public void build_resumeTokenWithNaturalOrderSort_succeeds() {
    new CollectionScanFindCommand.Builder(COLLECTION_NAME)
        .hint(CollectionScanFindCommand.NATURAL_ORDER)
        .sort(CollectionScanFindCommand.NATURAL_ORDER)
        .requestResumeToken(BsonBoolean.TRUE)
        .readConcern(ReadConcern.MAJORITY)
        .build();
  }

  @Test
  public void build_resumeTokenWithNonNaturalOrderSort_throws() {
    CollectionScanFindCommand.Builder builder =
        new CollectionScanFindCommand.Builder(COLLECTION_NAME)
            .hint(CollectionScanFindCommand.NATURAL_ORDER)
            .sort(Indexes.ascending("_id"))
            .requestResumeToken(BsonBoolean.TRUE)
            .readConcern(ReadConcern.MAJORITY);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("sort unset or {$natural: 1}");
  }

  @Test
  public void limit_nonPositive_throws() {
    CollectionScanFindCommand.Builder builder =
        new CollectionScanFindCommand.Builder(COLLECTION_NAME);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.limit(0));

    assertThat(exception).hasMessageThat().contains("limit must be positive");
  }

  @Test
  public void build_resumeTokenDisabledWithoutHint_succeeds() {
    new CollectionScanFindCommand.Builder(COLLECTION_NAME)
        .requestResumeToken(BsonBoolean.FALSE)
        .readConcern(ReadConcern.MAJORITY)
        .build();
  }
}
