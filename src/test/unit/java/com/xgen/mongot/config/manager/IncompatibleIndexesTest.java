package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import java.util.List;
import java.util.UUID;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class IncompatibleIndexesTest {

  private static final String DATABASE = "bar";
  private static final UUID COLLECTION_UUID =
      UUID.fromString("00000000-1111-2222-3333-444444444444");
  private static final ObjectId INDEX_ID = new ObjectId("012345678901234567890123");
  private static final ObjectId OTHER_INDEX_ID = new ObjectId("012345678901234567890124");

  @Test
  public void newRegistry_isEmpty() {
    var incompatibleIndexes = new IncompatibleIndexes();

    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().isEmpty());
    Assert.assertTrue(incompatibleIndexes.getStagedIndexes().isEmpty());
  }

  @Test
  public void addLiveIndex_isTrackedAsLive() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "field is required", 1);

    incompatibleIndexes.addLiveIndex(live);

    Assert.assertEquals(List.of(live), List.copyOf(incompatibleIndexes.getLiveIndexes()));
    Assert.assertTrue(incompatibleIndexes.getStagedIndexes().isEmpty());
  }

  @Test
  public void addStagedIndex_isTrackedAsStaged() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "field is required", 1);

    incompatibleIndexes.addStagedIndex(staged);

    Assert.assertEquals(List.of(staged), List.copyOf(incompatibleIndexes.getStagedIndexes()));
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().isEmpty());
  }

  /** An index can have a quarantined generation staged and live at once. */
  @Test
  public void sameIndexLiveAndStaged_keepsBothGenerations() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "live is broken", 1);
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "staged is broken", 2);

    incompatibleIndexes.addLiveIndex(live);
    incompatibleIndexes.addStagedIndex(staged);

    Assert.assertEquals(List.of(live), List.copyOf(incompatibleIndexes.getLiveIndexes()));
    Assert.assertEquals(List.of(staged), List.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  @Test
  public void differentIndexes_areTrackedIndependently() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex first = quarantined(INDEX_ID, "foo", "foo is broken", 1);
    QuarantinedIndex second = quarantined(OTHER_INDEX_ID, "other", "other is broken", 1);

    incompatibleIndexes.addLiveIndex(first);
    incompatibleIndexes.addLiveIndex(second);

    Assert.assertEquals(2, incompatibleIndexes.getLiveIndexes().size());
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().contains(first));
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().contains(second));
  }

  @Test
  public void addLiveIndex_sameIndexTwice_replacesTheEarlierGeneration() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex second = quarantined(INDEX_ID, "foo", "second", 2);

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "first", 1));
    incompatibleIndexes.addLiveIndex(second);

    Assert.assertEquals(List.of(second), List.copyOf(incompatibleIndexes.getLiveIndexes()));
  }

  @Test
  public void addStagedIndex_sameIndexTwice_replacesTheEarlierGeneration() {
    var incompatibleIndexes = new IncompatibleIndexes();
    QuarantinedIndex second = quarantined(INDEX_ID, "foo", "second", 2);

    incompatibleIndexes.addStagedIndex(quarantined(INDEX_ID, "foo", "first", 1));
    incompatibleIndexes.addStagedIndex(second);

    Assert.assertEquals(List.of(second), List.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  private static QuarantinedIndex quarantined(
      ObjectId indexId, String name, String parseError, int userIndexVersion) {
    var identity =
        new JournalEntryIdentity(
            indexId,
            DATABASE,
            COLLECTION_UUID,
            name,
            new Generation(new UserIndexVersion(userIndexVersion), IndexFormatVersion.CURRENT));

    return new QuarantinedIndex(
        identity,
        IndexDefinitionGeneration.Type.SEARCH,
        parseError,
        new BsonDocument("fieldFromTheFuture", new BsonBoolean(true)));
  }
}
