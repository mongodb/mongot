package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class CollectionRosterTest {

  private static final String DATABASE = "testdb";

  private CollectionRoster roster;

  @Before
  public void setUp() {
    this.roster = new CollectionRoster();
  }

  @Test
  public void deregisterLastIndexRemovesCollection() {
    UUID collection = UUID.randomUUID();
    GenerationId first = newGenerationId();
    GenerationId second = newGenerationId();
    this.roster.register(first, namespace("coll"), collection, null);
    this.roster.register(second, namespace("coll"), collection, null);

    assertThat(this.roster.trackedIndexCount()).isEqualTo(2);
    assertThat(this.roster.trackedCollectionCount()).isEqualTo(1);

    assertThat(this.roster.deregister(first)).isFalse();
    assertThat(this.roster.trackedIndexCount()).isEqualTo(1);
    assertThat(this.roster.trackedCollectionCount()).isEqualTo(1);

    assertThat(this.roster.deregister(second)).isTrue();
    assertThat(this.roster.trackedIndexCount()).isEqualTo(0);
    assertThat(this.roster.trackedCollectionCount()).isEqualTo(0);
  }

  @Test
  public void unmanagedNamespaceIsNotTracked() {
    UUID collection = UUID.randomUUID();
    this.roster.register(newGenerationId(), namespace("managed"), collection, null);

    assertThat(this.roster.trackedCollectionFor(namespace("managed"))).hasValue(collection);
    assertThat(this.roster.trackedCollectionFor(namespace("unmanaged"))).isEmpty();
  }

  @Test
  public void renameReregistrationRedirectsDemux() {
    UUID collection = UUID.randomUUID();
    GenerationId index = newGenerationId();
    this.roster.register(index, namespace("before"), collection, null);
    assertThat(this.roster.trackedCollectionFor(namespace("before"))).hasValue(collection);

    // The index lifecycle re-registers the same collection under its new name after a rename.
    this.roster.register(index, namespace("after"), collection, null);

    // Events under the old name no longer demux; events under the new name do.
    assertThat(this.roster.trackedCollectionFor(namespace("before"))).isEmpty();
    assertThat(this.roster.trackedCollectionFor(namespace("after"))).hasValue(collection);
  }

  @Test
  public void namespaceReuseByAnotherCollectionKeepsDemuxConsistent() {
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    GenerationId firstIndex = newGenerationId();
    GenerationId secondIndex = newGenerationId();
    this.roster.register(firstIndex, namespace("cycled"), first, null);

    // A new collection takes over the same name (drop/recreate): the name must demux to the new
    // collection, and deregistering it must not corrupt other mappings.
    this.roster.register(secondIndex, namespace("cycled"), second, null);
    assertThat(this.roster.trackedCollectionFor(namespace("cycled"))).hasValue(second);

    this.roster.deregister(secondIndex);
    // The second collection owned the name; deregistering it leaves the name unmapped.
    assertThat(this.roster.trackedCollectionFor(namespace("cycled"))).isEmpty();

    // Re-registering the surviving first collection under the reused name restores demuxing for it.
    this.roster.register(firstIndex, namespace("cycled"), first, null);
    assertThat(this.roster.trackedCollectionFor(namespace("cycled"))).hasValue(first);
  }

  @Test
  public void multipleIndexesOnOneCollectionAllCountInactive() {
    UUID collection = UUID.randomUUID();
    this.roster.register(newGenerationId(), namespace("coll"), collection, null);
    this.roster.register(newGenerationId(), namespace("coll"), collection, null);

    this.roster.seedBaselines(timestamp(1000));

    // Seeded at 1000, position advanced to 1400 with no events: idle 400 >= threshold 300, so both
    // indexes on the collection count inactive.
    assertThat(this.roster.countInactiveIndexes(timestamp(1400), 300)).isEqualTo(2);
    // Position 1100: idle 100 < threshold, so neither counts.
    assertThat(this.roster.countInactiveIndexes(timestamp(1100), 300)).isEqualTo(0);
  }

  @Test
  public void recordedActivityLowersIdle() {
    UUID collection = UUID.randomUUID();
    this.roster.register(newGenerationId(), namespace("coll"), collection, timestamp(1000));

    // Before any event, seeded at 1000: idle at 1600 is 600 >= threshold 300, so inactive.
    assertThat(this.roster.countInactiveIndexes(timestamp(1600), 300)).isEqualTo(1);

    // An event at 1500 refreshes activity: idle at 1600 is now 100 < threshold, so active.
    this.roster.recordActivity(collection, timestamp(1500));
    assertThat(this.roster.countInactiveIndexes(timestamp(1600), 300)).isEqualTo(0);
  }

  @Test
  public void reRegisterClearsStaleActivity() {
    UUID collection = UUID.randomUUID();
    GenerationId index = newGenerationId();
    this.roster.register(index, namespace("coll"), collection, timestamp(1000));
    this.roster.recordActivity(collection, timestamp(2000));

    this.roster.deregister(index);
    // A residual read-loop write can slip through after deregistration; it must not survive
    // re-registration and resurrect stale activity.
    this.roster.recordActivity(collection, timestamp(2000));

    // Re-entering the roster clears the stale activity and re-seeds from the fresh position.
    this.roster.register(index, namespace("coll"), collection, timestamp(3000));

    // Idle at 3200 is 200 (< threshold 300), so the index is active. Had the stale 2000 activity
    // survived, idle would be 1200 (>= threshold) and count inactive.
    assertThat(this.roster.countInactiveIndexes(timestamp(3200), 300)).isEqualTo(0);
  }

  @Test
  public void clearActivityResetsBaselines() {
    UUID collection = UUID.randomUUID();
    this.roster.register(newGenerationId(), namespace("coll"), collection, timestamp(1000));

    // Seeded at 1000: idle at 5000 is 4000 >= threshold, so inactive.
    assertThat(this.roster.countInactiveIndexes(timestamp(5000), 300)).isEqualTo(1);

    // Clearing activity drops the baseline; an unseeded collection reports zero idle until
    // re-seeded, so it no longer counts inactive.
    this.roster.clearActivity();
    assertThat(this.roster.countInactiveIndexes(timestamp(5000), 300)).isEqualTo(0);

    // Re-seeding at the reopened position (5000) ages from there, not the stale pre-clear point.
    this.roster.seedBaselines(timestamp(5000));
    assertThat(this.roster.countInactiveIndexes(timestamp(5100), 300)).isEqualTo(0);
  }

  @Test
  public void unseededCollectionReportsZeroIdle() {
    UUID collection = UUID.randomUUID();
    this.roster.register(newGenerationId(), namespace("coll"), collection, null);

    // Registered before any batch (no seed position): idle is defined as zero until a baseline is
    // seeded, so it never counts inactive.
    assertThat(this.roster.countInactiveIndexes(timestamp(9999), 1)).isEqualTo(0);
  }

  private static GenerationId newGenerationId() {
    return new GenerationId(new ObjectId(), Generation.CURRENT);
  }

  private static MongoNamespace namespace(String collection) {
    return new MongoNamespace(DATABASE, collection);
  }

  private static BsonTimestamp timestamp(int seconds) {
    return new BsonTimestamp(seconds, 0);
  }
}
