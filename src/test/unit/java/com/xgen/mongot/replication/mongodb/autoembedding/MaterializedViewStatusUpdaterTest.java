package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.realMatViewIndexFactory;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests that {@link MaterializedViewStatusUpdater} cannot publish a status through a generator that
 * was replaced after it snapshotted the generator map. Generations sharing a materialized view
 * collection share one status reference, so such a write would overwrite the successor's status,
 * and {@link MaterializedViewGenerator#shutdown} clears isLeader, so the updater's own checks let a
 * displaced generator through.
 */
public class MaterializedViewStatusUpdaterTest {

  private static final ObjectId INDEX_ID = new ObjectId();
  private static final UUID COLLECTION_UUID = UUID.randomUUID();

  @Test
  public void copyFollowerStatuses_generatorReplacedAfterSnapshot_doesNotClobberSuccessor() {
    MaterializedViewIndexFactory factory = realMatViewIndexFactory(ignored -> COLLECTION_UUID);
    var displaced = generation(factory, Generation.CURRENT);
    var surviving = generation(factory, Generation.CURRENT.incrementUser());

    // The successor is installed and has published STEADY.
    surviving.getIndex().setStatus(IndexStatus.steady());

    // The updater still holds the pre-replacement snapshot, and the lease still carries a status
    // for the displaced generation.
    var updater =
        updaterOver(
            Map.of(COLLECTION_UUID, follower(displaced)),
            Map.of(COLLECTION_UUID, follower(surviving)));
    updater.copyFollowerStatuses(
        new LeaseManager.FollowerPollResult(
            Map.of(displaced.getGenerationId(), IndexStatus.failed("stale")), Set.of()));

    assertEquals(
        "A displaced generator must not overwrite the status its successor shares with it",
        IndexStatus.StatusCode.STEADY,
        surviving.getIndex().getStatus().getStatusCode());
  }

  @Test
  public void copyFollowerStatuses_generatorStillInstalled_publishesStatus() {
    MaterializedViewIndexFactory factory = realMatViewIndexFactory(ignored -> COLLECTION_UUID);
    var current = generation(factory, Generation.CURRENT);
    var installed = Map.of(COLLECTION_UUID, follower(current));

    updaterOver(installed, installed)
        .copyFollowerStatuses(
            new LeaseManager.FollowerPollResult(
                Map.of(current.getGenerationId(), IndexStatus.steady()), Set.of()));

    assertEquals(IndexStatus.StatusCode.STEADY, current.getIndex().getStatus().getStatusCode());
  }

  /** Updater whose generator snapshot is {@code snapshot} while {@code installed} is live. */
  private static MaterializedViewStatusUpdater updaterOver(
      Map<UUID, MaterializedViewGenerator> snapshot,
      Map<UUID, MaterializedViewGenerator> installed) {
    return new MaterializedViewStatusUpdater(
        mock(AutoEmbeddingMongoClient.class),
        new SimpleMeterRegistry(),
        () -> snapshot,
        (uuid, generator, publish) -> {
          if (installed.get(uuid) == generator) {
            publish.run();
          }
        },
        generationId -> false,
        () -> true);
  }

  private static MaterializedViewGenerator follower(MaterializedViewIndexGeneration generation) {
    var generator = mock(MaterializedViewGenerator.class);
    when(generator.getIndexGeneration()).thenReturn(generation);
    // shutdown() clears isLeader, so a displaced generator looks like a follower here too.
    when(generator.isLeader()).thenReturn(false);
    return generator;
  }

  private static MaterializedViewIndexGeneration generation(
      MaterializedViewIndexFactory factory, Generation generation) {
    var definition =
        mockMatViewDefinitionGeneration(
            new MaterializedViewGenerationId(INDEX_ID, new MaterializedViewGeneration(generation)));
    try {
      return new MaterializedViewIndexGeneration(factory.getIndex(definition), definition);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
