package com.xgen.mongot.index.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.realMatViewIndexFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for the source collection presence check that gates materialized-view setup on a shard, and
 * for the per-collection status reference {@code getIndex} hands out.
 */
public class MaterializedViewIndexFactoryTest {

  private static final String DATABASE = "db";
  private static final UUID SOURCE_COLLECTION_UUID = UUID.randomUUID();
  private static final UUID MAT_VIEW_COLLECTION_UUID = UUID.randomUUID();
  private static final ObjectId INDEX_ID = new ObjectId();

  @Test
  public void isSourceCollectionAbsentOnMongod_resolverClientNotReady_returnsFalse()
      throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();

    // Presence cannot be confirmed without a resolver client, so we do NOT declare it absent --
    // the caller proceeds and getIndex fails to a recoverable FAILED status (no MV/lease created).
    assertFalse(
        "Cannot determine presence without a resolver client; must not declare absent",
        factory.isSourceCollectionAbsentOnMongod(Optional.empty(), indexDefinition()));
    // Presence is never queried when the sync-source client is not yet available.
    verify(factory, never()).collectionUuidsOnShard(any(), any());
  }

  @Test
  public void isSourceCollectionAbsentOnMongod_collectionPresent_returnsFalse() throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();
    when(factory.collectionUuidsOnShard(any(), eq(DATABASE)))
        .thenReturn(Set.of(SOURCE_COLLECTION_UUID));

    assertFalse(
        factory.isSourceCollectionAbsentOnMongod(
            Optional.of(mock(MongoClient.class)), indexDefinition()));
  }

  @Test
  public void isSourceCollectionAbsentOnMongod_collectionMissing_returnsTrue() throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();
    when(factory.collectionUuidsOnShard(any(), eq(DATABASE))).thenReturn(Set.of(UUID.randomUUID()));

    assertTrue(
        factory.isSourceCollectionAbsentOnMongod(
            Optional.of(mock(MongoClient.class)), indexDefinition()));
  }

  @Test
  public void isSourceCollectionAbsentOnMongod_queryFails_returnsFalse() throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();
    when(factory.collectionUuidsOnShard(any(), any()))
        .thenThrow(new CheckedMongoException(new MongoException("boom")));

    // A transient query failure does not confirm absence, so we do NOT declare the collection
    // absent -- the caller proceeds and the normal transient-retry path handles it (getIndex hits
    // the same failure and parks the index in a recoverable FAILED/RETRY, with no MV/lease
    // created).
    assertFalse(
        "A transient query failure must not declare the collection absent",
        factory.isSourceCollectionAbsentOnMongod(
            Optional.of(mock(MongoClient.class)), indexDefinition()));
  }

  @Test
  public void isMongodReachableFromCachedTopology_resolverClientNotReady_returnsFalse() {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();

    assertFalse(factory.isMongodReachableFromCachedTopology(Optional.empty()));
  }

  @Test
  public void isMongodReachableFromCachedTopology_primaryAvailable_returnsTrue() {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();
    MongoClient client = mock(MongoClient.class);
    when(client.getClusterDescription())
        .thenReturn(clusterDescription(ServerType.REPLICA_SET_PRIMARY, true));

    assertTrue(factory.isMongodReachableFromCachedTopology(Optional.of(client)));
  }

  @Test
  public void isMongodReachableFromCachedTopology_noPrimaryAvailable_returnsFalse() {
    MaterializedViewIndexFactory factory = factoryWithRealChecks();
    MongoClient client = mock(MongoClient.class);
    when(client.getClusterDescription())
        .thenReturn(clusterDescription(ServerType.REPLICA_SET_SECONDARY, true));

    assertFalse(factory.isMongodReachableFromCachedTopology(Optional.of(client)));
  }

  /**
   * Producer 2 (in-flight staged rebuild): a Lucene-only change leaves user generations N and N+1
   * on one collection, so N's generator is replaced and only N+1's index is written to.
   */
  @Test
  public void getIndex_twoUserGenerationsOnOneMatViewCollection_shareStatus() throws Exception {
    MaterializedViewIndexFactory factory =
        realMatViewIndexFactory(ignored -> MAT_VIEW_COLLECTION_UUID);
    MaterializedViewGenerationId userGeneration1 =
        new MaterializedViewGenerationId(
            INDEX_ID, new MaterializedViewGeneration(Generation.CURRENT));
    MaterializedViewGenerationId userGeneration2 =
        new MaterializedViewGenerationId(INDEX_ID, userGeneration1.generation.incrementUser());

    InitializedMaterializedViewIndex displacedIndex =
        factory.getIndex(mockMatViewDefinitionGeneration(userGeneration1, 0));
    InitializedMaterializedViewIndex survivingIndex =
        factory.getIndex(mockMatViewDefinitionGeneration(userGeneration2, 1));

    // MaterializedViewStatusUpdater only writes to the installed generator's index.
    survivingIndex.setStatus(IndexStatus.steady());

    assertEquals(
        "Displaced generation must read the surviving generator's status, not a dead reference",
        IndexStatus.StatusCode.STEADY,
        displacedIndex.getStatus().getStatusCode());
  }

  /**
   * A resync on a shared collection now reaches the displaced generation too. Its queryable ratchet
   * turns the shared INITIAL_SYNC into RECOVERING_TRANSIENT, reported as stale rather than the
   * STEADY it was frozen at before.
   */
  @Test
  public void getIndex_resyncOnSharedCollection_reportsDisplacedGenerationStale() throws Exception {
    MaterializedViewIndexFactory factory =
        realMatViewIndexFactory(ignored -> MAT_VIEW_COLLECTION_UUID);
    MaterializedViewGenerationId userGeneration1 =
        new MaterializedViewGenerationId(
            INDEX_ID, new MaterializedViewGeneration(Generation.CURRENT));
    MaterializedViewGenerationId userGeneration2 =
        new MaterializedViewGenerationId(INDEX_ID, userGeneration1.generation.incrementUser());

    InitializedMaterializedViewIndex displacedIndex =
        factory.getIndex(mockMatViewDefinitionGeneration(userGeneration1, 0));
    displacedIndex.setStatus(IndexStatus.steady());

    InitializedMaterializedViewIndex resyncingIndex =
        factory.getIndex(mockMatViewDefinitionGeneration(userGeneration2, 1));
    resyncingIndex.setStatus(new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC));

    assertEquals(
        IndexStatus.StatusCode.RECOVERING_TRANSIENT, displacedIndex.getStatus().getStatusCode());
  }

  /** Control: different collections keep independent statuses. */
  @Test
  public void getIndex_separateMatViewCollections_doNotShareStatus() throws Exception {
    MaterializedViewIndexFactory factory = realMatViewIndexFactory(ignored -> UUID.randomUUID());
    MaterializedViewGenerationId generationId =
        new MaterializedViewGenerationId(
            INDEX_ID, new MaterializedViewGeneration(Generation.CURRENT));

    InitializedMaterializedViewIndex index1 =
        factory.getIndex(mockMatViewDefinitionGeneration(generationId, 0));
    InitializedMaterializedViewIndex index2 =
        factory.getIndex(mockMatViewDefinitionGeneration(generationId, 0));

    index2.setStatus(IndexStatus.steady());

    assertEquals(IndexStatus.StatusCode.UNKNOWN, index1.getStatus().getStatusCode());
  }

  private static MaterializedViewIndexFactory factoryWithRealChecks() {
    MaterializedViewIndexFactory factory = mock(MaterializedViewIndexFactory.class);
    when(factory.isSourceCollectionAbsentOnMongod(any(), any(IndexDefinition.class)))
        .thenCallRealMethod();
    when(factory.isMongodReachableFromCachedTopology(any())).thenCallRealMethod();
    return factory;
  }

  private static ClusterDescription clusterDescription(ServerType serverType, boolean ok) {
    ServerDescription serverDescription =
        ServerDescription.builder()
            .address(new ServerAddress())
            .type(serverType)
            .state(ServerConnectionState.CONNECTED)
            .ok(ok)
            .build();
    return new ClusterDescription(
        ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, List.of(serverDescription));
  }

  private static IndexDefinition indexDefinition() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(new ObjectId())
        .name("mock")
        .database(DATABASE)
        .lastObservedCollectionName("mock")
        .collectionUuid(SOURCE_COLLECTION_UUID)
        .withAutoEmbedField("title")
        .build();
  }
}
