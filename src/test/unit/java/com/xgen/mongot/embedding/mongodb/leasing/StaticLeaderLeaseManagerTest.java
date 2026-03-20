package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link StaticLeaderLeaseManager}.
 *
 * <p>These tests verify the static leader behavior, focusing on the initializeLease method which
 * handles lease initialization for both leaders and followers in Community edition.
 */
@SuppressWarnings("deprecation")
public class StaticLeaderLeaseManagerTest {

  private static final String HOSTNAME = "test-host";
  private static final String DATABASE_NAME = "test-db";

  private MongoClient mockMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection<BsonDocument> mockCollection;
  private FindIterable<BsonDocument> mockFindIterable;
  private MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    this.mockMongoClient = mock(MongoClient.class);
    this.mockDatabase = mock(MongoDatabase.class);
    this.mockCollection = mock(MongoCollection.class);
    this.mockFindIterable = mock(FindIterable.class);
    this.mvMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();

    when(this.mockMongoClient.getDatabase(DATABASE_NAME)).thenReturn(this.mockDatabase);
    when(this.mockDatabase.getCollection(
            StaticLeaderLeaseManager.LEASE_COLLECTION_NAME, BsonDocument.class))
        .thenReturn(this.mockCollection);
    when(this.mockCollection.withReadConcern(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.withReadPreference(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.find()).thenReturn(this.mockFindIterable);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(this.mockFindIterable);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());
  }

  // ==================== initializeLease Tests ====================

  @Test
  public void initializeLease_noExistingLease_returnsProposedMetadata() throws Exception {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.randomUUID(), "mv-collection-name");

    // Act
    MaterializedViewCollectionMetadata result =
        leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return proposed metadata (no existing lease)
    assertThat(result).isEqualTo(proposedMetadata);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void initializeLease_existingLeaseInMemory_returnsExistingMetadata() throws Exception {
    // Arrange - setup a lease in the database that will be loaded on construction
    String collectionName = "existing-mv-collection";
    UUID existingUuid = UUID.randomUUID();
    MaterializedViewCollectionMetadata existingMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, existingUuid, collectionName);

    Lease existingLease = createLeaseWithMetadata(collectionName, existingMetadata);
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(existingLease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Mock find(Document).first() to return the existing lease when queried by collection name
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(existingLease.toBson());

    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    // Different proposed metadata but same collection name
    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(1, Map.of()),
            UUID.randomUUID(),
            collectionName);

    // Act
    leaseManager.syncLeasesFromMongod();
    MaterializedViewCollectionMetadata result =
        leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return the existing lease's metadata, not the proposed one
    assertThat(result.collectionName()).isEqualTo(collectionName);
    assertThat(result.collectionUuid()).isEqualTo(existingUuid);
    assertThat(result.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(0L);
  }

  @Test
  public void initializeLease_followerWithNoExistingLease_returnsProposedMetadata()
      throws Exception {
    // Arrange - follower (isLeader = false)
    StaticLeaderLeaseManager followerLeaseManager = createLeaseManager(false);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.randomUUID(), "follower-mv-collection");

    // Act
    MaterializedViewCollectionMetadata result =
        followerLeaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - follower should return proposed metadata when no existing lease
    assertThat(result).isEqualTo(proposedMetadata);
  }

  // ==================== Helper Methods ====================

  private StaticLeaderLeaseManager createLeaseManager(boolean isLeader) {
    return new StaticLeaderLeaseManager(
        this.mockMongoClient,
        new SimpleMetricsFactory(),
        HOSTNAME,
        DATABASE_NAME,
        isLeader,
        this.mvMetadataCatalog);
  }

  private Lease createLeaseWithMetadata(
      String collectionName, MaterializedViewCollectionMetadata metadata) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        "source-collection",
        HOSTNAME,
        Instant.now().plusSeconds(60),
        1L,
        "",
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        metadata,
        null);
  }
}
