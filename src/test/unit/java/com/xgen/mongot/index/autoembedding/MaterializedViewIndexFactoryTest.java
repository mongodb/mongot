package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link MaterializedViewIndexFactory#isSourceCollectionAbsentOnMongod}: the source
 * collection presence check that gates materialized-view setup on a shard. Exercises the
 * package-private {@code (Optional<MongoClient>, IndexDefinition)} overload and stubs the {@code
 * collectionUuidsOnShard} seam so all four branches are covered without building the factory.
 */
public class MaterializedViewIndexFactoryTest {

  private static final String DATABASE = "db";
  private static final UUID SOURCE_COLLECTION_UUID = UUID.randomUUID();

  @Test
  public void isSourceCollectionAbsentOnMongod_resolverClientNotReady_returnsFalse()
      throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealAbsenceCheck();

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
    MaterializedViewIndexFactory factory = factoryWithRealAbsenceCheck();
    when(factory.collectionUuidsOnShard(any(), eq(DATABASE)))
        .thenReturn(Set.of(SOURCE_COLLECTION_UUID));

    assertFalse(
        factory.isSourceCollectionAbsentOnMongod(
            Optional.of(mock(MongoClient.class)), indexDefinition()));
  }

  @Test
  public void isSourceCollectionAbsentOnMongod_collectionMissing_returnsTrue() throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealAbsenceCheck();
    when(factory.collectionUuidsOnShard(any(), eq(DATABASE))).thenReturn(Set.of(UUID.randomUUID()));

    assertTrue(
        factory.isSourceCollectionAbsentOnMongod(
            Optional.of(mock(MongoClient.class)), indexDefinition()));
  }

  @Test
  public void isSourceCollectionAbsentOnMongod_queryFails_returnsFalse() throws Exception {
    MaterializedViewIndexFactory factory = factoryWithRealAbsenceCheck();
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

  private static MaterializedViewIndexFactory factoryWithRealAbsenceCheck() {
    MaterializedViewIndexFactory factory = mock(MaterializedViewIndexFactory.class);
    when(factory.isSourceCollectionAbsentOnMongod(any(), any(IndexDefinition.class)))
        .thenCallRealMethod();
    return factory;
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
