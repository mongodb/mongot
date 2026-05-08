package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockIndex;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CommunityConfigUpdaterTest {

  private static final ConnectionInfo TEST_CLUSTER_URI =
      new ConnectionInfo(new ConnectionString("mongodb://localhost:27017/"));

  private static final ConnectionInfo MONGOD_HOST_1 =
      new ConnectionInfo(new ConnectionString("mongodb://rs1.local:27017/?directConnection=true"));
  private static final ConnectionInfo MONGOD_HOST_2 =
      new ConnectionInfo(new ConnectionString("mongodb://rs2.local:27018/?directConnection=true"));
  private static final ConnectionInfo MONGOS_HOST =
      new ConnectionInfo(
          new ConnectionString("mongodb://mongos.local:27019/?directConnection=true"));
  private static final ConnectionInfo MONGOS_HOST_2 =
      new ConnectionInfo(
          new ConnectionString("mongodb://mongos2.local:27020/?directConnection=true"));

  private AuthoritativeIndexCatalog authoritativeIndexCatalog;
  private MongoDbMetadataClient mongoDbMetadataClient;
  private ConfigManager configManager;
  private FeatureFlags featureFlags;
  private SyncSourceConfig syncSourceConfig;
  private InitialSyncHostProvider initialSyncHostProvider;

  private CommunityConfigUpdater communityConfigUpdater;

  private static SyncSourceConfig testSyncSourceConfig() {
    return SyncSourceConfig.builder()
        .mongodClusterReplicationUri(TEST_CLUSTER_URI)
        .mongodClusterReadWriteUri(TEST_CLUSTER_URI)
        .build();
  }

  private static SyncSourceConfig configWithMongodHost(ConnectionInfo host) {
    return SyncSourceConfig.builder()
        .mongodSingleHostReplicationUri(host)
        .mongodClusterReplicationUri(TEST_CLUSTER_URI)
        .mongodClusterReadWriteUri(TEST_CLUSTER_URI)
        .build();
  }

  private static SyncSourceConfig shardedConfigWithHosts(
      Optional<ConnectionInfo> mongodHost, Optional<ConnectionInfo> mongosHost) {
    return SyncSourceConfig.builder()
        .mongodSingleHostReplicationUri(mongodHost)
        .mongodClusterReplicationUri(TEST_CLUSTER_URI)
        .mongodClusterReadWriteUri(TEST_CLUSTER_URI)
        .mongosSingleHostReplicationUri(mongosHost)
        .mongosClusterReadWriteUri(TEST_CLUSTER_URI)
        .isSharded(true)
        .build();
  }

  @Before
  public void setUp() throws Exception {
    this.authoritativeIndexCatalog = mock(AuthoritativeIndexCatalog.class);
    this.mongoDbMetadataClient = mock(MongoDbMetadataClient.class);
    this.configManager = mock(ConfigManager.class);
    this.featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .enable(Feature.INDEX_FEATURE_VERSION_FOUR)
            .build();
    this.syncSourceConfig = testSyncSourceConfig();
    this.initialSyncHostProvider = mock(InitialSyncHostProvider.class);
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.empty());

    this.communityConfigUpdater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            this.syncSourceConfig,
            this.initialSyncHostProvider);
  }

  @After
  public void tearDown() throws Exception {
    this.communityConfigUpdater.close();
  }

  @Test
  public void testUpdatesSearchIndex() throws MetadataServiceException {
    when(this.authoritativeIndexCatalog.listIndexDefinitions())
        .thenReturn(List.of(SearchIndex.MOCK_INDEX_DEFINITION));

    this.communityConfigUpdater.update();

    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(this.syncSourceConfig);
    verify(this.configManager).handleReplicationAndSyncSourceUpdate(this.syncSourceConfig);
    verify(this.configManager)
        .update(List.of(), List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(), Set.of());
    verify(this.authoritativeIndexCatalog, never()).updateIndex(any(), any(), any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void testUpdatesSearchAndVectorIndex() throws MetadataServiceException {
    when(this.authoritativeIndexCatalog.listIndexDefinitions())
        .thenReturn(List.of(SearchIndex.MOCK_INDEX_DEFINITION, VectorIndex.MOCK_VECTOR_DEFINITION));

    this.communityConfigUpdater.update();

    verify(this.configManager)
        .update(
            List.of(VectorIndex.MOCK_VECTOR_DEFINITION),
            List.of(SearchIndex.MOCK_INDEX_DEFINITION),
            List.of(),
            Set.of());
    verify(this.authoritativeIndexCatalog, never()).updateIndex(any(), any(), any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void testUpdatesViewWithValidDefinition()
      throws CheckedMongoException, MetadataServiceException {
    var viewDefinition =
        ViewDefinition.existing(
            "myView",
            List.of(new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(420)))));
    var indexDefinition =
        SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
            .view(viewDefinition)
            .build();
    var indexDefinitions = List.of((IndexDefinition) indexDefinition);

    var newViewPipeline =
        List.of(new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(420))));
    var newViewDefinition = ViewDefinition.existing("myView", newViewPipeline);
    var newIndexDefinition =
        SearchIndexDefinitionBuilder.from(indexDefinition).view(newViewDefinition).build();
    var collectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(indexDefinition.getDatabase(), "myView"),
                new MongoDbCollectionInfo.View(
                    "myView",
                    new MongoDbCollectionInfo.View.Options(
                        indexDefinition.getLastObservedCollectionName(), newViewPipeline)),
                new MongoNamespace(
                    indexDefinition.getDatabase(), indexDefinition.getLastObservedCollectionName()),
                new MongoDbCollectionInfo.Collection(
                    indexDefinition.getLastObservedCollectionName(),
                    new MongoDbCollectionInfo.Collection.Info(
                        indexDefinition.getCollectionUuid()))));
    when(this.mongoDbMetadataClient.resolveCollectionInfos(Set.of(indexDefinition.getDatabase())))
        .thenReturn(collectionInfos);

    when(this.configManager.getLiveIndexes()).thenReturn(indexDefinitions);
    when(this.authoritativeIndexCatalog.listIndexDefinitions())
        .thenReturn(List.of(newIndexDefinition));

    this.communityConfigUpdater.update();

    verify(this.configManager).update(List.of(), List.of(newIndexDefinition), List.of(), Set.of());
    verify(this.authoritativeIndexCatalog)
        .updateIndexDefinition(AuthoritativeIndexKey.from(indexDefinition), newIndexDefinition);
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void testUpdatesVectorViewWithValidDefinition()
      throws CheckedMongoException, MetadataServiceException {
    var viewDefinition =
        ViewDefinition.existing(
            "myView",
            List.of(new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(420)))));
    var indexDefinition =
        VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
            .view(viewDefinition)
            .build();
    var indexDefinitions = List.of((IndexDefinition) indexDefinition);

    var newViewPipeline =
        List.of(new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(420))));
    var newViewDefinition = ViewDefinition.existing("myView", newViewPipeline);
    var newIndexDefinition =
        VectorIndexDefinitionBuilder.from(indexDefinition).view(newViewDefinition).build();
    var collectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(indexDefinition.getDatabase(), "myView"),
                new MongoDbCollectionInfo.View(
                    "myView",
                    new MongoDbCollectionInfo.View.Options(
                        indexDefinition.getLastObservedCollectionName(), newViewPipeline)),
                new MongoNamespace(
                    indexDefinition.getDatabase(), indexDefinition.getLastObservedCollectionName()),
                new MongoDbCollectionInfo.Collection(
                    indexDefinition.getLastObservedCollectionName(),
                    new MongoDbCollectionInfo.Collection.Info(
                        indexDefinition.getCollectionUuid()))));
    when(this.mongoDbMetadataClient.resolveCollectionInfos(Set.of(indexDefinition.getDatabase())))
        .thenReturn(collectionInfos);

    when(this.configManager.getLiveIndexes()).thenReturn(indexDefinitions);
    when(this.authoritativeIndexCatalog.listIndexDefinitions())
        .thenReturn(List.of(newIndexDefinition));

    this.communityConfigUpdater.update();

    verify(this.configManager).update(List.of(newIndexDefinition), List.of(), List.of(), Set.of());
    verify(this.authoritativeIndexCatalog)
        .updateIndexDefinition(AuthoritativeIndexKey.from(indexDefinition), newIndexDefinition);
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void testUpdatesViewWithInvalidDefinition()
      throws CheckedMongoException, MetadataServiceException {
    var viewDefinition =
        ViewDefinition.existing(
            "myView",
            List.of(new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(420)))));
    var indexDefinition =
        SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
            .view(viewDefinition)
            .build();
    var indexDefinitions = List.of((IndexDefinition) indexDefinition);

    var newViewPipeline =
        List.of(new BsonDocument("$project", new BsonDocument("a", new BsonInt32(1))));
    var collectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(indexDefinition.getDatabase(), "myView"),
                new MongoDbCollectionInfo.View(
                    "myView",
                    new MongoDbCollectionInfo.View.Options(
                        indexDefinition.getLastObservedCollectionName(), newViewPipeline)),
                new MongoNamespace(
                    indexDefinition.getDatabase(), indexDefinition.getLastObservedCollectionName()),
                new MongoDbCollectionInfo.Collection(
                    indexDefinition.getLastObservedCollectionName(),
                    new MongoDbCollectionInfo.Collection.Info(
                        indexDefinition.getCollectionUuid()))));
    when(this.mongoDbMetadataClient.resolveCollectionInfos(Set.of(indexDefinition.getDatabase())))
        .thenReturn(collectionInfos);

    when(this.configManager.getLiveIndexes()).thenReturn(indexDefinitions);
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of());

    this.communityConfigUpdater.update();

    verify(this.configManager).update(List.of(), List.of(), List.of(), Set.of());
    verify(this.authoritativeIndexCatalog, never()).updateIndexDefinition(any(), any());
    verify(this.authoritativeIndexCatalog).deleteIndex(AuthoritativeIndexKey.from(indexDefinition));
  }

  @Test
  public void testMockResolveCollectionInfosOnDirectMongod() throws Exception {
    var mockMongoDbMetadataClient = mock(MongoDbMetadataClient.class);
    var mockConfigManager = mock(ConfigManager.class);
    var mockAuthoritativeIndexCatalog = mock(AuthoritativeIndexCatalog.class);

    String mockDatabase = "testDatabase";
    var collectionUuid = UUID.randomUUID();

    var mockCollectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(mockDatabase, "collectionName"),
                new MongoDbCollectionInfo.Collection(
                    "collectionName", new MongoDbCollectionInfo.Collection.Info(collectionUuid))));

    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database(mockDatabase)
            .lastObservedCollectionName("collectionName")
            .collectionUuid(collectionUuid)
            .synonyms(List.of())
            .dynamicMapping()
            .build();

    IndexDefinitionGeneration definitionGeneration =
        SearchIndexDefinitionGenerationBuilder.create(
            indexDefinition, Generation.CURRENT, Collections.emptyList());

    Index index = mockIndex(indexDefinition);

    IndexGeneration indexGeneration = new IndexGeneration(index, definitionGeneration);
    indexGeneration
        .getIndex()
        .setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    when(mockConfigManager.getLiveIndexGenerations()).thenReturn(List.of(indexGeneration));
    when(mockMongoDbMetadataClient.resolveCollectionInfosOnDirectMongod(Set.of(mockDatabase)))
        .thenReturn(mockCollectionInfos);

    var mockInitialSyncHostProvider = mock(InitialSyncHostProvider.class);
    when(mockInitialSyncHostProvider.getMongodInitialSyncConnection()).thenReturn(Optional.empty());

    var communityConfigUpdater =
        new CommunityConfigUpdater(
            mockAuthoritativeIndexCatalog,
            mockMongoDbMetadataClient,
            mockConfigManager,
            this.featureFlags,
            testSyncSourceConfig(),
            mockInitialSyncHostProvider);

    communityConfigUpdater.update();
    verify(mockConfigManager).update(List.of(), List.of(), List.of(), Set.of(collectionUuid));

    // Verify no updates were issued to the AuthoritativeIndexCatalog
    verify(mockAuthoritativeIndexCatalog, never()).updateIndexDefinition(any(), any());
    verify(mockAuthoritativeIndexCatalog, never()).deleteIndex(any());
  }

  // ---- getRefreshedSyncSourceConfig: mongod host refresh ----

  @Test
  public void testMongodHostUnchangedWhenStillValid() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1),
            this.initialSyncHostProvider);
    try {
      updater.update();
    } finally {
      updater.close();
    }

    // Host still valid — getMongodInitialSyncConnection must not be called.
    verify(this.initialSyncHostProvider, never()).getMongodInitialSyncConnection();

    // The original MONGOD_HOST_1 must be propagated unchanged.
    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongodSingleHostReplicationUri.isPresent()).isTrue();
    assertThat(captor.getValue().mongodSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("rs1.local:27017");
  }

  @Test
  public void testMongodHostRefreshedWhenInvalid() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.of(MONGOD_HOST_2));
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1),
            this.initialSyncHostProvider);
    try {
      for (int i = 0; i < 5; i++) {
        updater.update();
      }
    } finally {
      updater.close();
    }

    // Stale host retained for calls 1–4; fresh host applied only at MISS_THRESHOLD (call 5).
    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient, times(5)).maybeUpdateSyncSource(captor.capture());
    for (int i = 0; i < 4; i++) {
      assertThat(captor.getAllValues().get(i).mongodSingleHostReplicationUri.get().uri().getHosts())
          .containsExactly("rs1.local:27017");
    }
    assertThat(captor.getAllValues().get(4).mongodSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("rs2.local:27018");
  }

  @Test
  public void testMongodFreshHostNotAppliedBelowThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.of(MONGOD_HOST_2));
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1),
            this.initialSyncHostProvider);
    try {
      // Single miss with fresh host available: below threshold, stale host must be retained.
      updater.update();
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongodSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("rs1.local:27017");
  }

  @Test
  public void testMongodHostPopulatedWhenPreviouslyEmpty() throws Exception {
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.of(MONGOD_HOST_1));
    // syncSourceConfig from setUp has no mongodSingleHostReplicationUri.
    this.communityConfigUpdater.update();

    // isMongodHostStillValid must not be called when the URI starts empty.
    verify(this.initialSyncHostProvider, never()).isMongodHostStillValid(any());

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongodSingleHostReplicationUri.isPresent()).isTrue();
    assertThat(captor.getValue().mongodSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("rs1.local:27017");
  }

  @Test
  public void testMongodStaleUriRetainedBelowMissThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.empty());
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1),
            this.initialSyncHostProvider);
    try {
      // Single miss: below the threshold — stale URI must be retained.
      updater.update();
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongodSingleHostReplicationUri.isPresent()).isTrue();
    assertThat(captor.getValue().mongodSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("rs1.local:27017");
  }

  @Test
  public void testMongodUriClearedAfterExceedingMissThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongodInitialSyncConnection())
        .thenReturn(Optional.empty());
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1),
            this.initialSyncHostProvider);
    try {
      for (int i = 0; i < 5; i++) {
        updater.update();
      }
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient, times(5)).maybeUpdateSyncSource(captor.capture());
    for (int i = 0; i < 4; i++) {
      assertThat(captor.getAllValues().get(i).mongodSingleHostReplicationUri.isPresent()).isTrue();
    }
    assertThat(captor.getAllValues().get(4).mongodSingleHostReplicationUri.isPresent()).isFalse();
  }

  // ---- getRefreshedSyncSourceConfig: mongos host refresh (sharded) ----

  @Test
  public void testMongosHostRefreshedWhenInvalid() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    when(this.initialSyncHostProvider.isMongosHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongosInitialSyncConnection())
        .thenReturn(Optional.of(MONGOS_HOST_2));
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            shardedConfigWithHosts(Optional.of(MONGOD_HOST_1), Optional.of(MONGOS_HOST)),
            this.initialSyncHostProvider);
    try {
      for (int i = 0; i < 5; i++) {
        updater.update();
      }
    } finally {
      updater.close();
    }

    // Stale host retained for calls 1–4; fresh host applied only at MISS_THRESHOLD (call 5).
    verify(this.initialSyncHostProvider, times(5)).getMongosInitialSyncConnection();
    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient, times(5)).maybeUpdateSyncSource(captor.capture());
    for (int i = 0; i < 4; i++) {
      assertThat(
              captor.getAllValues().get(i).mongosSingleHostReplicationUri.get().uri().getHosts())
          .containsExactly("mongos.local:27019");
    }
    assertThat(captor.getAllValues().get(4).mongosSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("mongos2.local:27020");
  }

  @Test
  public void testMongosFreshHostNotAppliedBelowThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    when(this.initialSyncHostProvider.isMongosHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongosInitialSyncConnection())
        .thenReturn(Optional.of(MONGOS_HOST_2));
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            shardedConfigWithHosts(Optional.of(MONGOD_HOST_1), Optional.of(MONGOS_HOST)),
            this.initialSyncHostProvider);
    try {
      // Single miss with fresh host available: below threshold, stale host must be retained.
      updater.update();
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongosSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("mongos.local:27019");
  }

  @Test
  public void testMongosStaleUriRetainedBelowMissThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    when(this.initialSyncHostProvider.isMongosHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongosInitialSyncConnection())
        .thenReturn(Optional.empty());
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            shardedConfigWithHosts(Optional.of(MONGOD_HOST_1), Optional.of(MONGOS_HOST)),
            this.initialSyncHostProvider);
    try {
      // Single miss: below the threshold — stale URI must be retained.
      updater.update();
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient).maybeUpdateSyncSource(captor.capture());
    assertThat(captor.getValue().mongosSingleHostReplicationUri.isPresent()).isTrue();
    assertThat(captor.getValue().mongosSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("mongos.local:27019");
  }

  @Test
  public void testMongosUriClearedAfterExceedingMissThreshold() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    when(this.initialSyncHostProvider.isMongosHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongosInitialSyncConnection())
        .thenReturn(Optional.empty());
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            shardedConfigWithHosts(Optional.of(MONGOD_HOST_1), Optional.of(MONGOS_HOST)),
            this.initialSyncHostProvider);
    try {
      // MISS_THRESHOLD consecutive misses — URI must be cleared on the last one.
      for (int i = 0; i < 5; i++) {
        updater.update();
      }
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient, times(5)).maybeUpdateSyncSource(captor.capture());
    // First 4 calls: stale host retained.
    for (int i = 0; i < 4; i++) {
      assertThat(captor.getAllValues().get(i).mongosSingleHostReplicationUri.isPresent()).isTrue();
    }
    // 5th call: threshold exceeded — URI cleared.
    assertThat(captor.getAllValues().get(4).mongosSingleHostReplicationUri.isPresent()).isFalse();
  }

  @Test
  public void testMongosHostMissCountResetsOnRecovery() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    when(this.initialSyncHostProvider.isMongosHostStillValid(any())).thenReturn(false);
    when(this.initialSyncHostProvider.getMongosInitialSyncConnection())
        .thenReturn(Optional.empty())          // calls 1–4: no fresh host
        .thenReturn(Optional.empty())
        .thenReturn(Optional.empty())
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(MONGOS_HOST_2)) // call 5: threshold reached, fresh host found
        .thenReturn(Optional.empty());          // call 6: single miss after reset
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            shardedConfigWithHosts(Optional.of(MONGOD_HOST_1), Optional.of(MONGOS_HOST)),
            this.initialSyncHostProvider);
    try {
      for (int i = 0; i < 6; i++) {
        updater.update();
      }
    } finally {
      updater.close();
    }

    var captor = ArgumentCaptor.forClass(SyncSourceConfig.class);
    verify(this.mongoDbMetadataClient, times(6)).maybeUpdateSyncSource(captor.capture());
    // Calls 1–4: stale MONGOS_HOST retained while miss count is below MISS_THRESHOLD.
    for (int i = 0; i < 4; i++) {
      assertThat(captor.getAllValues().get(i).mongosSingleHostReplicationUri.get().uri().getHosts())
          .containsExactly("mongos.local:27019");
    }
    // Call 5: threshold exactly reached — MONGOS_HOST_2 applied, miss count resets to 0.
    assertThat(captor.getAllValues().get(4).mongosSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("mongos2.local:27020");
    // Call 6: single miss after reset — MONGOS_HOST_2 retained (count = 1, below threshold).
    assertThat(captor.getAllValues().get(5).mongosSingleHostReplicationUri.get().uri().getHosts())
        .containsExactly("mongos2.local:27020");
  }

  @Test
  public void testMongosHostNotRefreshedForNonShardedConfig() throws Exception {
    when(this.initialSyncHostProvider.isMongodHostStillValid(any())).thenReturn(true);
    var updater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags,
            configWithMongodHost(MONGOD_HOST_1), // isSharded=false
            this.initialSyncHostProvider);
    try {
      updater.update();
    } finally {
      updater.close();
    }

    // Non-sharded: mongos validity and selection must never be consulted.
    verify(this.initialSyncHostProvider, never()).isMongosHostStillValid(any());
    verify(this.initialSyncHostProvider, never()).getMongosInitialSyncConnection();
  }
}
