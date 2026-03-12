package com.xgen.mongot.config.provider.community;

import static com.xgen.testing.mongot.mock.index.SearchIndex.mockIndex;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
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
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CommunityConfigUpdaterTest {

  private AuthoritativeIndexCatalog authoritativeIndexCatalog;
  private MongoDbMetadataClient mongoDbMetadataClient;
  private ConfigManager configManager;
  private FeatureFlags featureFlags;

  private CommunityConfigUpdater communityConfigUpdater;

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

    this.communityConfigUpdater =
        new CommunityConfigUpdater(
            this.authoritativeIndexCatalog,
            this.mongoDbMetadataClient,
            this.configManager,
            this.featureFlags);
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

    var communityConfigUpdater =
        new CommunityConfigUpdater(
            mockAuthoritativeIndexCatalog,
            mockMongoDbMetadataClient,
            mockConfigManager,
            this.featureFlags);

    communityConfigUpdater.update();
    verify(mockConfigManager).update(List.of(), List.of(), List.of(), Set.of(collectionUuid));

    // Verify no updates were issued to the AuthoritativeIndexCatalog
    verify(mockAuthoritativeIndexCatalog, never()).updateIndexDefinition(any(), any());
    verify(mockAuthoritativeIndexCatalog, never()).deleteIndex(any());
  }
}
