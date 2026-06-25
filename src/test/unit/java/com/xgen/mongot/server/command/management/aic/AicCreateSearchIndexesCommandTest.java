package com.xgen.mongot.server.command.management.aic;

import static com.xgen.testing.BsonTestUtils.bson;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_UUID;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.DATABASE_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.INDEX_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VECTOR_DIMENSIONS;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VECTOR_INDEX_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VECTOR_PATH;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VECTOR_QUANTIZATION;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VECTOR_SIMILARITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.catalogservice.schema.AuthoritativeIndexKey;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.config.IndexDefinitionConfigBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AicCreateSearchIndexesCommandTest {

  @Test
  public void testCreateSearchIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    var definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    var indexesCreated = response.getArray("indexesCreated");
    assertEquals(1, indexesCreated.size());

    var createdNamedIndex = indexesCreated.getFirst().asDocument();
    assertEquals(INDEX_NAME, createdNamedIndex.getString("name").getValue());

    var expected =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId(createdNamedIndex.getString("id").getValue()))
            .database(DATABASE_NAME)
            .collectionUuid(COLLECTION_UUID)
            .name(INDEX_NAME)
            .lastObservedCollectionName(COLLECTION_NAME)
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .dynamicMapping()
            .build();
    ArgumentCaptor<SearchIndexDefinition> captor =
        ArgumentCaptor.forClass(SearchIndexDefinition.class);
    ArgumentCaptor<BsonDocument> rawDefCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    verify(mockAic)
        .createIndex(
            eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)),
            captor.capture(),
            rawDefCaptor.capture());
    assertNotNull(captor.getValue());
    assertTrue(IndexMapper.areEquivalent(expected, captor.getValue()));
    assertEquals(0L, (long) captor.getValue().getDefinitionVersion().orElseThrow());
    assertTrue(captor.getValue().getDefinitionVersionCreatedAt().isPresent());
    assertEquals(definition.indexes().getFirst().definitionBson(), rawDefCaptor.getValue());
  }

  @Test
  public void testCreateVectorIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    var definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withVectorIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    var indexesCreated = response.getArray("indexesCreated");
    assertEquals(1, indexesCreated.size());

    var createdNamedIndex = indexesCreated.getFirst().asDocument();
    assertEquals(VECTOR_INDEX_NAME, createdNamedIndex.getString("name").getValue());

    var expected =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId(createdNamedIndex.getString("id").getValue()))
            .database(DATABASE_NAME)
            .collectionUuid(COLLECTION_UUID)
            .name(VECTOR_INDEX_NAME)
            .lastObservedCollectionName(COLLECTION_NAME)
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .withVectorFieldDefaultOptions(
                VECTOR_PATH, VECTOR_DIMENSIONS, VECTOR_SIMILARITY, VECTOR_QUANTIZATION)
            .build();
    ArgumentCaptor<VectorIndexDefinition> captor =
        ArgumentCaptor.forClass(VectorIndexDefinition.class);
    verify(mockAic)
        .createIndex(
            eq(new AuthoritativeIndexKey(COLLECTION_UUID, VECTOR_INDEX_NAME)),
            captor.capture(),
            any());
    assertNotNull(captor.getValue());
    assertTrue(IndexMapper.areEquivalent(expected, captor.getValue()));
    assertEquals(0L, (long) captor.getValue().getDefinitionVersion().orElseThrow());
    assertTrue(captor.getValue().getDefinitionVersionCreatedAt().isPresent());
  }

  @Test
  public void testDedupeExistingIndexes() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .indexId(new ObjectId())
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .name(INDEX_NAME)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .dynamicMapping()
                    .build()));

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    var indexesCreated = response.getArray("indexesCreated");
    assertEquals(1, indexesCreated.size());
    assertEquals(INDEX_NAME, indexesCreated.getFirst().asDocument().getString("name").getValue());

    verify(mockAic, never()).createIndex(any(), any(), any());
    verify(mockAic, never()).updateIndex(any(), any(), any());
  }

  @Test
  public void testExistingIndexesDifferentDefinitionThrowsError() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .indexId(new ObjectId())
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .name(INDEX_NAME)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .mappings(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "subdoc",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build()));

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    assertEquals(Errors.INDEX_ALREADY_EXISTS.code, response.getInt32("code").getValue());
    assertEquals(Errors.INDEX_ALREADY_EXISTS.name, response.getString("codeName").getValue());
    assertTrue(
        response
            .getString("errmsg")
            .getValue()
            .startsWith("Index " + INDEX_NAME + " already exists"));
    assertFalse(response.containsKey("indexesCreated"));

    verify(mockAic, never()).createIndex(any(), any(), any());
    verify(mockAic, never()).updateIndex(any(), any(), any());
  }

  @Test
  public void testExistingIndexesAfterValidation() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    doReturn(
            List.of(),
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .indexId(new ObjectId())
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .name(INDEX_NAME)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build()))
        .when(mockAic)
        .listIndexDefinitions(COLLECTION_UUID);
    doThrow(
            MetadataServiceException.createFailed(
                new MongoWriteException(
                    new WriteError(11000, "duplicate key", new BsonDocument()),
                    new ServerAddress("localhost"))))
        .when(mockAic)
        .createIndex(eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)), any(), any());

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    var indexesCreated = response.getArray("indexesCreated");
    assertEquals(1, indexesCreated.size());
  }

  @Test
  public void testExistingIndexesDifferentDefinitionAfterValidationThrowsError()
      throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    doReturn(
            List.of(),
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .indexId(new ObjectId())
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .name(INDEX_NAME)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .mappings(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "subdoc",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build()))
        .when(mockAic)
        .listIndexDefinitions(COLLECTION_UUID);
    doThrow(
            MetadataServiceException.createFailed(
                new MongoWriteException(
                    new WriteError(11000, "duplicate key", new BsonDocument()),
                    new ServerAddress("localhost"))))
        .when(mockAic)
        .createIndex(eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)), any(), any());

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    assertEquals(Errors.INDEX_ALREADY_EXISTS.code, response.getInt32("code").getValue());
    assertEquals(Errors.INDEX_ALREADY_EXISTS.name, response.getString("codeName").getValue());
    assertFalse(response.containsKey("indexesCreated"));
  }

  @Test
  public void testNonMetadataServiceExceptionDuringCreateIndexFails()
      throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID)).thenReturn(List.of());

    doThrow(new RuntimeException("Connection timeout to metadata service"))
        .when(mockAic)
        .createIndex(eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)), any(), any());

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    // Verify that the command fails with COMMAND_FAILED error
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(Errors.COMMAND_FAILED.name, response.getString("codeName").getValue());
    assertTrue(
        response.getString("errmsg").getValue().contains("Connection timeout to metadata service"));
    assertFalse(response.containsKey("indexesCreated"));
  }

  @Test
  public void testNonDuplicateKeyMetadataServiceExceptionDuringCreateIndexFails()
      throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID)).thenReturn(List.of());

    // Simulate a non-duplicate-key write error (e.g., write concern error)
    var failedException =
        MetadataServiceException.createFailed(
            new MongoWriteException(
                new WriteError(50, "write concern error", new BsonDocument()),
                new ServerAddress("localhost")));
    doThrow(failedException)
        .when(mockAic)
        .createIndex(eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)), any(), any());

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    // Verify that the command fails with COMMAND_FAILED error
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(Errors.COMMAND_FAILED.name, response.getString("codeName").getValue());
    assertTrue(response.getString("errmsg").getValue().contains("write concern error"));
    assertFalse(response.containsKey("indexesCreated"));
  }

  @Test
  public void testPartialFailureWithMultipleIndexes() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID)).thenReturn(List.of());

    // Create a definition with two indexes
    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex() // First index with INDEX_NAME
                .withVectorIndex() // Second index with VECTOR_INDEX_NAME
                .buildSearchIndexCommand();

    // First index succeeds, second index fails with transient error
    var transientException =
        MetadataServiceException.createTransient(new RuntimeException("Transient network error"));
    doThrow(transientException)
        .when(mockAic)
        .createIndex(
            eq(new AuthoritativeIndexKey(COLLECTION_UUID, VECTOR_INDEX_NAME)), any(), any());

    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    // Verify that the entire command fails (no partial success)
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(Errors.COMMAND_FAILED.name, response.getString("codeName").getValue());
    assertTrue(response.getString("errmsg").getValue().contains("Transient network error"));
    assertFalse(response.containsKey("indexesCreated"));

    // Verify that the first index was attempted to be created
    verify(mockAic)
        .createIndex(eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)), any(), any());
  }

  @Test
  public void missingAnalyzerFailsToCreate() throws BsonParseException, MetadataServiceException {
    this.failsWithCommandFailed(
        "references non-existent analyzers: non-standard",
        bson(
            """
            {
              "name": "some-index",
              "type": "search",
              "definition": {
                "mappings": {"dynamic":  true},
                "analyzer": "non-standard",
              }
            }
            """));
  }

  @Test
  public void mismatchedNestedRootFailsToCreate()
      throws BsonParseException, MetadataServiceException {
    this.failsWithCommandFailed(
        "nestedRoot \"sections\" does not match any vector field path " + "in the index definition",
        bson(
            """
            {
              "name": "bad-nested-root",
              "type": "vectorSearch",
              "definition": {
                "fields": [
                  {"path": "topLevelVector", "type": "vector", "numDimensions": 128,
                  "similarity": "cosine"}
                ],
                "nestedRoot": "sections"
              }
            }
            """));
  }

  private void failsWithCommandFailed(String expectedMessage, BsonDocument indexDefinition)
      throws BsonParseException, MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID)).thenReturn(List.of());

    NamedSearchIndex index;
    try (var parser = BsonDocumentParser.fromRoot(indexDefinition).build()) {
      index = NamedSearchIndex.fromBson(parser);
    }

    CreateSearchIndexesCommandDefinition definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .addIndex(index)
                .buildSearchIndexCommand();

    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(expectedMessage, response.getString("errmsg").getValue());
  }

  @Test
  public void testTopologyMismatchExceptionReturnsCommandFailed() throws Exception {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var mockGuard = mock(CatalogAccessGuard.class);
    doThrow(new TopologyMismatchException("router topology mismatch"))
        .when(mockGuard)
        .requireTopologyMatch();

    var definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mockGuard,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(Errors.COMMAND_FAILED.name, response.getString("codeName").getValue());
    assertTrue(response.getString("errmsg").getValue().contains("router topology mismatch"));
    verify(mockAic, never()).createIndex(any(), any(), any());
  }

  @Test
  public void testCheckedMongoExceptionReturnsCommandFailed() throws Exception {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var mockGuard = mock(CatalogAccessGuard.class);
    doThrow(new CheckedMongoException(new MongoException("mongo connection failed")))
        .when(mockGuard)
        .requireTopologyMatch();

    var definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mockGuard,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    var response = command.run();

    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(Errors.COMMAND_FAILED.name, response.getString("codeName").getValue());
    assertTrue(response.getString("errmsg").getValue().contains("mongo connection failed"));
    verify(mockAic, never()).createIndex(any(), any(), any());
  }

  @Test
  public void maybeLoadShed_alwaysReturnsFalse() {
    AuthoritativeIndexCatalog mockAic = mock(AuthoritativeIndexCatalog.class);
    var definition =
        (CreateSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.createIndexes()
                .withDynamicIndex()
                .buildSearchIndexCommand();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfig.getDefault());
    assertFalse(command.maybeLoadShed());
  }

  @Test
  public void testCreateSearchIndexExceedingMaxNestingLevelRejected()
      throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var definition = twoLayerEmbeddedDocumentsSearchIndex();
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfigBuilder.builder().maxEmbeddedDocumentsNestingLevel(1).build());

    var response = command.run();

    assertEquals(Errors.BAD_VALUE.code, response.getInt32("code").getValue());
    assertEquals(Errors.BAD_VALUE.name, response.getString("codeName").getValue());
    assertTrue(
        response
            .getString("errmsg")
            .getValue()
            .contains(
                "defines 2 layers of embeddedDocuments fields; "
                    + "indexes may nest a maximum of 1 embeddedDocuments inside one another"));
    verify(mockAic, never()).createIndex(any(), any(), any());
  }

  @Test
  public void testCreateSearchIndexWithinConfiguredMaxNestingLevelAllowed()
      throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var definition = twoLayerEmbeddedDocumentsSearchIndex();
    // A configured limit of 2 admits the same definition that a limit of 1 rejects, proving the
    // configured value is honored on the community index-creation path.
    var command =
        new AicCreateSearchIndexesCommand(
            mockAic,
            mock(CatalogAccessGuard.class),
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            definition,
            IndexDefinitionConfigBuilder.builder().maxEmbeddedDocumentsNestingLevel(2).build());

    var response = command.run();

    assertEquals(1, response.getArray("indexesCreated").size());
    verify(mockAic).createIndex(any(), any(), any());
  }

  /**
   * Builds a create command for a single search index whose mappings nest two layers of {@code
   * embeddedDocuments} fields.
   */
  private static CreateSearchIndexesCommandDefinition twoLayerEmbeddedDocumentsSearchIndex() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "foo",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                            .dynamic(true)
                            .field(
                                "bar",
                                FieldDefinitionBuilder.builder()
                                    .embeddedDocuments(
                                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    var index =
        new NamedSearchIndex(
            INDEX_NAME,
            IndexDefinition.Type.SEARCH,
            new UserSearchIndexDefinition(
                Optional.empty(),
                Optional.empty(),
                mappings,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                1));
    return (CreateSearchIndexesCommandDefinition)
        ManageSearchIndexCommandDefinitionBuilder.createIndexes()
            .addIndex(index)
            .buildSearchIndexCommand();
  }
}
