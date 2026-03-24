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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserSearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserVectorIndexDefinitionBuilder;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AicUpdateSearchIndexCommandTest {

  @Test
  public void testUpdateSearchIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return an existing search index.
    var indexId = new ObjectId();
    Instant now = Instant.now();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(INDEX_NAME)
                    .mappings(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "field",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .definitionVersion(1L)
                    .definitionVersionCreatedAt(now)
                    .build()));

    // Make the index dynamic, don't specify index type
    var updateDefinition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .withIndexName(INDEX_NAME)
                .withDefinition(
                    UserSearchIndexDefinitionBuilder.builder()
                        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
                        .build())
                .buildSearchIndexCommand();

    var command =
        new AicUpdateSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            updateDefinition);

    // Update command succeeds
    BsonDocument updateResponse = command.run();
    assertEquals(1, updateResponse.getInt32("ok").getValue());

    ArgumentCaptor<SearchIndexDefinition> captor =
        ArgumentCaptor.forClass(SearchIndexDefinition.class);
    ArgumentCaptor<BsonDocument> definitionBsonCaptor = ArgumentCaptor.forClass(BsonDocument.class);
    verify(mockAic)
        .updateIndex(
            eq(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME)),
            captor.capture(),
            definitionBsonCaptor.capture());
    assertNotNull(captor.getValue());
    assertEquals(DATABASE_NAME, captor.getValue().getDatabase());
    assertEquals(COLLECTION_UUID, captor.getValue().getCollectionUuid());
    assertEquals(COLLECTION_NAME, captor.getValue().getLastObservedCollectionName());
    assertEquals(indexId, captor.getValue().getIndexId());
    assertEquals(INDEX_NAME, captor.getValue().getName());
    assertEquals(
        SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
        captor.getValue().getParsedIndexFeatureVersion());
    assertEquals(
        DocumentFieldDefinitionBuilder.builder().dynamic(true).build(),
        captor.getValue().getMappings());
    assertEquals(2L, (long) captor.getValue().getDefinitionVersion().get());
    assertTrue(captor.getValue().getDefinitionVersionCreatedAt().get().isAfter(now));
    assertEquals(updateDefinition.definitionBson(), definitionBsonCaptor.getValue());
  }

  @Test
  public void testUpdateVectorIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return an existing vector index.
    var indexId = new ObjectId();
    Instant now = Instant.now();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                VectorIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(VECTOR_INDEX_NAME)
                    .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
                    .withVectorFieldDefaultOptions(
                        VECTOR_PATH, VECTOR_DIMENSIONS, VECTOR_SIMILARITY, VECTOR_QUANTIZATION)
                    .withDefinitionVersion(Optional.of(1L))
                    .withDefinitionVersionCreatedAt(Optional.of(now))
                    .build()));

    // Try to increase the number of dimensions, don't specify index type
    var updateDefinition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .withIndexName(VECTOR_INDEX_NAME)
                .withDefinition(
                    UserVectorIndexDefinitionBuilder.builder()
                        .addVectorField(
                            VECTOR_DIMENSIONS + 1,
                            VECTOR_SIMILARITY,
                            VECTOR_QUANTIZATION,
                            VECTOR_PATH)
                        .build())
                .buildSearchIndexCommand();

    var command =
        new AicUpdateSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            updateDefinition);

    // Update command succeeds
    BsonDocument updateResponse = command.run();
    assertEquals(1, updateResponse.getInt32("ok").getValue());

    ArgumentCaptor<VectorIndexDefinition> captor =
        ArgumentCaptor.forClass(VectorIndexDefinition.class);
    verify(mockAic)
        .updateIndex(
            eq(new AuthoritativeIndexKey(COLLECTION_UUID, VECTOR_INDEX_NAME)),
            captor.capture(),
            any());
    assertNotNull(captor.getValue());
    assertEquals(DATABASE_NAME, captor.getValue().getDatabase());
    assertEquals(COLLECTION_UUID, captor.getValue().getCollectionUuid());
    assertEquals(COLLECTION_NAME, captor.getValue().getLastObservedCollectionName());
    assertEquals(indexId, captor.getValue().getIndexId());
    assertEquals(VECTOR_INDEX_NAME, captor.getValue().getName());
    assertEquals(
        SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
        captor.getValue().getParsedIndexFeatureVersion());
    assertTrue(
        captor
            .getValue()
            .getFields()
            .contains(
                new VectorDataFieldDefinition(
                    FieldPath.parse(VECTOR_PATH),
                    new VectorFieldSpecification(
                        VECTOR_DIMENSIONS + 1,
                        VECTOR_SIMILARITY,
                        VECTOR_QUANTIZATION,
                        new VectorIndexingAlgorithm.HnswIndexingAlgorithm()))));
    assertEquals(2L, (long) captor.getValue().getDefinitionVersion().get());
    assertTrue(captor.getValue().getDefinitionVersionCreatedAt().get().isAfter(now));
  }

  @Test
  public void testUpdatingIndexTypeFails() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return an existing vector index.
    var indexId = new ObjectId();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                VectorIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(VECTOR_INDEX_NAME)
                    .withVectorFieldDefaultOptions(
                        VECTOR_PATH, VECTOR_DIMENSIONS, VECTOR_SIMILARITY, VECTOR_QUANTIZATION)
                    .build()));

    // Try to update to a search index
    var updateDefinition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .searchIndex(VECTOR_INDEX_NAME)
                .withDefinition(
                    UserSearchIndexDefinitionBuilder.builder()
                        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
                        .build())
                .buildSearchIndexCommand();
    var command =
        new AicUpdateSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            updateDefinition);

    // Update command fails
    BsonDocument updateResponse = command.run();
    assertEquals(0, updateResponse.getInt32("ok").getValue());
    assertEquals(
        Errors.INVALID_INDEX_SPECIFICATION_OPTION.code, updateResponse.getInt32("code").getValue());
    assertEquals(
        "An index's type cannot be changed", updateResponse.getString("errmsg").getValue());
  }

  @Test
  public void testUpdatingIndexWrongType() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return some existing index.
    var indexId = new ObjectId();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                VectorIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(VECTOR_INDEX_NAME)
                    .withVectorFieldDefaultOptions(
                        VECTOR_PATH, VECTOR_DIMENSIONS, VECTOR_SIMILARITY, VECTOR_QUANTIZATION)
                    .build()));

    // Try to update to a search index with a vector definition
    var updateDefinition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .searchIndex(VECTOR_INDEX_NAME)
                .withDefinition(
                    UserVectorIndexDefinitionBuilder.builder()
                        .addVectorField(
                            VECTOR_DIMENSIONS, VECTOR_SIMILARITY, VECTOR_QUANTIZATION, VECTOR_PATH)
                        .build())
                .buildSearchIndexCommand();
    var command =
        new AicUpdateSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.empty(),
            updateDefinition);

    // Update command fails
    BsonDocument updateResponse = command.run();
    assertEquals(0, updateResponse.getInt32("ok").getValue());
    assertEquals("\"mappings\" is required", updateResponse.getString("errmsg").getValue());
  }

  @Test
  public void missingAnalyzerFailsToUpdate() throws BsonParseException, MetadataServiceException {
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

  private void failsWithCommandFailed(String expectedMessage, BsonDocument indexDefinition)
      throws BsonParseException, MetadataServiceException {
    NamedSearchIndex index;
    try (var parser = BsonDocumentParser.fromRoot(indexDefinition).build()) {
      index = NamedSearchIndex.fromBson(parser);
    }

    var objectId = new ObjectId();
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(objectId)
                    .name(index.name())
                    .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build()));

    var updateDefinition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .searchIndex(index.name())
                .withDefinition(index.definition())
                .buildSearchIndexCommand();

    var command = makeTarget(mockAic, updateDefinition);
    var response = command.run();
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals(expectedMessage, response.getString("errmsg").getValue());
  }

  private static @NotNull AicUpdateSearchIndexCommand makeTarget(
      AuthoritativeIndexCatalog mockAic, UpdateSearchIndexCommandDefinition updateDefinition) {
    return new AicUpdateSearchIndexCommand(
        mockAic,
        DATABASE_NAME,
        COLLECTION_UUID,
        COLLECTION_NAME,
        Optional.empty(),
        updateDefinition);
  }

  @Test
  public void maybeLoadShed_alwaysReturnsFalse() {
    AuthoritativeIndexCatalog mockAic = mock(AuthoritativeIndexCatalog.class);
    var definition =
        (UpdateSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.updateIndex()
                .withIndexName(INDEX_NAME)
                .withDefinition(
                    UserSearchIndexDefinitionBuilder.builder()
                        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
                        .build())
                .buildSearchIndexCommand();
    var command =
        new AicUpdateSearchIndexCommand(
            mockAic, DATABASE_NAME, COLLECTION_UUID, COLLECTION_NAME, Optional.empty(), definition);
    assertFalse(command.maybeLoadShed());
  }
}
