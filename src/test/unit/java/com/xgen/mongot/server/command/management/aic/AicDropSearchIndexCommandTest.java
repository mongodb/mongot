package com.xgen.mongot.server.command.management.aic;

import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_UUID;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.DATABASE_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.INDEX_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VIEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AicDropSearchIndexCommandTest {
  @Test
  public void testDropSearchIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return an existing search index.
    var indexId = new ObjectId();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(INDEX_NAME)
                    .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build()));

    var definition =
        (DropSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.dropIndex()
                .withIndexName(INDEX_NAME)
                .buildSearchIndexCommand();
    AicDropSearchIndexCommand command =
        new AicDropSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();

    assertEquals(1, response.getInt32("ok").getValue());
    verify(mockAic).deleteIndex(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME));
  }

  @Test
  public void testDropSearchIndexById() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);

    // Return an existing search index.
    var indexId = new ObjectId();
    when(mockAic.listIndexDefinitions(COLLECTION_UUID))
        .thenReturn(
            List.of(
                SearchIndexDefinitionBuilder.builder()
                    .database(DATABASE_NAME)
                    .collectionUuid(COLLECTION_UUID)
                    .lastObservedCollectionName(COLLECTION_NAME)
                    .indexId(indexId)
                    .name(INDEX_NAME)
                    .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build()));

    var definition =
        (DropSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.dropIndex()
                .withIndexId(indexId)
                .buildSearchIndexCommand();
    AicDropSearchIndexCommand command =
        new AicDropSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();

    assertEquals(1, response.getInt32("ok").getValue());
    verify(mockAic).deleteIndex(new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME));
  }

  @Test
  public void testDropMissingSearchIndex() throws MetadataServiceException {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    when(mockAic.listIndexDefinitions(any())).thenReturn(List.of());

    var definition =
        (DropSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.dropIndex()
                .withIndexName(INDEX_NAME)
                .buildSearchIndexCommand();
    AicDropSearchIndexCommand command =
        new AicDropSearchIndexCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();

    assertEquals(0, response.getInt32("ok").getValue());
    verify(mockAic, never()).deleteIndex(any());
  }

  @Test
  public void maybeLoadShed_alwaysReturnsFalse() {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var definition =
        (DropSearchIndexCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.dropIndex()
                .withIndexName(INDEX_NAME)
                .buildSearchIndexCommand();
    var command =
        new AicDropSearchIndexCommand(
            mockAic, DATABASE_NAME, COLLECTION_UUID, COLLECTION_NAME,
            Optional.of(VIEW), definition);
    assertFalse(command.maybeLoadShed());
  }
}
