package com.xgen.mongot.catalogservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Test;

public class IndexEntryTest {

  private static final UUID COLLECTION_UUID =
      UUID.fromString("beceffcc-66ee-4339-9092-1147075c7602");
  private static final String INDEX_NAME = "test-index";
  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testColl";

  private static AuthoritativeIndexKey key() {
    return new AuthoritativeIndexKey(COLLECTION_UUID, INDEX_NAME);
  }

  private static SearchIndexDefinition minimalDefinition(ObjectId indexId) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(INDEX_NAME)
        .database(DATABASE_NAME)
        .collectionUuid(COLLECTION_UUID)
        .lastObservedCollectionName(COLLECTION_NAME)
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }

  @Test
  public void testToBsonAndFromBsonRoundTrip() throws BsonParseException {
    ObjectId indexId = new ObjectId();
    IndexDefinition definition = minimalDefinition(indexId);
    BsonDocument customerDefinitionBson =
        new BsonDocument("customKey", new BsonString("customValue"));

    IndexEntry entry =
        new IndexEntry(
            key(),
            indexId,
            1,
            definition,
            Optional.of(customerDefinitionBson));

    BsonDocument bson = entry.toBson();
    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      IndexEntry roundTripped = IndexEntry.fromBson(parser);
      assertEquals(entry.indexKey(), roundTripped.indexKey());
      assertEquals(entry.indexId(), roundTripped.indexId());
      assertEquals(entry.schemaVersion(), roundTripped.schemaVersion());
      assertTrue(
          "definition should round-trip equivalently",
          definitionEquivalence(entry.definition(), roundTripped.definition()));
      assertTrue(roundTripped.customerDefinition().isPresent());
      assertEquals(
          customerDefinitionBson,
          roundTripped.customerDefinition().get());
    }
  }

  @Test
  public void testToBsonAndFromBsonRoundTripWithoutCustomerDefinition() throws BsonParseException {
    ObjectId indexId = new ObjectId();
    IndexDefinition definition = minimalDefinition(indexId);

    IndexEntry entry =
        new IndexEntry(key(), indexId, 1, definition, Optional.empty());

    BsonDocument bson = entry.toBson();
    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      IndexEntry roundTripped = IndexEntry.fromBson(parser);
      assertEquals(entry.indexKey(), roundTripped.indexKey());
      assertEquals(entry.indexId(), roundTripped.indexId());
      assertEquals(entry.schemaVersion(), roundTripped.schemaVersion());
      assertTrue(
          "definition should round-trip equivalently",
          definitionEquivalence(entry.definition(), roundTripped.definition()));
      assertTrue(
          "customerDefinition should be empty after round-trip",
          roundTripped.customerDefinition().isEmpty());
    }
  }

  @Test
  public void testKeyAsBson() {
    AuthoritativeIndexKey indexKey = key();
    BsonDocument keyBson = IndexEntry.keyAsBson(indexKey);

    assertTrue(keyBson.containsKey("_id"));
    assertEquals(indexKey.toBson(), keyBson.getDocument("_id"));

    IndexEntry entry =
        new IndexEntry(
            indexKey,
            new ObjectId(),
            1,
            minimalDefinition(new ObjectId()),
            Optional.empty());
    assertEquals(keyBson, entry.keyBson());
  }

  @Test
  public void testUpdateDefinition() {
    ObjectId indexId = new ObjectId();
    IndexDefinition definition = minimalDefinition(indexId);
    BsonDocument updateDoc = IndexEntry.updateDefinition(definition);

    // Updates.set().toBsonDocument() produces {"$set": {"definition": ...}}
    assertTrue(updateDoc.containsKey("$set"));
    BsonDocument setDoc = updateDoc.getDocument("$set");
    assertTrue(setDoc.containsKey("definition"));
    assertEquals(definition.toBson(), setDoc.getDocument("definition"));
  }

  private static boolean definitionEquivalence(IndexDefinition a, IndexDefinition b) {
    return a.toBson().equals(b.toBson());
  }
}
