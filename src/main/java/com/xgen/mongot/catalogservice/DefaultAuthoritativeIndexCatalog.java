package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalogservice.IndexEntry.Fields;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultAuthoritativeIndexCatalog implements AuthoritativeIndexCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthoritativeIndexCatalog.class);

  private static final int CURRENT_WRITTEN_VERSION = 1;

  private final MetadataClient<IndexEntry> aicMongoClient;

  @VisibleForTesting
  DefaultAuthoritativeIndexCatalog(MetadataClient<IndexEntry> aicMongoClient) {
    this.aicMongoClient = aicMongoClient;
  }

  @Override
  public void createIndex(
      AuthoritativeIndexKey indexKey, IndexDefinition definition, BsonDocument customerDefinition)
      throws MetadataServiceException {
    this.aicMongoClient.insert(
        new IndexEntry(
            indexKey,
            definition.getIndexId(),
            CURRENT_WRITTEN_VERSION,
            definition,
            Optional.of(customerDefinition)));
  }

  @Override
  public void updateIndex(
      AuthoritativeIndexKey indexKey, IndexDefinition definition, BsonDocument customerDefinition)
      throws MetadataServiceException {
    IndexEntry entry =
        new IndexEntry(
            indexKey,
            definition.getIndexId(),
            CURRENT_WRITTEN_VERSION,
            definition,
            Optional.of(customerDefinition));
    this.aicMongoClient.replace(entry.keyBson(), entry, false);
  }

  @Override
  public boolean updateIndexDefinition(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException {
    return this.aicMongoClient
            .updateOne(IndexEntry.keyAsBson(indexKey), IndexEntry.updateDefinition(definition))
            .getMatchedCount()
        == 1;
  }

  @Override
  public void deleteIndex(AuthoritativeIndexKey indexKey) throws MetadataServiceException {
    this.aicMongoClient.delete(IndexEntry.keyAsBson(indexKey));
  }

  @Override
  public List<IndexEntry> listIndexes(UUID collectionUuid) throws MetadataServiceException {
    String dottedFieldName =
        String.join(
            ".",
            Fields.INDEX_KEY.getName(),
            AuthoritativeIndexKey.Fields.COLLECTION_UUID.getName());
    return listIndexes(new BsonDocument(dottedFieldName, new BsonBinary(collectionUuid)));
  }

  @Override
  public List<IndexEntry> listIndexes() throws MetadataServiceException {
    return listIndexes(new BsonDocument());
  }

  private List<IndexEntry> listIndexes(BsonDocument filter) throws MetadataServiceException {
    return this.aicMongoClient.list(filter).stream()
        .flatMap(DefaultAuthoritativeIndexCatalog::parseToStream)
        .toList();
  }

  @Override
  public List<IndexDefinition> listIndexDefinitions() throws MetadataServiceException {
    return listIndexes().stream().map(IndexEntry::definition).toList();
  }

  @Override
  public List<IndexDefinition> listIndexDefinitions(UUID collectionUuid)
      throws MetadataServiceException {
    return listIndexes(collectionUuid).stream().map(IndexEntry::definition).toList();
  }

  private static Stream<IndexEntry> parseToStream(BsonDocument document) {
    try {
      return Stream.of(IndexEntry.fromBson(BsonDocumentParser.fromRoot(document).build()));
    } catch (BsonParseException e) {
      LOG.atWarn()
          .addKeyValue("document", document)
          .setCause(e)
          .log("Ignoring unparsable document from Authoritative Index Catalog");
      return Stream.empty();
    }
  }

  public static DefaultAuthoritativeIndexCatalog create(MongoClient client) {
    return new DefaultAuthoritativeIndexCatalog(new MetadataClient<>(client, COLLECTION_NAME));
  }

  @VisibleForTesting
  static DefaultAuthoritativeIndexCatalog createForTesting(
      MongoClient client, String databaseName) {
    return new DefaultAuthoritativeIndexCatalog(
        new MetadataClient<>(client, databaseName, COLLECTION_NAME));
  }
}
