package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalogservice.IndexEntry.Fields;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
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
  public void createIndex(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException {
    this.aicMongoClient.insert(
        new IndexEntry(indexKey, definition.getIndexId(), CURRENT_WRITTEN_VERSION, definition));
  }

  @Override
  public void updateIndex(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException {
    IndexEntry entry =
        new IndexEntry(indexKey, definition.getIndexId(), CURRENT_WRITTEN_VERSION, definition);
    this.aicMongoClient.replace(entry.keyBson(), entry, false);
  }

  @Override
  public void deleteIndex(AuthoritativeIndexKey indexKey) throws MetadataServiceException {
    this.aicMongoClient.delete(IndexEntry.keyAsBson(indexKey));
  }

  @Override
  public List<IndexDefinition> listIndexes() {
    return listIndexes(new BsonDocument());
  }

  @Override
  public List<IndexDefinition> listIndexes(UUID collectionUuid) {
    String dottedFieldName =
        String.join(
            ".",
            Fields.INDEX_KEY.getName(),
            AuthoritativeIndexKey.Fields.COLLECTION_UUID.getName());

    return listIndexes(new BsonDocument(dottedFieldName, new BsonBinary(collectionUuid)));
  }

  private List<IndexDefinition> listIndexes(BsonDocument filter) {
    return this.aicMongoClient.list(filter).stream()
        .flatMap(DefaultAuthoritativeIndexCatalog::parseToStream)
        .toList();
  }

  private static Stream<IndexDefinition> parseToStream(BsonDocument document) {
    try {
      return Stream.of(
          IndexEntry.fromBson(BsonDocumentParser.fromRoot(document).build()).definition());
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
}
