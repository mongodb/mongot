package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.xgen.mongot.catalogservice.schema.IndexStatsEntry;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultIndexStats implements IndexStats {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultIndexStats.class);

  @VisibleForTesting protected static final String COLLECTION_NAME = "indexStats";

  private final MetadataClient<IndexStatsEntry> client;

  @VisibleForTesting
  DefaultIndexStats(MetadataClient<IndexStatsEntry> client) {
    this.client = client;
  }

  @Override
  public void insert(IndexStatsEntry indexStats) throws MetadataServiceException {
    this.client.insert(indexStats);
  }

  @Override
  public void upsertAll(Set<IndexStatsEntry> indexStats) throws MetadataServiceException {
    Map<BsonDocument, IndexStatsEntry> replaceMap;
    try {
      replaceMap =
          indexStats.stream()
              .collect(
                  Collectors.toMap(
                      i -> IndexStatsEntry.keyAsBson(i.key()),
                      i -> i,
                      (i1, i2) -> {
                        LOG.atWarn()
                            .addKeyValue("key", i1.key())
                            .addKeyValue("entry1", i1)
                            .addKeyValue("entry2", i2)
                            .log("Duplicate index stats entries in provided list");
                        throw new IllegalArgumentException("Duplicate entries in index stats list");
                      }));
    } catch (IllegalArgumentException e) {
      throw MetadataServiceException.createFailed(e);
    }

    this.client.bulkReplace(replaceMap, false, true);
  }

  @Override
  public void delete(BsonDocument filter) throws MetadataServiceException {
    this.client.deleteMany(filter);
  }

  @Override
  public void deleteAll(Set<IndexStatsEntry.IndexStatsKey> keys) throws MetadataServiceException {
    List<BsonDocument> deleteFilters = keys.stream().map(IndexStatsEntry::keyAsBson).toList();
    this.client.bulkDelete(deleteFilters, false);
  }

  @Override
  public List<IndexStatsEntry> list(BsonDocument filter) throws MetadataServiceException {
    return this.client.list(filter).stream()
        .flatMap(
            d -> {
              try {
                return Stream.of(IndexStatsEntry.fromBson(BsonDocumentParser.fromRoot(d).build()));
              } catch (BsonParseException e) {
                LOG.atWarn()
                    .addKeyValue("document", d)
                    .setCause(e)
                    .log("Ignoring unparsed document from Index Stats Catalog");
                return Stream.empty();
              }
            })
        .toList();
  }

  @Override
  public List<IndexStatsEntry> list() throws MetadataServiceException {
    return list(new BsonDocument());
  }

  @Override
  public void createCollectionIndexes() throws MetadataServiceException {
    this.client.createIndex(
        Indexes.ascending(IndexStatsEntry.serverIdFilterKey()),
        new IndexOptions().background(true));

    this.client.createIndex(
        Indexes.ascending(IndexStatsEntry.indexIdFilterKey()), new IndexOptions().background(true));
  }

  public static DefaultIndexStats create(MongoClient mongoClient) {
    return new DefaultIndexStats(new MetadataClient<>(mongoClient, COLLECTION_NAME));
  }

  @VisibleForTesting
  static DefaultIndexStats createForTesting(MongoClient client, String databaseName) {
    return new DefaultIndexStats(new MetadataClient<>(client, databaseName, COLLECTION_NAME));
  }
}
