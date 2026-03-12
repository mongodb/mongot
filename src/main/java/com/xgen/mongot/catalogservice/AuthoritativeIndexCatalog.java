package com.xgen.mongot.catalogservice;

import com.xgen.mongot.index.definition.IndexDefinition;
import java.util.List;
import java.util.UUID;
import org.bson.BsonDocument;

public interface AuthoritativeIndexCatalog {

  String COLLECTION_NAME = "indexCatalog";

  /** Stores an index in the catalog. */
  void createIndex(
      AuthoritativeIndexKey indexKey, IndexDefinition definition, BsonDocument customerDefinition)
      throws MetadataServiceException;

  /** Modifies an index in the catalog with the new definition. */
  void updateIndex(
      AuthoritativeIndexKey indexKey, IndexDefinition definition, BsonDocument customerDefinition)
      throws MetadataServiceException;

  /**
   * Updates the internal {@link IndexDefinition} for the index, keeping the customer provided
   * definition the same. Used to apply collection name or index view changes without changing the
   * underlying definition.
   */
  boolean updateIndexDefinition(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException;

  /** Deletes a single index. If the index does not exist, does nothing. */
  void deleteIndex(AuthoritativeIndexKey indexKey) throws MetadataServiceException;

  /**
   * Returns the full index entries from the Authoritative Index Catalog for the specific
   * collection.
   */
  List<IndexEntry> listIndexes(UUID collectionUuid) throws MetadataServiceException;

  /** Returns all the index entries from the Authoritative Index Catalog. */
  List<IndexEntry> listIndexes() throws MetadataServiceException;

  /** Lists all indexes defined in the Authoritative Index Catalog. */
  List<IndexDefinition> listIndexDefinitions() throws MetadataServiceException;

  /** Lists all indexes defined on the specified collection in the Authoritative Index Catalog. */
  List<IndexDefinition> listIndexDefinitions(UUID collectionUuid) throws MetadataServiceException;
}
