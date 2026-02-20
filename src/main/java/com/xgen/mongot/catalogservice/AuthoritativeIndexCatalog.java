package com.xgen.mongot.catalogservice;

import com.xgen.mongot.index.definition.IndexDefinition;
import java.util.List;
import java.util.UUID;

public interface AuthoritativeIndexCatalog {

  String COLLECTION_NAME = "indexCatalog";

  /** Stores an index in the catalog. */
  void createIndex(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException;

  /** Modifies an index in the catalog with the new definition. */
  void updateIndex(AuthoritativeIndexKey indexKey, IndexDefinition definition)
      throws MetadataServiceException;

  /** Deletes a single index. If the index does not exist, does nothing. */
  void deleteIndex(AuthoritativeIndexKey indexKey) throws MetadataServiceException;

  /** Lists all indexes defined in the Authoritative Index Catalog. */
  List<IndexDefinition> listIndexes();

  /** Lists all indexes defined on the specified collection in the Authoritative Index Catalog. */
  List<IndexDefinition> listIndexes(UUID collectionUuid);
}
