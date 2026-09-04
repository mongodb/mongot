package com.xgen.mongot.catalog;

import com.xgen.mongot.index.definition.IndexDefinition;
import java.util.UUID;

/**
 * The properties a query identifies an index by: its name, qualified by the database and collection
 * it is defined on.
 *
 * <p>This is the key indexes are looked up by, both in {@link IndexCatalog} and in the parallel
 * registry of indexes that could not be parsed. It deliberately does not carry a view name: an
 * index on a view is keyed by the view's own collection uuid, and the view name is checked
 * separately after the lookup.
 */
public record IndexProperties(String indexName, String databaseName, UUID collectionUuid) {

  public static IndexProperties of(IndexDefinition definition) {
    return new IndexProperties(
        definition.getName(), definition.getDatabase(), definition.getCollectionUuid());
  }
}
