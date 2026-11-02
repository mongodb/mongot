package com.xgen.mongot.replication.mongodb.synonyms;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.DerivedLocalIndexGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;

/** Resolves the namespace of a synonym mapping's source collection. */
final class SynonymSourceNamespace {

  private SynonymSourceNamespace() {}

  /**
   * Returns the namespace of {@code synonymDefinition}'s source collection, which lives in the
   * user's database. For an auto-embedding derived index, whose definition points at the
   * materialized view database, that is the original user definition's database.
   */
  static MongoNamespace of(
      IndexGeneration indexGeneration, SynonymMappingDefinition synonymDefinition) {
    String database =
        indexGeneration instanceof DerivedLocalIndexGeneration derived
            ? derived.getRawIndexSourceDatabase()
            : indexGeneration.getDefinition().getDatabase();
    return new MongoNamespace(database, synonymDefinition.source().collection());
  }
}
