package com.xgen.testing.mongot.config.backup;

import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.config.manager.QuarantinedIndex;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

/** Builds the quarantined counterpart of an index definition, for tests. */
public final class QuarantinedIndexBuilder {

  /** Stands in for whatever a future binary wrote that this one cannot read. */
  public static final String UNREADABLE_FIELD = "fieldFromTheFuture";

  private static final String DEFAULT_PARSE_ERROR = "unknown field '" + UNREADABLE_FIELD + "'";

  private QuarantinedIndexBuilder() {}

  /** A quarantined twin of {@code definitionGeneration}: same identity, unparseable entry. */
  public static QuarantinedIndex quarantine(IndexDefinitionGeneration definitionGeneration) {
    return quarantine(definitionGeneration, DEFAULT_PARSE_ERROR);
  }

  /** See {@link #quarantine(IndexDefinitionGeneration)}. */
  public static QuarantinedIndex quarantine(
      IndexDefinitionGeneration definitionGeneration, String parseError) {
    BsonDocument rawEntry = definitionGeneration.toBson().clone();
    rawEntry.put(UNREADABLE_FIELD, BsonBoolean.TRUE);

    return new QuarantinedIndex(
        JournalEntryIdentity.of(definitionGeneration),
        definitionGeneration.getType(),
        parseError,
        rawEntry);
  }
}
