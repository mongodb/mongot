package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * An index whose definition could not be parsed out of the config journal, described by everything
 * we do know about it.
 *
 * <p>The identity is thinly parsed and the type comes from the journal array the entry was read
 * from, so both are available even though the definition is not. State is not carried here; the
 * {@link IncompatibleIndexes} collection an entry is in is what says which it is.
 *
 * @param rawConfigJournalEntry the entry exactly as it was read, so that it can be inspected or
 *     re-serialized without being rewritten.
 */
public record QuarantinedIndex(
    JournalEntryIdentity identity,
    IndexDefinitionGeneration.Type type,
    String parseError,
    BsonDocument rawConfigJournalEntry) {

  public ObjectId indexId() {
    return this.identity.indexId();
  }

  public String name() {
    return this.identity.name();
  }

  public String database() {
    return this.identity.database();
  }

  public UUID collectionUuid() {
    return this.identity.collectionUuid();
  }

  public Generation generation() {
    return this.identity.generation();
  }

  public GenerationId generationId() {
    return this.identity.generationId();
  }
}
