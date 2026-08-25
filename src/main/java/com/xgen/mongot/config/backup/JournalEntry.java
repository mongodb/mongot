package com.xgen.mongot.config.backup;

import com.xgen.mongot.config.util.IndexIdentity;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * One element of one of the config journal's index arrays: either a definition that parsed, or an
 * entry that did not parse and was quarantined instead.
 *
 * <p>An entry's state (staged/live/deleted) and type (search/vector) are structural - they are the
 * array it was read from - so keeping quarantined entries in place in that array is what makes them
 * survive a read/write round trip and keeps them in the order they were in.
 *
 * @param <T> the definition type of the array this entry belongs to. For {@link Quarantined} this
 *     is a phantom parameter, present only so the journal's six fields stay homogeneous.
 */
public sealed interface JournalEntry<T extends IndexDefinitionGeneration> extends DocumentEncodable
    permits JournalEntry.Parsed, JournalEntry.Quarantined {

  /** The identifying fields of this entry, available whether or not its definition parsed. */
  IndexIdentity getIdentity();

  /** The definition, absent if this entry is quarantined. */
  Optional<T> getDefinition();

  boolean isQuarantined();

  default GenerationId getGenerationId() {
    return getIdentity().generationId();
  }

  static <T extends IndexDefinitionGeneration> JournalEntry<T> parsed(T definition) {
    return new Parsed<>(definition);
  }

  /** An entry whose definition parsed. Serializes exactly as the definition does. */
  record Parsed<T extends IndexDefinitionGeneration>(T definition) implements JournalEntry<T> {

    @Override
    public IndexIdentity getIdentity() {
      return JournalEntryIdentity.of(this.definition);
    }

    @Override
    public Optional<T> getDefinition() {
      return Optional.of(this.definition);
    }

    @Override
    public boolean isQuarantined() {
      return false;
    }

    @Override
    public BsonDocument toBson() {
      return this.definition.toBson();
    }
  }

  /**
   * An entry whose definition could not be parsed. Serializes the document it was read from
   * verbatim, so that quarantining an entry never loses or rewrites it and it can later be
   * inspected or removed.
   */
  record Quarantined<T extends IndexDefinitionGeneration>(
      BsonDocument rawBson, JournalEntryIdentity identity, String parseError)
      implements JournalEntry<T> {

    @Override
    public IndexIdentity getIdentity() {
      return this.identity;
    }

    @Override
    public Optional<T> getDefinition() {
      return Optional.empty();
    }

    @Override
    public boolean isQuarantined() {
      return true;
    }

    @Override
    public BsonDocument toBson() {
      return this.rawBson;
    }
  }
}
