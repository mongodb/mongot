package com.xgen.mongot.config.backup;

import com.xgen.mongot.config.util.IndexIdentity;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * The identity of a config journal entry, obtained without parsing the index definition it carries.
 *
 * <p>The two entry shapes differ in where the {@link IndexDefinition} fields sit: a search entry
 * wraps them in an {@code index} sub-document alongside its {@code analyzers}, while a vector entry
 * writes them at the top level. Both carry {@code generation} at the top level. Which shape to
 * expect comes from the array the entry was found in - the entry's own {@code type} field is
 * deliberately not read, since the array is the sole authority on search versus vector.
 */
public record JournalEntryIdentity(
    ObjectId indexId, String database, UUID collectionUuid, String name, Generation generation)
    implements IndexIdentity {

  private static class Fields {
    static final Field.Required<BsonDocument> INDEX =
        Field.builder("index").documentField().required();
  }

  /** Reads the identity of an entry, given where in that entry the definition fields are. */
  @FunctionalInterface
  public interface ThinParser {
    JournalEntryIdentity parse(BsonParseContext context, BsonDocument entry)
        throws BsonParseException;
  }

  /**
   * Parses only the fields that identify a search entry, ignoring everything else about it.
   *
   * <p>Throws if even the identity cannot be read: an entry we cannot name is one we cannot
   * quarantine, report on, or later remove, so it stays fatal.
   */
  public static JournalEntryIdentity thinParseSearchEntry(
      BsonParseContext context, BsonDocument entry) throws BsonParseException {
    try (DocumentParser parser =
        BsonDocumentParser.withContext(context, entry).allowUnknownFields(true).build()) {
      BsonDocument definition = parser.getField(Fields.INDEX).unwrap();
      Generation generation = parser.getField(IndexDefinitionGeneration.Fields.GENERATION).unwrap();

      return thinParseDefinition(context.child(Fields.INDEX.getName()), definition, generation);
    }
  }

  /** Parses only the fields that identify a vector entry. See {@link #thinParseSearchEntry}. */
  public static JournalEntryIdentity thinParseVectorEntry(
      BsonParseContext context, BsonDocument entry) throws BsonParseException {
    try (DocumentParser parser =
        BsonDocumentParser.withContext(context, entry).allowUnknownFields(true).build()) {
      Generation generation = parser.getField(IndexDefinitionGeneration.Fields.GENERATION).unwrap();

      return thinParseDefinition(context, entry, generation);
    }
  }

  private static JournalEntryIdentity thinParseDefinition(
      BsonParseContext context, BsonDocument definition, Generation generation)
      throws BsonParseException {
    try (DocumentParser parser =
        BsonDocumentParser.withContext(context, definition).allowUnknownFields(true).build()) {
      return new JournalEntryIdentity(
          parser.getField(IndexDefinition.Fields.INDEX_ID).unwrap(),
          parser.getField(IndexDefinition.Fields.DATABASE).unwrap(),
          parser.getField(IndexDefinition.Fields.COLLECTION_UUID).unwrap(),
          parser.getField(IndexDefinition.Fields.NAME).unwrap(),
          generation);
    }
  }

  /** The identity of an entry that did parse, for a uniform view over both kinds of entry. */
  public static JournalEntryIdentity of(IndexDefinitionGeneration definitionGeneration) {
    IndexDefinition definition = definitionGeneration.getIndexDefinition();
    return new JournalEntryIdentity(
        definition.getIndexId(),
        definition.getDatabase(),
        definition.getCollectionUuid(),
        definition.getName(),
        definitionGeneration.generation());
  }
}
