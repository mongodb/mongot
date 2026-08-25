package com.xgen.mongot.config.backup;

import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.TypeDescription;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See the config journals section in: src/main/java/com/xgen/mongot/config/README.md
 *
 * <p>A config journal describing indexes in different generations and states. One index generation
 * can be either staged, live or deleted.
 *
 * <ul>
 *   <li>Staged - Means that the index is staged to swap a live index once it is ready.
 *   <li>Live - Index should be queryable.
 *   <li>Deleted - Index that should be deleted once this journal is used to restore mongot.
 * </ul>
 *
 * <p>Each of the six index arrays holds {@link JournalEntry}s rather than definitions, so that an
 * entry whose definition does not parse can be quarantined in place instead of failing the whole
 * read - see {@link QuarantinePolicy}. The definition-returning accessors expose only the entries
 * that parsed.
 */
public final class ConfigJournalV1 implements DocumentEncodable {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigJournalV1.class);

  public static final int VERSION = 1;

  public static class Fields {
    static final Field.Required<Integer> VERSION_FIELD =
        Field.builder("version")
            .intField()
            .validate(
                x ->
                    x == VERSION
                        ? Optional.empty()
                        : Optional.of(
                            String.format("must be set to %s for this config format.", VERSION)))
            .required();

    static final Field.Required<List<BsonValue>> STAGED_SEARCH_INDEXES =
        searchEntriesField("stagedIndexes");
    static final Field.Required<List<BsonValue>> LIVE_SEARCH_INDEXES =
        searchEntriesField("indexes");
    static final Field.Required<List<BsonValue>> DELETED_SEARCH_INDEXES =
        searchEntriesField("deletedIndexes");

    static final Field.WithDefault<List<BsonValue>> STAGED_VECTOR_INDEXES =
        vectorEntriesField("stagedVectorIndexes");
    static final Field.WithDefault<List<BsonValue>> LIVE_VECTOR_INDEXES =
        vectorEntriesField("vectorIndexes");
    static final Field.WithDefault<List<BsonValue>> DELETED_VECTOR_INDEXES =
        vectorEntriesField("deletedVectorIndexes");

    /**
     * The elements are read unparsed so that each one can be parsed individually, with the failure
     * of one element kept away from the rest of the list. The definitions themselves are parsed by
     * {@link #parseEntries}, which reproduces what {@code classField(...).disallowUnknownFields()}
     * would have done for an element.
     */
    private static Field.Required<List<BsonValue>> searchEntriesField(String name) {
      return Field.builder(name).unparsedValueField().asList().required();
    }

    private static Field.WithDefault<List<BsonValue>> vectorEntriesField(String name) {
      return Field.builder(name).unparsedValueField().asList().optional().withDefault(List.of());
    }
  }

  /* First we compare indexIds, then break ties by format and user versions.*/
  private static final Comparator<GenerationId> GENERATION_ID_SORTER =
      Comparator.<GenerationId, ObjectId>comparing(genId -> genId.indexId)
          .thenComparingInt(genId -> genId.generation.indexFormatVersion.versionNumber)
          .thenComparingInt(genId -> genId.generation.userIndexVersion.versionNumber);

  /** Compares journal entries by their generation id. */
  private static final Comparator<JournalEntry<?>> ENTRY_SORTER =
      Comparator.comparing(JournalEntry::getGenerationId, GENERATION_ID_SORTER);

  private final ImmutableList<JournalEntry<SearchIndexDefinitionGeneration>> stagedSearchIndexes;
  private final ImmutableList<JournalEntry<SearchIndexDefinitionGeneration>> liveSearchIndexes;
  private final ImmutableList<JournalEntry<SearchIndexDefinitionGeneration>> deletedSerchIndexes;

  private final ImmutableList<JournalEntry<VectorIndexDefinitionGeneration>> stagedVectorIndexes;
  private final ImmutableList<JournalEntry<VectorIndexDefinitionGeneration>> liveVectorIndexes;
  private final ImmutableList<JournalEntry<VectorIndexDefinitionGeneration>> deletedVectorIndexes;

  public ConfigJournalV1(
      List<IndexDefinitionGeneration> staged,
      List<IndexDefinitionGeneration> live,
      List<IndexDefinitionGeneration> deleted) {
    this(
        asEntries(extractSearchIndexDefinitionGeneration(staged)),
        asEntries(extractSearchIndexDefinitionGeneration(live)),
        asEntries(extractSearchIndexDefinitionGeneration(deleted)),
        asEntries(extractVectorIndexDefinitionGeneration(staged)),
        asEntries(extractVectorIndexDefinitionGeneration(live)),
        asEntries(extractVectorIndexDefinitionGeneration(deleted)));
  }

  private ConfigJournalV1(
      List<JournalEntry<SearchIndexDefinitionGeneration>> stagedSearchIndexes,
      List<JournalEntry<SearchIndexDefinitionGeneration>> liveSearchIndexes,
      List<JournalEntry<SearchIndexDefinitionGeneration>> deletedSearchIndexes,
      List<JournalEntry<VectorIndexDefinitionGeneration>> stagedVectorIndexes,
      List<JournalEntry<VectorIndexDefinitionGeneration>> liveVectorIndexes,
      List<JournalEntry<VectorIndexDefinitionGeneration>> deletedVectorIndexes) {
    this.stagedSearchIndexes = consistentOrdering(stagedSearchIndexes);
    this.liveSearchIndexes = consistentOrdering(liveSearchIndexes);
    this.deletedSerchIndexes = consistentOrdering(deletedSearchIndexes);
    this.stagedVectorIndexes = consistentOrdering(stagedVectorIndexes);
    this.liveVectorIndexes = consistentOrdering(liveVectorIndexes);
    this.deletedVectorIndexes = consistentOrdering(deletedVectorIndexes);
  }

  private static <T extends IndexDefinitionGeneration> ImmutableList<JournalEntry<T>> asEntries(
      List<T> definitions) {
    return definitions.stream()
        .map(JournalEntry::<T>parsed)
        .collect(ImmutableList.toImmutableList());
  }

  private static List<SearchIndexDefinitionGeneration> extractSearchIndexDefinitionGeneration(
      List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .filter(d -> d.getType() == Type.SEARCH)
        .map(IndexDefinitionGeneration::asSearch)
        .collect(Collectors.toList());
  }

  private static List<VectorIndexDefinitionGeneration> extractVectorIndexDefinitionGeneration(
      List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .filter(d -> d.getType() == Type.VECTOR)
        .map(IndexDefinitionGeneration::asVector)
        .collect(Collectors.toList());
  }

  /** Reads a journal, failing on the first unparseable index definition. */
  public static Optional<ConfigJournalV1> fromFileIfExists(Path path)
      throws IOException, BsonParseException {
    return fromFileIfExists(path, QuarantinePolicy.FAIL_FAST);
  }

  /** Reads a journal, handling unparseable index definitions as {@code policy} dictates. */
  public static Optional<ConfigJournalV1> fromFileIfExists(Path path, QuarantinePolicy policy)
      throws IOException, BsonParseException {
    File file = path.toFile();
    if (!file.exists()) {
      return Optional.empty();
    }

    String json = Files.readString(path);
    BsonDocument document = JsonCodec.fromJson(json);

    try (BsonDocumentParser parser =
        BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return Optional.of(ConfigJournalV1.fromBson(parser, policy));
    }
  }

  /** deserialize, throws if the document represents a config format from a different version. */
  public static ConfigJournalV1 fromBson(DocumentParser parser) throws BsonParseException {
    return fromBson(parser, QuarantinePolicy.FAIL_FAST);
  }

  /** deserialize, throws if the document represents a config format from a different version. */
  public static ConfigJournalV1 fromBson(DocumentParser parser, QuarantinePolicy policy)
      throws BsonParseException {
    // make sure this is the correct version:
    parser.getField(Fields.VERSION_FIELD).unwrap();

    return new ConfigJournalV1(
        parseSearchEntries(parser, Fields.STAGED_SEARCH_INDEXES, policy),
        parseSearchEntries(parser, Fields.LIVE_SEARCH_INDEXES, policy),
        parseSearchEntries(parser, Fields.DELETED_SEARCH_INDEXES, policy),
        parseVectorEntries(parser, Fields.STAGED_VECTOR_INDEXES, policy),
        parseVectorEntries(parser, Fields.LIVE_VECTOR_INDEXES, policy),
        parseVectorEntries(parser, Fields.DELETED_VECTOR_INDEXES, policy));
  }

  private static List<JournalEntry<SearchIndexDefinitionGeneration>> parseSearchEntries(
      DocumentParser parser, Field.Required<List<BsonValue>> field, QuarantinePolicy policy)
      throws BsonParseException {
    return parseEntries(
        parser.getContext().child(field.getName()),
        parser.getField(field).unwrap(),
        SearchIndexDefinitionGeneration::fromBson,
        JournalEntryIdentity::thinParseSearchEntry,
        policy);
  }

  private static List<JournalEntry<VectorIndexDefinitionGeneration>> parseVectorEntries(
      DocumentParser parser, Field.WithDefault<List<BsonValue>> field, QuarantinePolicy policy)
      throws BsonParseException {
    return parseEntries(
        parser.getContext().child(field.getName()),
        parser.getField(field).unwrap(),
        VectorIndexDefinitionGeneration::fromBson,
        JournalEntryIdentity::thinParseVectorEntry,
        policy);
  }

  private static <T extends IndexDefinitionGeneration> List<JournalEntry<T>> parseEntries(
      BsonParseContext listContext,
      List<BsonValue> rawEntries,
      ClassField.FromDocumentParser<T> elementParser,
      JournalEntryIdentity.ThinParser thinParser,
      QuarantinePolicy policy)
      throws BsonParseException {
    List<JournalEntry<T>> entries = new ArrayList<>();
    for (int index = 0; index < rawEntries.size(); index++) {
      BsonValue rawEntry = rawEntries.get(index);
      BsonParseContext entryContext = listContext.arrayElement(index);
      if (rawEntry.getBsonType() != BsonType.DOCUMENT) {
        entryContext.handleUnexpectedType(TypeDescription.DOCUMENT, rawEntry.getBsonType());
      }

      entries.add(
          parseEntry(entryContext, rawEntry.asDocument(), elementParser, thinParser, policy));
    }
    return entries;
  }

  private static <T extends IndexDefinitionGeneration> JournalEntry<T> parseEntry(
      BsonParseContext entryContext,
      BsonDocument rawEntry,
      ClassField.FromDocumentParser<T> elementParser,
      JournalEntryIdentity.ThinParser thinParser,
      QuarantinePolicy policy)
      throws BsonParseException {
    try {
      return JournalEntry.parsed(parseDefinition(entryContext, rawEntry, elementParser));
    } catch (BsonParseException e) {
      if (policy == QuarantinePolicy.FAIL_FAST) {
        throw e;
      }

      // Quarantining an entry we cannot even name would leave us unable to report on it or remove
      // it later, so a failure to thin parse the identity is deliberately not caught.
      JournalEntryIdentity identity = thinParser.parse(entryContext, rawEntry);
      LOG.atWarn()
          .addKeyValue("indexId", identity.indexId().toHexString())
          .addKeyValue("indexName", identity.name())
          .addKeyValue("generation", identity.generation())
          .setCause(e)
          .log("quarantining unparseable config journal entry");
      // Cloned so that the entry we re-emit cannot be changed by anything that still holds the
      // document we parsed.
      return new JournalEntry.Quarantined<>(rawEntry.clone(), identity, e.getMessage());
    }
  }

  /**
   * Parses a single entry exactly as {@code classField(...).disallowUnknownFields()} would have, so
   * that error messages and parse paths are unchanged.
   */
  private static <T extends IndexDefinitionGeneration> T parseDefinition(
      BsonParseContext entryContext,
      BsonDocument rawEntry,
      ClassField.FromDocumentParser<T> elementParser)
      throws BsonParseException {
    try (DocumentParser entryParser =
        BsonDocumentParser.withContext(entryContext, rawEntry).allowUnknownFields(false).build()) {
      return elementParser.parse(entryParser);
    }
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VERSION_FIELD, VERSION)
        .field(Fields.STAGED_SEARCH_INDEXES, encode(this.stagedSearchIndexes))
        .field(Fields.LIVE_SEARCH_INDEXES, encode(this.liveSearchIndexes))
        .field(Fields.DELETED_SEARCH_INDEXES, encode(this.deletedSerchIndexes))
        .field(Fields.STAGED_VECTOR_INDEXES, encode(this.stagedVectorIndexes))
        .field(Fields.LIVE_VECTOR_INDEXES, encode(this.liveVectorIndexes))
        .field(Fields.DELETED_VECTOR_INDEXES, encode(this.deletedVectorIndexes))
        .build();
  }

  private static List<BsonValue> encode(List<? extends JournalEntry<?>> entries) {
    return entries.stream().map(entry -> (BsonValue) entry.toBson()).collect(Collectors.toList());
  }

  public ImmutableList<IndexDefinitionGeneration> getStagedIndexes() {
    return parsedDefinitions(this.stagedSearchIndexes, this.stagedVectorIndexes);
  }

  public ImmutableList<IndexDefinitionGeneration> getLiveIndexes() {
    return parsedDefinitions(this.liveSearchIndexes, this.liveVectorIndexes);
  }

  public ImmutableList<IndexDefinitionGeneration> getDeletedIndexes() {
    return parsedDefinitions(this.deletedSerchIndexes, this.deletedVectorIndexes);
  }

  /** All staged entries, quarantined ones included. */
  public ImmutableList<JournalEntry<? extends IndexDefinitionGeneration>> getStagedEntries() {
    return allEntries(this.stagedSearchIndexes, this.stagedVectorIndexes);
  }

  /** All live entries, quarantined ones included. */
  public ImmutableList<JournalEntry<? extends IndexDefinitionGeneration>> getLiveEntries() {
    return allEntries(this.liveSearchIndexes, this.liveVectorIndexes);
  }

  /** All deleted entries, quarantined ones included. */
  public ImmutableList<JournalEntry<? extends IndexDefinitionGeneration>> getDeletedEntries() {
    return allEntries(this.deletedSerchIndexes, this.deletedVectorIndexes);
  }

  /** The entries, in any state, whose index definition could not be parsed. */
  public ImmutableList<JournalEntry.Quarantined<? extends IndexDefinitionGeneration>>
      getQuarantinedEntries() {
    return Stream.of(getStagedEntries(), getLiveEntries(), getDeletedEntries())
        .flatMap(List::stream)
        .filter(entry -> entry instanceof JournalEntry.Quarantined)
        .map(entry -> (JournalEntry.Quarantined<? extends IndexDefinitionGeneration>) entry)
        .collect(ImmutableList.toImmutableList());
  }

  private static ImmutableList<IndexDefinitionGeneration> parsedDefinitions(
      List<? extends JournalEntry<? extends IndexDefinitionGeneration>> searchEntries,
      List<? extends JournalEntry<? extends IndexDefinitionGeneration>> vectorEntries) {
    return Stream.concat(searchEntries.stream(), vectorEntries.stream())
        .flatMap(entry -> entry.getDefinition().stream())
        .collect(ImmutableList.toImmutableList());
  }

  private static ImmutableList<JournalEntry<? extends IndexDefinitionGeneration>> allEntries(
      List<? extends JournalEntry<? extends IndexDefinitionGeneration>> searchEntries,
      List<? extends JournalEntry<? extends IndexDefinitionGeneration>> vectorEntries) {
    return ImmutableList.<JournalEntry<? extends IndexDefinitionGeneration>>builder()
        .addAll(searchEntries)
        .addAll(vectorEntries)
        .build();
  }

  private static <T extends IndexDefinitionGeneration>
      ImmutableList<JournalEntry<T>> consistentOrdering(List<JournalEntry<T>> entries) {
    // To keep a consistent ordering of definitions in our config, sort the definitions by
    // generation ids.
    // We do not rely on this order in any way, however it is convenient for reading and testing.
    // A quarantined entry sorts into the slot its parseable twin would have occupied: its key comes
    // from the same generation sub-document.
    return entries.stream().sorted(ENTRY_SORTER).collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigJournalV1 that = (ConfigJournalV1) o;
    return Objects.equal(this.stagedSearchIndexes, that.stagedSearchIndexes)
        && Objects.equal(this.liveSearchIndexes, that.liveSearchIndexes)
        && Objects.equal(this.deletedSerchIndexes, that.deletedSerchIndexes)
        && Objects.equal(this.stagedVectorIndexes, that.stagedVectorIndexes)
        && Objects.equal(this.liveVectorIndexes, that.liveVectorIndexes)
        && Objects.equal(this.deletedVectorIndexes, that.deletedVectorIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.stagedSearchIndexes,
        this.liveSearchIndexes,
        this.deletedSerchIndexes,
        this.stagedVectorIndexes,
        this.liveVectorIndexes,
        this.deletedVectorIndexes);
  }
}
