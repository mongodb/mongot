package com.xgen.mongot.index.definition;

import com.google.errorprone.annotations.DoNotMock;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Field.Required;
import com.xgen.mongot.util.bson.parser.Field.WithDefault;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

@DoNotMock("Cannot mock sealed class. Consider using SearchIndexDefinitionBuilder.VALID_INDEX")
public sealed interface IndexDefinition extends DocumentEncodable
    permits VectorIndexDefinition, SearchIndexDefinition {

  class Fields {

    public static final WithDefault<Type> TYPE =
        Field.builder("type")
            .enumField(Type.class)
            .asCamelCase()
            .optional()
            .withDefault(Type.SEARCH);

    public static final Required<ObjectId> INDEX_ID =
        Field.builder("indexID").objectIdField().required();

    public static final Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUUID").uuidField().required();

    public static final Required<String> LAST_OBSERVED_COLLECTION_NAME =
        Field.builder("lastObservedCollectionName").stringField().mustNotBeEmpty().required();

    /**
     * If this view field is present, collection UUID and name above represent the view's source
     * collection.
     */
    public static final Field.Optional<ViewDefinition> VIEW =
        Field.builder("view")
            .classField(ViewDefinition::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final WithDefault<Integer> NUM_PARTITIONS =
        Field.builder("numPartitions")
            .intField()
            .validate(
                num ->
                    Arrays.asList(1, 2, 4, 8, 16, 32, 64).contains(num)
                        ? Optional.empty()
                        : Optional.of(
                            String.format(
                                "numPartitions %d must be of 1, 2, 4, 8, 16, 32, or 64.", num)))
            .optional()
            .withDefault(1);

    public static final Required<String> NAME =
        Field.builder("name").stringField().mustNotBeEmpty().required();

    public static final Required<String> DATABASE =
        Field.builder("database").stringField().mustNotBeEmpty().required();

    static final Field.Optional<Long> DEFINITION_VERSION =
        Field.builder("definitionVersion").longField().optional().noDefault();

    static final Field.Optional<String> DEFINITION_VERSION_CREATED_AT =
        Field.builder("definitionVersionCreatedAt").stringField().optional().noDefault();

    static final Field.Optional<StoredSourceDefinition> STORED_SOURCE =
        Field.builder("storedSource")
            .classField(StoredSourceDefinition::fromBson)
            .optional()
            .noDefault();
  }

  DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("GMT"));

  enum Type {
    SEARCH,
    VECTOR_SEARCH
  }

  ObjectId getIndexId();

  String getName();

  String getDatabase();

  UUID getCollectionUuid();

  StoredSourceDefinition getStoredSource();

  Type getType();

  String getLastObservedCollectionName();

  void setLastObservedCollectionName(String name);

  Optional<ViewDefinition> getView();

  int getNumPartitions();

  /**
   * Returns the set of optional features which are supported for this index.
   *
   * <p>See {@link SearchIndexCapabilities} and {@link VectorIndexCapabilities} for instructions on
   * how to add new data types or features that require index time changes.
   */
  IndexCapabilities getIndexCapabilities(IndexFormatVersion version);

  FieldDefinitionResolver createFieldDefinitionResolver(IndexFormatVersion indexFormatVersion);

  Optional<Long> getDefinitionVersion();

  Optional<Instant> getDefinitionVersionCreatedAt();

  /**
   * The index_feature_version specified in the {@link SearchIndexDefinition} or in the {@link
   * VectorIndexDefinition}. Depending on the {@link IndexFormatVersion} of this index, this index
   * may support more features than defined in the index definition.
   *
   * <p>See {@link IndexFormatVersion#create(int)} for more details and an example.
   */
  int getParsedIndexFeatureVersion();

  /**
   * Returns true if any field requires auto-embedding.
   *
   * <p>Auto-embedding is only supported in {@link VectorIndexDefinition}. A vector index field
   * requires auto-embedding if it is specified with type {@link
   * VectorIndexFieldDefinition.Type#TEXT}.
   */
  boolean isAutoEmbeddingIndex();

  default MongoNamespace getLastObservedNamespace() {
    return new MongoNamespace(this.getDatabase(), this.getLastObservedCollectionName());
  }

  default VectorIndexDefinition asVectorDefinition() {
    return (VectorIndexDefinition) this;
  }

  default SearchIndexDefinition asSearchDefinition() {
    return (SearchIndexDefinition) this;
  }

  static IndexDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return switch (parser.getField(Fields.TYPE).unwrap()) {
      case SEARCH -> SearchIndexDefinition.fromBson(parser);
      case VECTOR_SEARCH -> VectorIndexDefinition.fromBson(parser);
    };
  }

  static Optional<ObjectId> getIndexIdFromBson(BsonDocument document) {

    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      ObjectId indexId = parser.getField(IndexDefinition.Fields.INDEX_ID).unwrap();
      return Optional.of(indexId);
    } catch (BsonParseException parseException) {
      return Optional.empty();
    }
  }
}
