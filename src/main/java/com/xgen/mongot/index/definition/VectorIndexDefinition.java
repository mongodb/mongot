package com.xgen.mongot.index.definition;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DateUtil;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VectorIndexDefinition implements IndexDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexDefinition.class);

  public static class Fields {
    private static final Field.Required<List<VectorIndexFieldDefinition>> FIELDS =
        Field.builder("fields")
            .classField(VectorIndexFieldDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();

    /** See {@link VectorIndexCapabilities} for reason for default value to be 3 */
    public static final Field.WithDefault<Integer> INDEX_FEATURE_VERSION =
        Field.builder("indexFeatureVersion").intField().optional().withDefault(3);
  }

  private final ObjectId indexId;
  private final String name;
  private final String database;
  private volatile String lastObservedCollectionName;
  private final UUID collectionUuid;
  private final Optional<ViewDefinition> view;
  private final int numPartitions;
  private final ImmutableList<VectorIndexFieldDefinition> fields;
  private final int indexFeatureVersion;
  private final Optional<Long> definitionVersion;
  private final Optional<Instant> definitionVersionCreatedAt;
  private final Optional<StoredSourceDefinition> storedSource;
  private final boolean isAutoEmbeddingIndex;
  private final int parsedAutoEmbeddingFeatureVersion;
  private final ImmutableMap<FieldPath, String> modelNamePerPath;

  private final VectorIndexFieldMapping mappings;

  /** Constructs a new VectorIndexDefinition for a MongoDB Atlas Search vector index. */
  public VectorIndexDefinition(
      ObjectId indexId,
      String name,
      String database,
      String lastObservedCollectionName,
      UUID collectionUuid,
      Optional<ViewDefinition> view,
      int numPartitions,
      List<VectorIndexFieldDefinition> fields,
      int parsedIndexFeatureVersion,
      Optional<Long> definitionVersion,
      Optional<Instant> definitionVersionCreatedAt,
      Optional<StoredSourceDefinition> storedSource) {
    this.indexId = indexId;
    this.name = name;
    this.database = database;
    this.lastObservedCollectionName = lastObservedCollectionName;
    this.collectionUuid = collectionUuid;
    this.view = view;
    this.numPartitions = numPartitions;
    this.fields = ImmutableList.copyOf(fields);
    this.indexFeatureVersion = parsedIndexFeatureVersion;
    this.definitionVersion = definitionVersion;
    this.definitionVersionCreatedAt = definitionVersionCreatedAt;
    this.storedSource = storedSource;
    this.mappings = VectorIndexFieldMapping.create(fields);
    this.isAutoEmbeddingIndex = calculateIsAutoEmbeddingIndex(fields);
    this.parsedAutoEmbeddingFeatureVersion = calculateAutoEmbeddingFeatureVersion(fields);
    this.modelNamePerPath = calculateAutoEmbeddingModelName(fields);

    this.storedSource.ifPresent(
        storedSourceDefinition ->
            Check.checkArg(
                !storedSourceDefinition.isAllIncluded(),
                "storedSource true not allowed for vector indexes"));
  }

  @Override
  public Type getType() {
    return Type.VECTOR_SEARCH;
  }

  @Override
  public ObjectId getIndexId() {
    return this.indexId;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getDatabase() {
    return this.database;
  }

  @Override
  public UUID getCollectionUuid() {
    return this.collectionUuid;
  }

  @Override
  public String getLastObservedCollectionName() {
    return this.lastObservedCollectionName;
  }

  @Override
  public void setLastObservedCollectionName(String name) {
    this.lastObservedCollectionName = name;
  }

  @Override
  public Optional<ViewDefinition> getView() {
    return this.view;
  }

  @Override
  public int getNumPartitions() {
    return this.numPartitions;
  }

  @Override
  public VectorIndexCapabilities getIndexCapabilities(IndexFormatVersion version) {
    return new VectorIndexCapabilities(version, this.indexFeatureVersion);
  }

  public ImmutableList<VectorIndexFieldDefinition> getFields() {
    return this.fields;
  }

  @Override
  public int getParsedIndexFeatureVersion() {
    return this.indexFeatureVersion;
  }

  public VectorIndexFieldMapping getMappings() {
    return this.mappings;
  }

  @Override
  public Optional<Long> getDefinitionVersion() {
    return this.definitionVersion;
  }

  public Optional<Instant> getDefinitionVersionCreatedAt() {
    return this.definitionVersionCreatedAt;
  }

  @Override
  public StoredSourceDefinition getStoredSource() {
    return this.storedSource.orElse(StoredSourceDefinition.defaultValue());
  }

  @Override
  public VectorFieldDefinitionResolver createFieldDefinitionResolver(
      IndexFormatVersion indexFormatVersion) {
    return new VectorFieldDefinitionResolver(this, indexFormatVersion);
  }

  @Override
  public boolean isAutoEmbeddingIndex() {
    return this.isAutoEmbeddingIndex;
  }

  /**
   * Returns autoEmbedding feature version if any field requires auto-embedding. Returns 0 if no
   * auto embedding field in index. Returns 1 if auto embedding field is text type and no Mat View
   * needed. Returns 2 if auto embedding field is built on top of Mat View collection. 3+ are
   * reserved for future Mat View collection schemas .
   *
   * <p>Auto-embedding is only supported in {@link VectorIndexDefinition}. A vector index field
   * requires auto-embedding if it is specified with type {@link
   * VectorIndexFieldDefinition.Type#TEXT} or {@link VectorIndexFieldDefinition.Type#AUTO_EMBED}.
   */
  public int getParsedAutoEmbeddingFeatureVersion() {
    return this.parsedAutoEmbeddingFeatureVersion;
  }

  /** Returns embedding model per FieldPath */
  public ImmutableMap<FieldPath, String> getModelNamePerPath() {
    return this.modelNamePerPath;
  }

  @Override
  public BsonDocument toBson() {
    var builder =
        BsonDocumentBuilder.builder()
            .field(IndexDefinition.Fields.TYPE, this.getType())
            .field(IndexDefinition.Fields.INDEX_ID, this.indexId)
            .field(IndexDefinition.Fields.NAME, this.name)
            .field(IndexDefinition.Fields.DATABASE, this.database)
            .field(
                IndexDefinition.Fields.LAST_OBSERVED_COLLECTION_NAME,
                this.lastObservedCollectionName)
            .field(IndexDefinition.Fields.COLLECTION_UUID, this.collectionUuid)
            .field(IndexDefinition.Fields.VIEW, this.view)
            .field(IndexDefinition.Fields.NUM_PARTITIONS, this.numPartitions)
            .field(Fields.FIELDS, this.fields)
            .field(IndexDefinition.Fields.DEFINITION_VERSION, this.definitionVersion)
            .field(
                IndexDefinition.Fields.DEFINITION_VERSION_CREATED_AT,
                this.definitionVersionCreatedAt.map(DATE_FORMAT::format))
            .field(Fields.INDEX_FEATURE_VERSION, this.indexFeatureVersion)
            .field(IndexDefinition.Fields.STORED_SOURCE, this.storedSource);
    return builder.build();
  }

  public static VectorIndexDefinition fromBson(BsonDocument document) throws BsonParseException {
    // Atlas sends extra fields that we don't care about, such as "createdDate" and
    // "lastUpdatedDate", so ignore any extra fields.
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return fromBson(parser);
    }
  }

  public static VectorIndexDefinition fromBson(DocumentParser parser) throws BsonParseException {
    Type type = parser.getField(IndexDefinition.Fields.TYPE).unwrap();
    if (!type.equals(Type.VECTOR_SEARCH)) {
      parser.getContext().handleSemanticError("Expected index of type vectorSearch");
    }

    ObjectId indexId = parser.getField(IndexDefinition.Fields.INDEX_ID).unwrap();
    int parsedIndexFeatureVersion = parser.getField(Fields.INDEX_FEATURE_VERSION).unwrap();
    if (parsedIndexFeatureVersion > VectorIndexCapabilities.CURRENT_FEATURE_VERSION) {
      LOG.warn(
          "indexFeatureVersion [{}] is higher than current IFV [{}] for index [{}]",
          parsedIndexFeatureVersion,
          VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
          indexId);
    }
    var fields = parser.getField(Fields.FIELDS).unwrap();
    var uniquePathsCount =
        fields.stream().map(VectorIndexFieldDefinition::getPath).distinct().count();
    if (uniquePathsCount < fields.size()) {
      parser
          .getContext()
          .handleSemanticError("Vector and filter fields should have distinct paths");
    }

    return new VectorIndexDefinition(
        parser.getField(IndexDefinition.Fields.INDEX_ID).unwrap(),
        parser.getField(IndexDefinition.Fields.NAME).unwrap(),
        parser.getField(IndexDefinition.Fields.DATABASE).unwrap(),
        parser.getField(IndexDefinition.Fields.LAST_OBSERVED_COLLECTION_NAME).unwrap(),
        parser.getField(IndexDefinition.Fields.COLLECTION_UUID).unwrap(),
        parser.getField(IndexDefinition.Fields.VIEW).unwrap(),
        parser.getField(IndexDefinition.Fields.NUM_PARTITIONS).unwrap(),
        fields,
        // Initialize with the minimum of the two IndexFeatureVersion values
        Integer.min(parsedIndexFeatureVersion, VectorIndexCapabilities.CURRENT_FEATURE_VERSION),
        parser.getField(IndexDefinition.Fields.DEFINITION_VERSION).unwrap(),
        DateUtil.parseInstantFromString(
            parser, DATE_FORMAT, IndexDefinition.Fields.DEFINITION_VERSION_CREATED_AT),
        parser.getField(IndexDefinition.Fields.STORED_SOURCE).unwrap());
  }

  public VectorIndexDefinition withUpdatedViewDefinition(ViewDefinition updatedViewDefinition) {
    return new VectorIndexDefinition(
        this.indexId,
        this.name,
        this.database,
        this.lastObservedCollectionName,
        this.collectionUuid,
        Optional.of(updatedViewDefinition),
        this.numPartitions,
        this.fields,
        this.indexFeatureVersion,
        this.definitionVersion,
        this.definitionVersionCreatedAt,
        this.storedSource);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorIndexDefinition that = (VectorIndexDefinition) o;
    return this.numPartitions == that.numPartitions
        && Objects.equal(this.indexId, that.indexId)
        && Objects.equal(this.name, that.name)
        && Objects.equal(this.database, that.database)
        && Objects.equal(this.lastObservedCollectionName, that.lastObservedCollectionName)
        && Objects.equal(this.collectionUuid, that.collectionUuid)
        && Objects.equal(this.view, that.view)
        && Objects.equal(this.fields, that.fields)
        && this.indexFeatureVersion == that.indexFeatureVersion
        && Objects.equal(this.definitionVersion, that.definitionVersion)
        // When serializing to BSON we convert to a string with second granularity (See
        // IndexDefinition#DATE_FORMAT). So the deserialized object equals the original we can only
        // compare with second granularity.
        && Objects.equal(
            this.definitionVersionCreatedAt.map(Instant::getEpochSecond),
            that.definitionVersionCreatedAt.map(Instant::getEpochSecond))
        && Objects.equal(this.storedSource, that.storedSource)
        && Objects.equal(this.mappings, that.mappings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.indexId,
        this.name,
        this.database,
        this.lastObservedCollectionName,
        this.collectionUuid,
        this.view,
        this.numPartitions,
        this.fields,
        this.indexFeatureVersion,
        this.definitionVersion,
        this.definitionVersionCreatedAt.map(Instant::getEpochSecond),
        this.storedSource,
        this.mappings);
  }

  @Override
  public String toString() {
    var collection =
        this.view
            .map(
                view ->
                    String.format(
                        "view '%s' on collection '%s'",
                        view.getName(), this.lastObservedCollectionName))
            .orElse("collection " + this.lastObservedCollectionName);
    return String.format(
        "%s (vector index %s %s (%s) in database %s)",
        this.getIndexId(),
        this.getName(),
        collection,
        this.getCollectionUuid(),
        this.getDatabase());
  }

  private boolean calculateIsAutoEmbeddingIndex(List<VectorIndexFieldDefinition> fields) {
    return fields.stream()
        .anyMatch(
            e ->
                e.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || e.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED);
  }

  private int calculateAutoEmbeddingFeatureVersion(List<VectorIndexFieldDefinition> fields) {
    @Var int highestVersion = 0;
    for (var field : fields) {
      if (field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
        return 2;
      }
      if (field.getType() == VectorIndexFieldDefinition.Type.TEXT) {
        highestVersion = 1;
      }
    }
    return highestVersion;
  }

  // TODO(CLOUDP-362123): Integrate this into IndexCapabilities
  private ImmutableMap<FieldPath, String> calculateAutoEmbeddingModelName(
      List<VectorIndexFieldDefinition> fields) {
    ImmutableMap.Builder<FieldPath, String> builder = ImmutableMap.builder();
    fields.stream()
        .filter(
            vectorFieldDefinition ->
                (vectorFieldDefinition.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || vectorFieldDefinition.getType()
                        == VectorIndexFieldDefinition.Type.AUTO_EMBED))
        .forEach(
            // TODO(CLOUDP-332187): Support multiple models on same paths.
            vectorTextDef ->
                builder.put(
                    vectorTextDef.path,
                    Check.instanceOf(
                            vectorTextDef.asVectorField().specification(),
                            VectorTextFieldSpecification.class)
                        .modelName()));
    return builder.build();
  }
}
