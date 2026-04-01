package com.xgen.mongot.index.definition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DateUtil;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.collections4.trie.UnmodifiableTrie;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexDefinition contains information about how documents should be indexed.
 *
 * <p>IndexDefinition instances are the entrypoint for all operations on the definition itself.
 * Fields must be retrieved through the IndexDefinition.
 */
public final class SearchIndexDefinition implements IndexDefinition {

  /** The name of the Analyzer that should be used if no Analyzer is configured. */
  public static final String DEFAULT_FALLBACK_ANALYZER =
      StockAnalyzerNames.LUCENE_STANDARD.getName();

  public static final StockNormalizerName DEFAULT_FALLBACK_NORMALIZER = StockNormalizerName.NONE;

  public static class Fields {

    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<String> SEARCH_ANALYZER =
        Field.builder("searchAnalyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<List<CustomAnalyzerDefinition>> ANALYZERS =
        Field.builder("analyzers")
            .classField(CustomAnalyzerDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustHaveUniqueAttribute("name", CustomAnalyzerDefinition::name)
            .optional()
            .noDefault();

    public static final Field.Required<DocumentFieldDefinition> MAPPINGS =
        Field.builder("mappings")
            /*
             * Encode to and from a DocumentFieldDefinition instead of FieldDataTypeDefinition so
             * that "type": "document" does not have to be included.
             */
            .classField(DocumentFieldDefinition::fromBson, DocumentFieldDefinition::fieldTypeToBson)
            .disallowUnknownFields()
            .required();

    public static final Field.WithDefault<Integer> INDEX_FEATURE_VERSION =
        Field.builder("indexFeatureVersion").intField().optional().withDefault(0);

    static final Field.Optional<List<SynonymMappingDefinition>> SYNONYMS =
        Field.builder("synonyms")
            .classField(SynonymMappingDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustHaveUniqueAttribute("name", definition -> definition.name())
            .optional()
            .noDefault();

    static final Field.Optional<List<TypeSetDefinition>> TYPE_SETS =
        Field.builder("typeSets")
            .listOf(
                Value.builder()
                    .classValue(TypeSetDefinition::fromBson)
                    .disallowUnknownFields()
                    .required())
            .mustHaveUniqueAttribute("name", TypeSetDefinition::name)
            .mustNotBeEmpty()
            .optional()
            .noDefault();

    static final Field.Optional<Sort> SORT =
        Field.builder("sort").classField(Sort::fromBsonAsSort).optional().noDefault();
  }

  private final ObjectId indexId;
  private final String name;
  private final String database;
  private volatile String lastObservedCollectionName;
  private final UUID collectionUuid;
  private final Optional<ViewDefinition> view;
  private final int numPartitions;
  private final Optional<String> analyzerName;
  private final Optional<String> searchAnalyzerName;
  private final DocumentFieldDefinition mappings;
  private final Optional<List<CustomAnalyzerDefinition>> analyzers;
  private final int indexFeatureVersion;
  private final Optional<List<SynonymMappingDefinition>> synonyms;
  private final Optional<StoredSourceDefinition> storedSource;
  private final Optional<List<TypeSetDefinition>> typeSets;
  private final Optional<Sort> sort;
  private final Map<String, TypeSetDefinition> typeSetsMap;
  private final Optional<Long> definitionVersion;
  private final Optional<Instant> definitionVersionCreatedAt;
  private final boolean isAutoEmbeddingIndex;
  private final ImmutableMap<FieldPath, String> modelNamePerPath;
  private final Optional<Long> autoEmbeddingDefinitionVersion;
  private final Optional<Long> materializedViewNameFormatVersion;

  /** Fast access to the statically configured FieldDefinitions. */
  private final UnmodifiableTrie<String, FieldDefinition> staticFieldDefinitions;

  /** Names of all overridden and custom analyzers referenced by this IndexDefinition. */
  private final Set<String> nonStockAnalyzerNames;

  private final Optional<ObjectId> indexIdAtCreationTime;

  /** Constructs a new SearchIndexDefinition for a MongoDB Atlas Search index. */
  private SearchIndexDefinition(
      ObjectId indexId,
      String name,
      String database,
      String lastObservedCollectionName,
      UUID collectionUuid,
      Optional<ViewDefinition> view,
      int numPartitions,
      DocumentFieldDefinition mappings,
      Optional<String> analyzerName,
      Optional<String> searchAnalyzerName,
      UnmodifiableTrie<String, FieldDefinition> staticFieldDefinitions,
      Set<String> nonStockAnalyzerNames,
      Optional<List<CustomAnalyzerDefinition>> analyzers,
      int parsedIndexFeatureVersion,
      Optional<List<SynonymMappingDefinition>> synonyms,
      Optional<StoredSourceDefinition> storedSource,
      Optional<List<TypeSetDefinition>> typeSets,
      Optional<Sort> sort,
      Optional<Long> definitionVersion,
      Optional<Instant> definitionVersionCreatedAt,
      Optional<ObjectId> indexIdAtCreationTime,
      Optional<Long> autoEmbeddingDefinitionVersion,
      Optional<Long> materializedViewNameFormatVersion) {
    this.indexId = indexId;
    this.name = name;
    this.database = database;
    this.lastObservedCollectionName = lastObservedCollectionName;
    this.collectionUuid = collectionUuid;
    this.view = view;
    this.numPartitions = numPartitions;
    this.analyzerName = analyzerName;
    this.searchAnalyzerName = searchAnalyzerName;
    this.mappings = mappings;
    this.staticFieldDefinitions = staticFieldDefinitions;
    this.nonStockAnalyzerNames = nonStockAnalyzerNames;
    this.analyzers = analyzers;
    this.indexFeatureVersion = parsedIndexFeatureVersion;
    this.synonyms = synonyms;
    this.storedSource = storedSource;
    this.typeSets = typeSets;
    this.sort = sort;
    this.typeSetsMap =
        this.typeSets
            .map(
                list ->
                    list.stream()
                        .collect(
                            CollectionUtils.toMapUnsafe(
                                TypeSetDefinition::name, Function.identity())))
            .orElse(Collections.emptyMap());
    this.definitionVersion = definitionVersion;
    this.definitionVersionCreatedAt = definitionVersionCreatedAt;
    this.isAutoEmbeddingIndex = calculateIsAutoEmbeddingIndex(mappings);
    this.modelNamePerPath = calculateModelNamePerPath(mappings);
    this.indexIdAtCreationTime = indexIdAtCreationTime;
    this.autoEmbeddingDefinitionVersion = autoEmbeddingDefinitionVersion;
    this.materializedViewNameFormatVersion = materializedViewNameFormatVersion;
  }

  public static SearchIndexDefinition create(
      ObjectId indexId,
      String name,
      String database,
      String lastObservedCollectionName,
      UUID collectionUuid,
      Optional<ViewDefinition> view,
      int numPartitions,
      DocumentFieldDefinition mappings,
      Optional<String> analyzerName,
      Optional<String> searchAnalyzerName,
      Optional<List<CustomAnalyzerDefinition>> analyzers,
      int indexFeatureVersionNumber,
      Optional<List<SynonymMappingDefinition>> synonyms,
      Optional<StoredSourceDefinition> storedSource,
      Optional<List<TypeSetDefinition>> typeSets,
      Optional<Sort> sort,
      Optional<Long> definitionVersion,
      Optional<Instant> definitionVersionCreatedAt,
      Optional<ObjectId> indexIdAtCreationTime,
      Optional<Long> autoEmbeddingDefinitionVersion,
      Optional<Long> materializedViewNameFormatVersion) {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    registerFields(staticFields, Optional.empty(), mappings);
    UnmodifiableTrie<String, FieldDefinition> unmodifiableStaticFields =
        new UnmodifiableTrie<>(staticFields);

    Set<String> analyzerNames = new HashSet<>();
    analyzerName.ifPresent(analyzerNames::add);
    searchAnalyzerName.ifPresent(analyzerNames::add);
    registerFieldAnalyzerNames(mappings, analyzerNames);
    synonyms.ifPresent(synMappings -> registerSynonymAnalyzerNames(synMappings, analyzerNames));
    typeSets.ifPresent(
        typeSetDefinitions -> registerTypeSetAnalyzerNames(typeSetDefinitions, analyzerNames));
    Set<String> nonStockAnalyzerNames =
        analyzerNames.stream()
            .filter(Predicate.not(StockAnalyzerNames::isStockAnalyzer))
            .collect(Collectors.toUnmodifiableSet());

    return new SearchIndexDefinition(
        indexId,
        name,
        database,
        lastObservedCollectionName,
        collectionUuid,
        view,
        numPartitions,
        mappings,
        analyzerName,
        searchAnalyzerName,
        unmodifiableStaticFields,
        nonStockAnalyzerNames,
        analyzers,
        indexFeatureVersionNumber,
        synonyms,
        storedSource,
        typeSets,
        sort,
        definitionVersion,
        definitionVersionCreatedAt,
        indexIdAtCreationTime,
        autoEmbeddingDefinitionVersion,
        materializedViewNameFormatVersion);
  }

  /**
   * Creates a new SearchIndexDefinition with an updated view definition.
   *
   * @param updatedViewDefinition the new view definition to associate with this index
   * @return a new SearchIndexDefinition instance with the updated view definition
   */
  public SearchIndexDefinition withUpdatedViewDefinition(ViewDefinition updatedViewDefinition) {
    return new SearchIndexDefinition(
        this.indexId,
        this.name,
        this.database,
        this.lastObservedCollectionName,
        this.collectionUuid,
        Optional.of(updatedViewDefinition),
        this.numPartitions,
        this.mappings,
        this.analyzerName,
        this.searchAnalyzerName,
        this.staticFieldDefinitions,
        this.nonStockAnalyzerNames,
        this.analyzers,
        this.indexFeatureVersion,
        this.synonyms,
        this.storedSource,
        this.typeSets,
        this.sort,
        this.definitionVersion,
        this.definitionVersionCreatedAt,
        this.indexIdAtCreationTime,
        this.autoEmbeddingDefinitionVersion,
        this.materializedViewNameFormatVersion);
  }

  public static SearchIndexDefinition fromBson(BsonDocument document) throws BsonParseException {
    // Atlas sends extra fields that we don't care about, such as "createdDate" and
    // "lastUpdatedDate", so ignore any extra fields.
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return fromBson(parser);
    }
  }

  public static SearchIndexDefinition fromBson(DocumentParser parser) throws BsonParseException {
    ObjectId indexId = parser.getField(IndexDefinition.Fields.INDEX_ID).unwrap();
    Logger log = LoggerFactory.getLogger(SearchIndexDefinition.class);
    int parsedIndexFeatureVersion = parser.getField(Fields.INDEX_FEATURE_VERSION).unwrap();
    if (parsedIndexFeatureVersion > SearchIndexCapabilities.CURRENT_FEATURE_VERSION) {
      log.warn(
          "indexFeatureVersion [{}] is higher than current IFV [{}] for index [{}]",
          parsedIndexFeatureVersion,
          SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
          indexId);
    }

    DocumentFieldDefinition mappings = parser.getField(Fields.MAPPINGS).unwrap();
    Optional<List<TypeSetDefinition>> typeSets = parser.getField(Fields.TYPE_SETS).unwrap();
    validateTypeSetReferences(typeSets, mappings);
    Optional<Sort> sort = parser.getField(Fields.SORT).unwrap();
    IndexSortValidator.validateSort(sort, mappings);

    return create(
        indexId,
        parser.getField(IndexDefinition.Fields.NAME).unwrap(),
        parser.getField(IndexDefinition.Fields.DATABASE).unwrap(),
        parser.getField(IndexDefinition.Fields.LAST_OBSERVED_COLLECTION_NAME).unwrap(),
        parser.getField(IndexDefinition.Fields.COLLECTION_UUID).unwrap(),
        parser.getField(IndexDefinition.Fields.VIEW).unwrap(),
        parser.getField(IndexDefinition.Fields.NUM_PARTITIONS).unwrap(),
        mappings,
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(Fields.SEARCH_ANALYZER).unwrap(),
        parser.getField(Fields.ANALYZERS).unwrap(),
        // Initialize with the minimum of the 2 IndexFeatureVersions
        Integer.min(parsedIndexFeatureVersion, SearchIndexCapabilities.CURRENT_FEATURE_VERSION),
        parser.getField(Fields.SYNONYMS).unwrap(),
        parser.getField(IndexDefinition.Fields.STORED_SOURCE).unwrap(),
        typeSets,
        sort,
        parser.getField(IndexDefinition.Fields.DEFINITION_VERSION).unwrap(),
        DateUtil.parseInstantFromString(
            parser, DATE_FORMAT, IndexDefinition.Fields.DEFINITION_VERSION_CREATED_AT),
        parser.getField(IndexDefinition.Fields.INDEX_ID_AT_CREATION_TIME).unwrap(),
        parser.getField(IndexDefinition.Fields.AUTO_EMBEDDING_DEFINITION_VERSION).unwrap(),
        parser.getField(IndexDefinition.Fields.MATERIALIZED_VIEW_NAME_FORMAT_VERSION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(IndexDefinition.Fields.INDEX_ID, this.indexId)
        .field(IndexDefinition.Fields.NAME, this.name)
        .field(IndexDefinition.Fields.DATABASE, this.database)
        .field(
            IndexDefinition.Fields.LAST_OBSERVED_COLLECTION_NAME, this.lastObservedCollectionName)
        .field(IndexDefinition.Fields.COLLECTION_UUID, this.collectionUuid)
        .field(IndexDefinition.Fields.VIEW, this.view)
        .field(IndexDefinition.Fields.NUM_PARTITIONS, this.numPartitions)
        .field(Fields.MAPPINGS, this.mappings)
        .field(Fields.ANALYZER, this.analyzerName)
        .field(Fields.SEARCH_ANALYZER, this.searchAnalyzerName)
        .field(Fields.ANALYZERS, this.analyzers)
        .field(Fields.SYNONYMS, this.synonyms)
        .field(Fields.INDEX_FEATURE_VERSION, this.indexFeatureVersion)
        .field(IndexDefinition.Fields.STORED_SOURCE, this.storedSource)
        .field(Fields.TYPE_SETS, this.typeSets)
        .field(Fields.SORT, this.sort)
        .field(IndexDefinition.Fields.DEFINITION_VERSION, this.definitionVersion)
        .field(
            IndexDefinition.Fields.DEFINITION_VERSION_CREATED_AT,
            this.definitionVersionCreatedAt.map(DATE_FORMAT::format))
        .fieldOmitDefaultValue(
            IndexDefinition.Fields.INDEX_ID_AT_CREATION_TIME,
            this.indexIdAtCreationTime,
            this.indexId)
        .field(
            IndexDefinition.Fields.AUTO_EMBEDDING_DEFINITION_VERSION,
            this.autoEmbeddingDefinitionVersion)
        .field(
            IndexDefinition.Fields.MATERIALIZED_VIEW_NAME_FORMAT_VERSION,
            this.materializedViewNameFormatVersion)
        .build();
  }

  @Override
  public Type getType() {
    return Type.SEARCH;
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
  public String getLastObservedCollectionName() {
    return this.lastObservedCollectionName;
  }

  @Override
  public void setLastObservedCollectionName(String observedCollectionName) {
    this.lastObservedCollectionName = observedCollectionName;
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
  public UUID getCollectionUuid() {
    return this.collectionUuid;
  }

  public DocumentFieldDefinition getMappings() {
    return this.mappings;
  }

  public Optional<String> getAnalyzerName() {
    return this.analyzerName;
  }

  public Optional<String> getSearchAnalyzerName() {
    return this.searchAnalyzerName;
  }

  public List<CustomAnalyzerDefinition> getAnalyzers() {
    return this.analyzers.orElseGet(Collections::emptyList);
  }

  public Map<String, CustomAnalyzerDefinition> getAnalyzerMap() {
    return getAnalyzers().stream()
        .collect(CollectionUtils.toMapUnsafe(CustomAnalyzerDefinition::name, Function.identity()));
  }

  public Optional<List<SynonymMappingDefinition>> getSynonyms() {
    return this.synonyms;
  }

  public Map<String, SynonymMappingDefinition> getSynonymMap() {
    return getSynonyms().orElseGet(Collections::emptyList).stream()
        .collect(CollectionUtils.toMapUnsafe(definition -> definition.name(), Function.identity()));
  }

  @Override
  public StoredSourceDefinition getStoredSource() {
    return this.storedSource.orElse(StoredSourceDefinition.defaultValue());
  }

  public Optional<List<TypeSetDefinition>> getTypeSets() {
    return this.typeSets;
  }

  public Optional<Sort> getSort() {
    return this.sort;
  }

  /**
   * Since each {@link TypeSetDefinition#name()} is guaranteed to be unique across an index
   * definition, this getter returns <code>typeSets</code> in map form for easy access.
   */
  public Map<String, TypeSetDefinition> getTypeSetsMap() {
    return this.typeSetsMap;
  }

  @Override
  public Optional<Long> getDefinitionVersion() {
    return this.definitionVersion;
  }

  @Override
  public Optional<Instant> getDefinitionVersionCreatedAt() {
    return this.definitionVersionCreatedAt;
  }

  @Override
  public int getParsedIndexFeatureVersion() {
    return this.indexFeatureVersion;
  }

  @Override
  public SearchIndexCapabilities getIndexCapabilities(IndexFormatVersion version) {
    return new SearchIndexCapabilities(version, this.indexFeatureVersion);
  }

  public boolean isStringFacetsFieldIndexed() {
    return !this.getStaticFieldDefinitionsOfType(FieldDefinition::stringFacetFieldDefinition)
        .isEmpty();
  }

  /** Returns fields of the given type in the static configuration. */
  @VisibleForTesting
  public <T> Set<Map.Entry<String, T>> getStaticFieldDefinitionsOfType(
      Function<FieldDefinition, Optional<T>> typeFilter) {
    return this.staticFieldDefinitions.entrySet().stream()
        .map(entry -> typeFilter.apply(entry.getValue()).map(val -> Map.entry(entry.getKey(), val)))
        .flatMap(Optional::stream)
        .collect(Collectors.toSet());
  }

  /** Returns the fields in the static configuration. */
  UnmodifiableTrie<String, FieldDefinition> getStaticFields() {
    return this.staticFieldDefinitions;
  }

  /**
   * An {@link EmbeddedDocumentsFieldDefinition} field may be the child of another {@link
   * EmbeddedDocumentsFieldDefinition}. An {@link EmbeddedDocumentsFieldDefinition} with an {@link
   * EmbeddedDocumentsFieldDefinition} field as a child is said to have two "layers" of
   * embeddedDocuments fields.
   *
   * <p>Atlas Search imposes limits on how many layers of {@link EmbeddedDocumentsFieldDefinition}s
   * may be in a single index definition.
   *
   * <p>This method returns the number of embeddedDocuments "layers" in the most-deeply layered
   * embeddedDocuments field in this index.
   */
  public int getNumEmbeddedDocumentsLayers() {
    return this.mappings.fieldHierarchyContext().getNumEmbeddedDocumentsLayers();
  }

  /**
   * Returns true if this index definition has an {@link EmbeddedDocumentsFieldDefinition} defined.
   */
  public boolean hasEmbeddedFields() {
    return this.mappings.fieldHierarchyContext().getNumEmbeddedDocumentsLayers() != 0;
  }

  public boolean isIndexedAsEmbeddedDocumentsField(FieldPath path) {
    return getFieldHierarchyContext().getFieldsByEmbeddedRoot().containsKey(path);
  }

  public FieldHierarchyContext getFieldHierarchyContext() {
    return this.mappings.fieldHierarchyContext();
  }

  /**
   * Returns a set of the names of all of the non-stock analyzers referenced in this definition.
   *
   * <p>Includes analyzers named in:
   *
   * <ul>
   *   <li>the {@code analyzer} field
   *   <li>the {@code searchAnalyzer} field
   *   <li>statically defined {@link com.xgen.mongot.index.definition.StringFieldDefinition}s
   *   <li>statically defined {@link com.xgen.mongot.index.definition.AutocompleteFieldDefinition}s
   *   <li>{@link TypeSetDefinition}
   *   <li>{@link com.xgen.mongot.index.definition.SynonymMappingDefinition}s
   * </ul>
   */
  public Set<String> getNonStockAnalyzerNames() {
    return this.nonStockAnalyzerNames;
  }

  @Override
  public SearchFieldDefinitionResolver createFieldDefinitionResolver(
      IndexFormatVersion indexFormatVersion) {
    return new SearchFieldDefinitionResolver(this, indexFormatVersion);
  }

  @Override
  public boolean isAutoEmbeddingIndex() {
    return this.isAutoEmbeddingIndex;
  }

  /**
   * Returns the auto-embedding feature version for this search index. Returns 2 (materialized view
   * based) if auto-embed fields are present, 0 otherwise.
   */
  public int getParsedAutoEmbeddingFeatureVersion() {
    return this.isAutoEmbeddingIndex ? 2 : 0;
  }

  public ImmutableMap<FieldPath, String> getModelNamePerPath() {
    return this.modelNamePerPath;
  }

  private static boolean calculateIsAutoEmbeddingIndex(DocumentFieldDefinition mappings) {
    return mappings.fields().values().stream()
        .anyMatch(fd -> fd.searchAutoEmbedFieldDefinition().isPresent());
  }

  private static ImmutableMap<FieldPath, String> calculateModelNamePerPath(
      DocumentFieldDefinition mappings) {
    ImmutableMap.Builder<FieldPath, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, FieldDefinition> entry : mappings.fields().entrySet()) {
      entry
          .getValue()
          .searchAutoEmbedFieldDefinition()
          .ifPresent(
              autoEmbed ->
                  builder.put(FieldPath.parse(entry.getKey()), autoEmbed.modelName()));
    }
    return builder.build();
  }

  @Override
  public Optional<ObjectId> getIndexIdAtCreationTime() {
    return this.indexIdAtCreationTime;
  }

  @Override
  public Optional<Long> getAutoEmbeddingDefinitionVersion() {
    return this.autoEmbeddingDefinitionVersion;
  }

  @Override
  public Optional<Long> getMaterializedViewNameFormatVersion() {
    return this.materializedViewNameFormatVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchIndexDefinition that = (SearchIndexDefinition) o;
    return this.numPartitions == that.numPartitions
        && this.indexFeatureVersion == that.indexFeatureVersion
        && Objects.equal(this.indexId, that.indexId)
        && Objects.equal(this.name, that.name)
        && Objects.equal(this.database, that.database)
        && Objects.equal(this.lastObservedCollectionName, that.lastObservedCollectionName)
        && Objects.equal(this.collectionUuid, that.collectionUuid)
        && Objects.equal(this.view, that.view)
        && Objects.equal(this.analyzerName, that.analyzerName)
        && Objects.equal(this.searchAnalyzerName, that.searchAnalyzerName)
        && Objects.equal(this.mappings, that.mappings)
        && Objects.equal(this.analyzers, that.analyzers)
        && Objects.equal(this.synonyms, that.synonyms)
        && Objects.equal(this.storedSource, that.storedSource)
        && Objects.equal(this.typeSets, that.typeSets)
        && Objects.equal(this.sort, that.sort)
        && Objects.equal(this.definitionVersion, that.definitionVersion)
        // When serializing to BSON we convert to a string with second granularity (See
        // IndexDefinition#DATE_FORMAT). So the deserialized object equals the original we can only
        // compare with second granularity.
        && Objects.equal(
            this.definitionVersionCreatedAt.map(Instant::getEpochSecond),
            that.definitionVersionCreatedAt.map(Instant::getEpochSecond))
        && Objects.equal(this.staticFieldDefinitions, that.staticFieldDefinitions)
        && Objects.equal(this.nonStockAnalyzerNames, that.nonStockAnalyzerNames)
        && Objects.equal(
            this.indexIdAtCreationTime.orElse(this.indexId),
            that.indexIdAtCreationTime.orElse(that.indexId))
        && Objects.equal(this.autoEmbeddingDefinitionVersion, that.autoEmbeddingDefinitionVersion)
        && Objects.equal(
            this.materializedViewNameFormatVersion, that.materializedViewNameFormatVersion);
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
        this.analyzerName,
        this.searchAnalyzerName,
        this.mappings,
        this.analyzers,
        this.indexFeatureVersion,
        this.synonyms,
        this.storedSource,
        this.typeSets,
        this.sort,
        this.definitionVersion,
        this.definitionVersionCreatedAt.map(Instant::getEpochSecond),
        this.staticFieldDefinitions,
        this.nonStockAnalyzerNames,
        this.indexIdAtCreationTime.orElse(this.indexId),
        this.autoEmbeddingDefinitionVersion,
        this.materializedViewNameFormatVersion);
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
        "%s (index %s %s (%s) in database %s)",
        this.indexId, this.name, collection, this.collectionUuid, this.database);
  }

  private static void validateTypeSetReferences(
      Optional<List<TypeSetDefinition>> typeSets, DocumentFieldDefinition mappings)
      throws BsonParseException {
    Set<String> definedTypeSets =
        typeSets.stream()
            .flatMap(list -> list.stream().map(TypeSetDefinition::name))
            .collect(Collectors.toSet());

    Set<String> invalidTypeSets = new HashSet<>();

    if (mappings.dynamic() instanceof DynamicDefinition.Document dynamicDocument
        && !definedTypeSets.contains(dynamicDocument.typeSet())) {
      invalidTypeSets.add(dynamicDocument.typeSet());
    }

    // All typeSets defined at both embedded and non-embedded root paths
    Set<String> rootDefinedTypeSets =
        mappings.fieldHierarchyContext().getRootFields().values().stream()
            .flatMap(SearchIndexDefinition::extractTypeSetNames)
            .collect(Collectors.toSet());
    if (!definedTypeSets.containsAll(rootDefinedTypeSets)) {
      invalidTypeSets.addAll(Sets.difference(rootDefinedTypeSets, definedTypeSets));
    }

    // All typeSets across all embedded paths, does not include the root embedded path
    Set<String> embeddedTypeSets =
        mappings.fieldHierarchyContext().getFieldsByEmbeddedRoot().values().stream()
            .flatMap(x -> x.values().stream())
            .flatMap(SearchIndexDefinition::extractTypeSetNames)
            .collect(Collectors.toSet());
    if (!definedTypeSets.containsAll(embeddedTypeSets)) {
      invalidTypeSets.addAll(Sets.difference(embeddedTypeSets, definedTypeSets));
    }

    if (!invalidTypeSets.isEmpty()) {
      throw new BsonParseException(
          String.format("Invalid typeSets defined in index definition: %s", invalidTypeSets),
          Optional.empty());
    }
  }

  /** Extracts all <code>typeSet</code> names defined in a {@link FieldDefinition}. */
  private static Stream<String> extractTypeSetNames(FieldDefinition fieldDefinition) {
    return Stream.of(
            fieldDefinition.documentFieldDefinition(),
            fieldDefinition.embeddedDocumentsFieldDefinition())
        .flatMap(Optional::stream)
        .map(
            (HierarchicalFieldDefinition docField) -> {
              Optional<DynamicDefinition.Document> result =
                  switch (docField.dynamic()) {
                    case DynamicDefinition.Boolean b -> Optional.empty();
                    case DynamicDefinition.Document d -> Optional.of(d);
                  };
              return result;
            })
        .flatMap(Optional::stream)
        .map(DynamicDefinition.Document::typeSet);
  }

  /**
   * Recursively adds fields from ObjectFields with their `full.dotted.path` to the supplied Trie.
   *
   * <p>TODO(CLOUDP-280897): In theory a sufficiently nested index definition could result in this
   * recursing deep enough to exceed the max stack size. We may want to consider putting a max
   * nesting level to protect against this (especially if multi-tenanting may be in the future).
   */
  static void registerFields(
      Trie<String, FieldDefinition> fields,
      Optional<FieldPath> path,
      DocumentFieldDefinition field) {
    // Register each child in field into fields, and recurse if field has an object definition.
    for (Map.Entry<String, FieldDefinition> entry : field.fields().entrySet()) {
      FieldDefinition childDefinition = entry.getValue();
      FieldPath childPath =
          path.map(p -> p.newChild(entry.getKey()))
              .orElseGet(() -> FieldPath.parse(entry.getKey()));

      fields.put(childPath.toString(), childDefinition);

      if (childDefinition.documentFieldDefinition().isPresent()) {
        registerFields(
            fields, Optional.of(childPath), childDefinition.documentFieldDefinition().get());
      }
    }
  }

  /**
   * Recursively adds all Analyzer names of children of the documentFieldDefinitionProxy to
   * analyzerNames.
   */
  private static void registerFieldAnalyzerNames(
      HierarchicalFieldDefinition hierarchicalFieldDefinition, Set<String> analyzerNames) {
    // For each field in the DocumentFieldDefinition:
    //   - if it has a StringFieldDefinition/AutocompleteFieldDefinition, add their analyzer names
    //     if they exist
    //   - if it has a DocumentFieldDefinition, recurse
    for (FieldDefinition fieldDefinition : hierarchicalFieldDefinition.fields().values()) {
      registerStringAndAutocompleteAnalyzerNames(fieldDefinition, analyzerNames);

      fieldDefinition
          .documentFieldDefinition()
          .ifPresent(documentField -> registerFieldAnalyzerNames(documentField, analyzerNames));

      fieldDefinition
          .embeddedDocumentsFieldDefinition()
          .ifPresent(
              embeddedDocsField -> registerFieldAnalyzerNames(embeddedDocsField, analyzerNames));
    }
  }

  private static void registerSynonymAnalyzerNames(
      List<SynonymMappingDefinition> synonymMappings, Set<String> analyzerNames) {
    synonymMappings.forEach(synonymMapping -> analyzerNames.add(synonymMapping.analyzer()));
  }

  private static void registerTypeSetAnalyzerNames(
      List<TypeSetDefinition> typeSets, Set<String> analyzerNames) {
    CheckedStream.from(typeSets.stream())
        .forEachChecked(
            typeSet ->
                registerStringAndAutocompleteAnalyzerNames(
                    typeSet.getFieldDefinition(), analyzerNames));
  }

  private static void registerStringAndAutocompleteAnalyzerNames(
      FieldDefinition fieldDefinition, Set<String> analyzerNames) {
    if (fieldDefinition.stringFieldDefinition().isPresent()) {
      StringFieldDefinition stringFieldDefinition = fieldDefinition.stringFieldDefinition().get();
      stringFieldDefinition.analyzerName().ifPresent(analyzerNames::add);
      stringFieldDefinition.searchAnalyzerName().ifPresent(analyzerNames::add);

      for (StringFieldDefinition multi : stringFieldDefinition.multi().values()) {
        multi.analyzerName().ifPresent(analyzerNames::add);
        multi.searchAnalyzerName().ifPresent(analyzerNames::add);
      }
    }

    fieldDefinition
        .autocompleteFieldDefinition()
        .map(AutocompleteFieldDefinition::getAnalyzer)
        .ifPresent(analyzerNames::add);
  }
}
