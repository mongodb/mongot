package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.util.Check;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;

public class SearchIndexDefinitionBuilder {

  private Optional<ObjectId> indexId = Optional.empty();
  private Optional<String> name = Optional.empty();
  private Optional<String> database = Optional.empty();
  private Optional<String> lastObservedCollectionName = Optional.empty();
  private Optional<UUID> collectionUuid = Optional.empty();
  private Optional<ViewDefinition> view = Optional.empty();
  private int numPartitions = IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue();
  private Optional<DocumentFieldDefinition> mappings = Optional.empty();
  private Optional<String> analyzerName = Optional.empty();
  private Optional<String> searchAnalyzerName = Optional.empty();
  private Optional<List<CustomAnalyzerDefinition>> analyzers = Optional.empty();
  private Optional<List<SynonymMappingDefinition>> synonyms = Optional.empty();
  private Optional<StoredSourceDefinition> storedSource = Optional.empty();
  private Optional<List<TypeSetDefinition>> typeSets = Optional.empty();
  private Optional<Sort> sort = Optional.empty();
  private Optional<Integer> indexFeatureVersion =
      Optional.of(SearchIndexDefinition.Fields.INDEX_FEATURE_VERSION.getDefaultValue());
  private Optional<Long> definitionVersion = Optional.empty();
  private Optional<Instant> definitionVersionCreatedAt = Optional.empty();
  private Optional<ObjectId> definitionVersionId = Optional.empty();
  private Optional<ObjectId> indexIdAtCreationTime = Optional.empty();
  private Optional<Long> autoEmbeddingDefinitionVersion = Optional.empty();
  private Optional<Long> materializedViewNameFormatVersion = Optional.empty();

  /**
   * This is a valid, minimally configured {@link SearchIndexDefinition} instance with dynamic
   * mapping. Use {@link #from(SearchIndexDefinition)} to create modified instances.
   */
  public static final SearchIndexDefinition VALID_INDEX =
      builder()
          .defaultMetadata()
          .dynamicMapping()
          .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
          .build();

  public static SearchIndexDefinitionBuilder builder() {
    return new SearchIndexDefinitionBuilder();
  }

  /** A builder initialized with all the same fields as the supplied prototype. */
  public static SearchIndexDefinitionBuilder from(SearchIndexDefinition protoType) {
    SearchIndexDefinitionBuilder builder =
        builder()
            .indexId(protoType.getIndexId())
            .name(protoType.getName())
            .database(protoType.getDatabase())
            .collectionUuid(protoType.getCollectionUuid())
            .lastObservedCollectionName(protoType.getLastObservedCollectionName())
            .numPartitions(protoType.getNumPartitions())
            .mappings(protoType.getMappings())
            .analyzers(protoType.getAnalyzers());

    protoType.getSynonyms().ifPresent(builder::synonyms);
    protoType.getAnalyzerName().ifPresent(builder::analyzerName);
    protoType.getSearchAnalyzerName().ifPresent(builder::searchAnalyzerName);
    protoType.getDefinitionVersion().ifPresent(builder::definitionVersion);
    protoType.getDefinitionVersionCreatedAt().ifPresent(builder::definitionVersionCreatedAt);
    protoType.getDefinitionVersionId().ifPresent(builder::definitionVersionId);
    return builder;
  }

  /**
   * Sets the IndexDefinitionBuilder's name, database, and collection to be "default", "database",
   * and "collection", and the indexId and collectionUuid to be randomly generated values.
   */
  public SearchIndexDefinitionBuilder defaultMetadata() {
    return this.indexId(new ObjectId())
        .name("default")
        .database("database")
        .lastObservedCollectionName("collection")
        .collectionUuid(UUID.randomUUID());
  }

  public SearchIndexDefinitionBuilder indexId(ObjectId indexId) {
    this.indexId = Optional.ofNullable(indexId);
    return this;
  }

  public SearchIndexDefinitionBuilder assignRandomIndexId() {
    return indexId(new ObjectId());
  }

  public SearchIndexDefinitionBuilder name(String name) {
    this.name = Optional.ofNullable(name);
    return this;
  }

  public SearchIndexDefinitionBuilder database(String database) {
    this.database = Optional.ofNullable(database);
    return this;
  }

  public SearchIndexDefinitionBuilder lastObservedCollectionName(
      String lastObservedCollectionName) {
    this.lastObservedCollectionName = Optional.ofNullable(lastObservedCollectionName);
    return this;
  }

  public SearchIndexDefinitionBuilder collectionUuid(UUID collectionUuid) {
    this.collectionUuid = Optional.ofNullable(collectionUuid);
    return this;
  }

  public SearchIndexDefinitionBuilder view(ViewDefinition view) {
    this.view = Optional.of(view);
    return this;
  }

  public SearchIndexDefinitionBuilder numPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  public SearchIndexDefinitionBuilder dynamicMapping() {
    this.mappings =
        Optional.ofNullable(DocumentFieldDefinitionBuilder.builder().dynamic(true).build());
    return this;
  }

  public SearchIndexDefinitionBuilder mappings(DocumentFieldDefinition mappings) {
    this.mappings = Optional.ofNullable(mappings);
    return this;
  }

  public SearchIndexDefinitionBuilder analyzerName(String analyzerName) {
    this.analyzerName = Optional.ofNullable(analyzerName);
    return this;
  }

  public SearchIndexDefinitionBuilder searchAnalyzerName(String searchAnalyzerName) {
    this.searchAnalyzerName = Optional.ofNullable(searchAnalyzerName);
    return this;
  }

  public SearchIndexDefinitionBuilder analyzers(List<CustomAnalyzerDefinition> analyzers) {
    this.analyzers = Optional.ofNullable(analyzers);
    return this;
  }

  public SearchIndexDefinitionBuilder synonyms(List<SynonymMappingDefinition> synonyms) {
    this.synonyms = Optional.ofNullable(synonyms);
    return this;
  }

  public SearchIndexDefinitionBuilder indexFeatureVersion(int indexFeatureVersion) {
    this.indexFeatureVersion = Optional.of(indexFeatureVersion);
    return this;
  }

  public SearchIndexDefinitionBuilder storedSource(StoredSourceDefinition storedSource) {
    this.storedSource = Optional.ofNullable(storedSource);
    return this;
  }

  public SearchIndexDefinitionBuilder typeSets(TypeSetDefinition... typeSetDefinitions) {
    this.typeSets = Optional.of(Arrays.stream(typeSetDefinitions).toList());
    return this;
  }

  public SearchIndexDefinitionBuilder typeSets(List<TypeSetDefinition> typeSetDefinitions) {
    this.typeSets = Optional.of(typeSetDefinitions);
    return this;
  }

  public SearchIndexDefinitionBuilder sort(Sort sort) {
    this.sort = Optional.of(sort);
    return this;
  }

  public SearchIndexDefinitionBuilder definitionVersion(Long version) {
    this.definitionVersion = Optional.ofNullable(version);
    return this;
  }

  public SearchIndexDefinitionBuilder definitionVersionCreatedAt(Instant versionCreatedAt) {
    this.definitionVersionCreatedAt = Optional.ofNullable(versionCreatedAt);
    return this;
  }

  public SearchIndexDefinitionBuilder definitionVersionId(ObjectId definitionVersionId) {
    this.definitionVersionId = Optional.ofNullable(definitionVersionId);
    return this;
  }

  public SearchIndexDefinitionBuilder indexIdAtCreationTime(ObjectId indexIdAtCreationTime) {
    this.indexIdAtCreationTime = Optional.ofNullable(indexIdAtCreationTime);
    return this;
  }

  public SearchIndexDefinitionBuilder autoEmbeddingDefinitionVersion(
      Long autoEmbeddingDefinitionVersion) {
    this.autoEmbeddingDefinitionVersion = Optional.ofNullable(autoEmbeddingDefinitionVersion);
    return this;
  }

  public SearchIndexDefinitionBuilder materializedViewNameFormatVersion(
      Long materializedViewNameFormatVersion) {
    this.materializedViewNameFormatVersion = Optional.ofNullable(materializedViewNameFormatVersion);
    return this;
  }

  public SearchIndexDefinition build() {
    return SearchIndexDefinition.create(
        Check.isPresent(this.indexId, "indexId"),
        Check.isPresent(this.name, "name"),
        Check.isPresent(this.database, "database"),
        Check.isPresent(this.lastObservedCollectionName, "lastObservedCollectionName"),
        Check.isPresent(this.collectionUuid, "collectionUuid"),
        this.view,
        this.numPartitions,
        Check.isPresent(this.mappings, "mappings"),
        this.analyzerName,
        this.searchAnalyzerName,
        this.analyzers,
        this.indexFeatureVersion.get(),
        this.synonyms,
        this.storedSource,
        this.typeSets,
        this.sort,
        this.definitionVersion,
        this.definitionVersionCreatedAt,
        this.definitionVersionId,
        this.indexIdAtCreationTime,
        this.autoEmbeddingDefinitionVersion,
        this.materializedViewNameFormatVersion);
  }

  public SearchIndexDefinitionGenerationBuilder asDefinitionGeneration() {
    return SearchIndexDefinitionGenerationBuilder.builder()
        .definition(AnalyzerBoundSearchIndexDefinition.create(build(), Collections.emptyList()));
  }
}
