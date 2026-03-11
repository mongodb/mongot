package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.writer.LuceneIndexWriter;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.FunctionalUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;

@VisibleForTesting
public final class LuceneSearchIndexMetricValuesSupplier extends LuceneIndexMetricValuesSupplier {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /**
   * Default cache duration for the numFieldsPerDatatype metric. This computation iterates over all
   * Lucene FieldInfos which can be expensive for large indices with many fields.
   */
  @VisibleForTesting
  public static final Duration DEFAULT_NUM_FIELDS_CACHE_DURATION = Duration.ofMinutes(5);

  private final SearchIndexReader indexReader;
  private final boolean hasEmbeddedFields;
  private final SearchFieldDefinitionResolver resolver;
  private final Set<String> autocompleteLikeCustomAnalyzerNames;
  private final Supplier<Map<FieldName.TypeField, Double>> cachedNumFieldsPerDatatype;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  public static class MetricNames extends IndexMetricValuesSupplier.MetricNames {
    public static final String NUM_EMBEDDED_ROOT_DOCS = "numEmbeddedRootDocs";
  }

  /**
   * Private constructor - does not register gauges. Use static factory methods to construct
   * instances.
   */
  private LuceneSearchIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      SearchIndexReader searchIndexReader,
      LuceneIndexWriter luceneIndexWriter,
      boolean hasEmbeddedFields,
      SearchFieldDefinitionResolver resolver,
      List<CustomAnalyzerDefinition> customAnalyzerDefinitions,
      Duration numFieldsCacheDuration,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    super(indexStatusSupplier, indexBackingStrategy, searchIndexReader, luceneIndexWriter);

    this.indexReader = searchIndexReader;
    this.hasEmbeddedFields = hasEmbeddedFields;
    this.resolver = resolver;
    this.autocompleteLikeCustomAnalyzerNames =
        getAutocompleteLikeCustomAnalyzerNames(customAnalyzerDefinitions);
    this.cachedNumFieldsPerDatatype =
        Suppliers.memoizeWithExpiration(
            this::computeNumFieldsPerDatatype,
            numFieldsCacheDuration.toMillis(),
            TimeUnit.MILLISECONDS);
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  /**
   * Static factory method that constructs the supplier and registers all gauges.
   */
  public static LuceneSearchIndexMetricValuesSupplier create(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      SearchIndexReader searchIndexReader,
      LuceneIndexWriter luceneIndexWriter,
      IndexFormatVersion indexFormatVersion,
      SearchIndexDefinition definition,
      PerIndexMetricsFactory indexStatsMetricFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return create(
        indexStatusSupplier,
        indexBackingStrategy,
        searchIndexReader,
        luceneIndexWriter,
        definition.hasEmbeddedFields(),
        definition.createFieldDefinitionResolver(indexFormatVersion),
        definition.getAnalyzers(),
        indexStatsMetricFactory,
        indexFeatureVersion,
        isIndexFeatureVersionFourEnabled,
        DEFAULT_NUM_FIELDS_CACHE_DURATION,
        dynamicFeatureFlagRegistry);
  }

  /**
   * Static factory method that constructs the supplier and registers all gauges.
   */
  public static LuceneSearchIndexMetricValuesSupplier create(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      SearchIndexReader searchIndexReader,
      LuceneIndexWriter luceneIndexWriter,
      boolean hasEmbeddedFields,
      SearchFieldDefinitionResolver resolver,
      List<CustomAnalyzerDefinition> customAnalyzerDefinitions,
      PerIndexMetricsFactory indexStatsMetricFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return create(
        indexStatusSupplier,
        indexBackingStrategy,
        searchIndexReader,
        luceneIndexWriter,
        hasEmbeddedFields,
        resolver,
        customAnalyzerDefinitions,
        indexStatsMetricFactory,
        indexFeatureVersion,
        isIndexFeatureVersionFourEnabled,
        DEFAULT_NUM_FIELDS_CACHE_DURATION,
        dynamicFeatureFlagRegistry);
  }

  /**
   * Static factory method with configurable cache duration for the numFieldsPerDatatype metric.
   */
  @VisibleForTesting
  public static LuceneSearchIndexMetricValuesSupplier create(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      SearchIndexReader searchIndexReader,
      LuceneIndexWriter luceneIndexWriter,
      boolean hasEmbeddedFields,
      SearchFieldDefinitionResolver resolver,
      List<CustomAnalyzerDefinition> customAnalyzerDefinitions,
      PerIndexMetricsFactory indexStatsMetricFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled,
      Duration numFieldsCacheDuration,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    LuceneSearchIndexMetricValuesSupplier supplier =
        new LuceneSearchIndexMetricValuesSupplier(
            indexStatusSupplier,
            indexBackingStrategy,
            searchIndexReader,
            luceneIndexWriter,
            hasEmbeddedFields,
            resolver,
            customAnalyzerDefinitions,
            numFieldsCacheDuration,
            dynamicFeatureFlagRegistry);

    // Register common gauges after construction is complete
    supplier.registerCommonGauges(
        indexStatsMetricFactory, indexFeatureVersion, isIndexFeatureVersionFourEnabled);

    // Register search-index-specific gauges
    if (supplier.hasEmbeddedFields) {
      indexStatsMetricFactory.perIndexObjectValueGauge(
          MetricNames.NUM_EMBEDDED_ROOT_DOCS,
          supplier,
          CachedGauge.of(
              LuceneSearchIndexMetricValuesSupplier::getNumEmbeddedRootDocs,
              Duration.ofMinutes(1)));
    }

    if (supplier.indexReader instanceof LuceneSearchIndexReader reader
        && reader.maxFacetCardinalityMetricEnabled()) {
      indexStatsMetricFactory.perIndexObjectValueGauge(
          MetricNames.MAX_STRING_FACET_CARDINALITY,
          supplier,
          CachedGauge.of(
              LuceneSearchIndexMetricValuesSupplier::getMaxStringFacetCardinality,
              Duration.ofMinutes(1)));
    }

    return supplier;
  }

  /**
   * Returns the names of custom analyzers that use edge_gram or n_gram tokenizers, which produce
   * autocomplete-like behavior.
   */
  private static Set<String> getAutocompleteLikeCustomAnalyzerNames(
      List<CustomAnalyzerDefinition> analyzers) {
    return analyzers.stream()
        .filter(
            analyzer ->
                analyzer.tokenizer().getType() == TokenizerDefinition.Type.EDGE_GRAM
                    || analyzer.tokenizer().getType() == TokenizerDefinition.Type.N_GRAM)
        .map(CustomAnalyzerDefinition::name)
        .collect(Collectors.toSet());
  }

  @Override
  public Map<FieldName.TypeField, Double> getNumFieldsPerDatatype() {
    // Check feature flag - if disabled, return empty map
    if (!this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.NUM_FIELDS_PER_DATATYPE_METRIC.getName(),
        DynamicFeatureFlags.NUM_FIELDS_PER_DATATYPE_METRIC.getFallback())) {
      return Collections.emptyMap();
    }
    return this.cachedNumFieldsPerDatatype.get();
  }

  /**
   * Computes the number of Lucene fields per datatype by iterating over FieldInfos in the index.
   * String fields configured with autocomplete-like custom analyzers (using edge_gram or n_gram
   * tokenizers) are counted as AUTOCOMPLETE type. This can be expensive for large indices with many
   * fields, so the result is cached.
   *
   * <p>Note: For MultiLuceneSearchIndexReader, getFieldInfos() returns FieldInfos from all
   * partitions. Since all partitions have the same field definitions, we only process the first
   * partition's fields.
   */
  private Map<FieldName.TypeField, Double> computeNumFieldsPerDatatype() {
    try {
      List<FieldInfos> fieldInfosList = this.indexReader.getFieldInfos();
      if (fieldInfosList.isEmpty()) {
        return Collections.emptyMap();
      }

      Map<FieldName.TypeField, Double> counts = new EnumMap<>(FieldName.TypeField.class);

      // All partitions have the same fields, so we only need to process the first one
      FieldInfos fieldInfos = fieldInfosList.get(0);
      StreamSupport.stream(fieldInfos.spliterator(), false)
          .filter(this::isDatatypeField)
          .map(fieldInfo -> classifyFieldType(fieldInfo.name))
          .flatMap(Optional::stream)
          .forEach(
              typeField -> counts.merge(typeField, 1.0, (existing, increment) -> existing + 1.0));

      return Collections.unmodifiableMap(counts);
    } catch (IOException | ReaderClosedException e) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
          "Failed to compute numFieldsPerDatatype metric, returning empty map");
      return Collections.emptyMap();
    }
  }

  /**
   * Returns true if the field is a datatype field (TypeField, EmbeddedField, or MultiField) that
   * should be counted in the metrics.
   */
  private boolean isDatatypeField(FieldInfo fieldInfo) {
    String name = fieldInfo.name;
    return FieldName.TypeField.getTypeOf(name).isPresent()
        || FieldName.EmbeddedField.isTypeOf(name)
        || FieldName.MultiField.isTypeOf(name);
  }

  /**
   * Classifies a Lucene field name into its TypeField. String fields configured with
   * autocomplete-like custom analyzers are classified as AUTOCOMPLETE.
   */
  private Optional<FieldName.TypeField> classifyFieldType(String luceneFieldName) {
    try {
      FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

      // For STRING fields when we have autocomplete-like analyzers, check if the field uses one
      if (!this.autocompleteLikeCustomAnalyzerNames.isEmpty()
          && components.typeField() == FieldName.TypeField.STRING) {
        return classifyStringFieldForAutocompleteLikeAnalyzer(components);
      }

      return Optional.of(components.typeField());
    } catch (IllegalArgumentException e) {
      // Field name could not be parsed
      FLOGGER.atInfo().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
          "Could not parse field name for metrics: %s", luceneFieldName);
      return Optional.empty();
    }
  }

  /**
   * Checks if a STRING field uses an autocomplete-like custom analyzer and returns the appropriate
   * classification. Returns Optional.empty() if the field should be skipped (when we have
   * autocomplete-like analyzers defined but can't find the field definition).
   *
   * @return Optional containing AUTOCOMPLETE if field uses autocomplete-like analyzer, STRING if it
   *     uses a regular analyzer, or empty if the field should be skipped
   */
  private Optional<FieldName.TypeField> classifyStringFieldForAutocompleteLikeAnalyzer(
      FieldName.Components components) {
    Optional<FieldDefinition> fieldDefinitionOpt =
        this.resolver.getFieldDefinition(components.fieldPath(), components.embeddedRoot());
    if (fieldDefinitionOpt.isEmpty()) {
      // If we have autocomplete-like analyzers but can't find field definition, skip the field
      return Optional.empty();
    }

    Optional<StringFieldDefinition> stringDefinitionOpt =
        fieldDefinitionOpt.get().stringFieldDefinition();
    if (stringDefinitionOpt.isEmpty()) {
      // Field definition exists but it's not a string field - skip
      return Optional.empty();
    }

    StringFieldDefinition stringDefinition = stringDefinitionOpt.get();

    // Check if this is a multi-field and get its definition
    Optional<StringFieldDefinition> multiDefinition =
        components
            .multiFieldPath()
            .map(StringMultiFieldPath::getMulti)
            .flatMap(multi -> Optional.ofNullable(stringDefinition.multi().get(multi)));

    // Collect all analyzer names referenced by this field
    Set<String> referencedAnalyzers = new HashSet<>();
    stringDefinition.analyzerName().ifPresent(referencedAnalyzers::add);
    stringDefinition.searchAnalyzerName().ifPresent(referencedAnalyzers::add);
    multiDefinition
        .flatMap(StringFieldDefinition::analyzerName)
        .ifPresent(referencedAnalyzers::add);
    multiDefinition
        .flatMap(StringFieldDefinition::searchAnalyzerName)
        .ifPresent(referencedAnalyzers::add);

    // Check if any referenced analyzer is autocomplete-like
    if (!Sets.intersection(referencedAnalyzers, this.autocompleteLikeCustomAnalyzerNames)
        .isEmpty()) {
      return Optional.of(FieldName.TypeField.AUTOCOMPLETE);
    }

    return Optional.of(FieldName.TypeField.STRING);
  }

  @Override
  public DocCounts getDocCounts() {
    long numDocs = getNumDocs();
    return new DocCounts(
        numDocs,
        getNumLuceneMaxDocs(),
        getMaxLuceneMaxDocs(),
        this.hasEmbeddedFields ? getNumEmbeddedRootDocs() : numDocs);
  }

  private long getNumEmbeddedRootDocs() {
    try {
      return this.indexReader.getNumEmbeddedRootDocuments();
    } catch (IOException | ReaderClosedException ignored) {
      return 0L;
    }
  }

  private int getMaxStringFacetCardinality() {
    return FunctionalUtils.getOrDefaultIfThrows(
        () -> {
          if (this.indexReader instanceof LuceneSearchIndexReader luceneSearchIndexReader) {
            return luceneSearchIndexReader.getMaxStringFacetCardinality();
          } else if (this.indexReader instanceof MultiLuceneSearchIndexReader multiReader) {
            return multiReader.getMaxStringFacetCardinality();
          }
          return 0;
        },
        Exception.class,
        0);
  }
}
