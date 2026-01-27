package com.xgen.mongot.index.lucene.query.context;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.QueryAnalyzerWrapper;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DateFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.ObjectIdFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SortableDateBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.SortableNumberBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.SortableStringBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.UuidFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.util.SafeQueryBuilder;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.SearchQueryTimeMappingChecks;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

public class SearchQueryFactoryContext implements QueryFactoryContext {

  private final AnalyzerRegistry analyzerRegistry;
  private final QueryAnalyzerWrapper analyzer;
  private final SearchFieldDefinitionResolver fieldDefinitionResolver;
  private final SynonymRegistry synonymRegistry;
  private final SearchQueryTimeMappingChecks queryTimeMappingChecks;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metrics;
  private final FeatureFlags featureFlags;

  public SearchQueryFactoryContext(
      AnalyzerRegistry analyzerRegistry,
      QueryAnalyzerWrapper analyzer,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      SynonymRegistry synonymRegistry,
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      FeatureFlags featureFlags) {
    this.analyzerRegistry = analyzerRegistry;
    this.analyzer = analyzer;
    this.fieldDefinitionResolver = fieldDefinitionResolver;
    this.synonymRegistry = synonymRegistry;
    this.queryTimeMappingChecks = new SearchQueryTimeMappingChecks(fieldDefinitionResolver);
    this.metrics = metrics;
    this.featureFlags = featureFlags;
  }

  public SafeQueryBuilder safeQueryBuilder(StringPath stringPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return new SafeQueryBuilder(getAnalyzer(stringPath, embeddedRoot));
  }

  public SafeQueryBuilder synonymQueryBuilder(
      StringPath stringPath, Optional<FieldPath> embeddedRoot, String synonymMappingName)
      throws InvalidQueryException {
    return SafeQueryBuilder.createSynonymsQueryBuilder(
        getSynonymAnalyzer(synonymMappingName, stringPath, embeddedRoot));
  }

  public Optional<AutocompleteFieldDefinition> getAutocompleteDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::autocompleteFieldDefinition);
  }

  public Optional<BooleanFieldDefinition> getBooleanFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::booleanFieldDefinition);
  }

  public Optional<DateFieldDefinition> getDateFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::dateFieldDefinition);
  }

  public Optional<NumberFieldDefinition> getNumericFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::numberFieldDefinition);
  }

  public Optional<ObjectIdFieldDefinition> getObjectIdFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::objectIdFieldDefinition);
  }

  public Optional<TokenFieldDefinition> getTokenFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::tokenFieldDefinition);
  }

  public Optional<SortableDateBetaV1FieldDefinition> getSortableDateFieldDefinition(
      FieldPath path) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, Optional.empty())
        .flatMap(FieldDefinition::sortableDateBetaV1FieldDefinition);
  }

  public Optional<SortableNumberBetaV1FieldDefinition> getSortableNumberFieldDefinition(
      FieldPath path) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, Optional.empty())
        .flatMap(FieldDefinition::sortableNumberBetaV1FieldDefinition);
  }

  public Optional<SortableStringBetaV1FieldDefinition> getSortableStringFieldDefinition(
      FieldPath path) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, Optional.empty())
        .flatMap(FieldDefinition::sortableStringBetaV1FieldDefinition);
  }

  public Optional<UuidFieldDefinition> getUuidFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::uuidFieldDefinition);
  }

  public Optional<EmbeddedDocumentsFieldDefinition> getEmbeddedDocumentsFieldDefinition(
      FieldPath path) {
    return this.fieldDefinitionResolver.getEmbeddedDocumentsFieldDefinition(path);
  }

  /**
   * This {@link Analyzer} can be applied to any valid Lucene field. Attempting to apply this
   * analyzer on a multi field that doesn't exist will throw an Exception.
   */
  public Analyzer getAnalyzer(StringPath stringPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    if (stringPath.isMultiField()) {
      this.analyzer.validateMultiPath(stringPath.asMultiField(), embeddedRoot);
    }
    return this.analyzer;
  }

  /**
   * No validation needed to get an analyzer to be used with token fields. Use {@link
   * SearchQueryFactoryContext#getAnalyzer(StringPath, Optional)} to safely get the analyzer for
   * string and multi-string fields.
   */
  @Override
  public Analyzer getTokenFieldAnalyzer() {
    return this.analyzer;
  }

  @Override
  public VectorSimilarity getIndexedVectorSimilarityFunction(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) throws InvalidQueryException {

    Optional<VectorSimilarity> vectorSimilarity =
        this.fieldDefinitionResolver
            .getFieldDefinition(fieldPath, embeddedRoot)
            .flatMap(FieldDefinition::vectorFieldSpecification)
            .map(VectorFieldSpecification::similarity);

    if (vectorSimilarity.isEmpty()) {
      throw new InvalidQueryException(
          String.format("%s is not indexed as a vector field", fieldPath));
    }

    return vectorSimilarity.get();
  }

  @Override
  public VectorQuantization getIndexedQuantization(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) {
    Optional<FieldDefinition> fieldDefinition =
        this.fieldDefinitionResolver.getFieldDefinition(fieldPath, embeddedRoot);
    if (fieldDefinition.isEmpty() || fieldDefinition.get().vectorFieldSpecification().isEmpty()) {
      return VectorQuantization.NONE;
    }

    return fieldDefinition.get().vectorFieldSpecification().get().quantization();
  }

  /**
   * Gets all the Lucene field names associated with a given path, including those corresponding to
   * any multi analyzers.
   */
  public List<String> getAllLuceneFieldNames(
      StringFieldPath stringFieldPath, Optional<FieldPath> embeddedRoot) {

    // All multis in this field, if any.
    var multiFieldNames =
        this.fieldDefinitionResolver
            .getStringFieldDefinition(stringFieldPath, embeddedRoot)
            .map(x -> x.multi().keySet())
            .orElse(ImmutableSet.of());

    List<String> luceneFieldNames = new ArrayList<>(multiFieldNames.size() + 1);
    luceneFieldNames.add(FieldName.getLuceneFieldNameForStringPath(stringFieldPath, embeddedRoot));

    // Get the analyzer to search each multi with.
    for (String multiName : multiFieldNames) {
      var pathForMulti = new StringMultiFieldPath(stringFieldPath.getBaseFieldPath(), multiName);
      String luceneFieldName =
          FieldName.getLuceneFieldNameForStringPath(pathForMulti, embeddedRoot);
      luceneFieldNames.add(luceneFieldName);
    }

    return luceneFieldNames;
  }

  /**
   * Get the autocomplete analyzer used at index-time. Useful for normalizing query tokens; not to
   * be used to tokenize query text.
   */
  public Analyzer getAutocompleteIndexAnalyzer(AutocompleteFieldDefinition autocompleteField) {
    return this.analyzerRegistry.getAutocompleteAnalyzer(autocompleteField);
  }

  /**
   * Get the base analyzer used by the autocomplete analyzer for a field. Useful for tokenizing
   * query text.
   */
  public Analyzer getAutocompleteBaseAnalyzer(AutocompleteFieldDefinition autocompleteField) {
    return this.analyzerRegistry.getAnalyzer(autocompleteField.getAnalyzer());
  }

  public Analyzer getSynonymAnalyzer(
      String synonymMappingName, StringPath stringPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    SynonymMapping synonymMapping =
        InvalidQueryException.wrapIfThrows(
            () -> this.synonymRegistry.get(synonymMappingName), SynonymMappingException.class);

    String fieldAnalyzerName = getAnalyzerMeta(stringPath, embeddedRoot).getName();
    InvalidQueryException.validate(
        Objects.equals(synonymMapping.baseAnalyzerName, fieldAnalyzerName),
        "synonym mapping \"%s\" is only allowed with %s analyzer, "
            + "field %s is analyzed with %s analyzer",
        synonymMappingName,
        synonymMapping.baseAnalyzerName,
        stringPath,
        fieldAnalyzerName);

    return synonymMapping.analyzer;
  }

  public AnalyzerMeta getAnalyzerMeta(StringPath stringPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return this.analyzer.getAnalyzerMeta(stringPath, embeddedRoot);
  }

  @Override
  public boolean isIndexWithEmbeddedFields() {
    return this.fieldDefinitionResolver.indexDefinition.hasEmbeddedFields();
  }

  public IndexCapabilities getIndexCapabilities() {
    return this.fieldDefinitionResolver.getIndexCapabilities();
  }

  @Override
  public SearchQueryTimeMappingChecks getQueryTimeMappingChecks() {
    return this.queryTimeMappingChecks;
  }

  @Override
  public SearchFieldDefinitionResolver getFieldDefinitionResolver() {
    return this.fieldDefinitionResolver;
  }

  @Override
  public FeatureFlags getFeatureFlags() {
    return this.featureFlags;
  }

  @Override
  public IndexMetricsUpdater.QueryingMetricsUpdater getMetrics() {
    return this.metrics;
  }

  public Optional<NumericFieldOptions.Representation> getNumericRepresentation(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return getNumericFieldDefinition(path, embeddedRoot).map(def -> def.options().representation());
  }
}
