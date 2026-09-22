package com.xgen.mongot.index.lucene.query.context;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorQueryTimeMappingChecks;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

public class VectorQueryFactoryContext implements QueryFactoryContext {

  private final VectorQueryTimeMappingChecks checks;

  private final FeatureFlags featureFlags;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metrics;
  private final VectorFieldDefinitionResolver resolver;

  public VectorQueryFactoryContext(
      VectorIndexDefinition indexDefinition,
      IndexFormatVersion ifv,
      FeatureFlags featureFlags,
      IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
    this(indexDefinition.createFieldDefinitionResolver(ifv), featureFlags, metrics);
  }

  public VectorQueryFactoryContext(
      VectorFieldDefinitionResolver resolver,
      FeatureFlags featureFlags,
      IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
    this.resolver = resolver;
    this.checks = new VectorQueryTimeMappingChecks(resolver);
    this.featureFlags = featureFlags;
    this.metrics = metrics;
  }

  @Override
  public VectorQueryTimeMappingChecks getQueryTimeMappingChecks() {
    return this.checks;
  }

  @Override
  public boolean isIndexWithEmbeddedFields() {
    return this.resolver.getNestedRoot().isPresent();
  }

  @Override
  public Analyzer getTokenFieldAnalyzer() {
    return new KeywordAnalyzer();
  }

  @Override
  public VectorSimilarity getIndexedVectorSimilarityFunction(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) throws InvalidQueryException {
    return resolveVectorFieldSpecification(fieldPath).similarity();
  }

  @Override
  public VectorQuantization getIndexedQuantization(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) throws InvalidQueryException {
    return resolveVectorFieldSpecification(fieldPath).quantization();
  }

  @Override
  public IndexMetricsUpdater.QueryingMetricsUpdater getMetrics() {
    return this.metrics;
  }

  @Override
  public FeatureFlags getFeatureFlags() {
    return this.featureFlags;
  }

  @Override
  public IndexCapabilities getIndexCapabilities() {
    return this.resolver.getIndexCapabilities();
  }

  @Override
  public Optional<Sort> getIndexSort() {
    // Vector indexes never support index sort.
    return Optional.empty();
  }

  @Override
  public VectorFieldDefinitionResolver getFieldDefinitionResolver() {
    return this.resolver;
  }

  /**
   * Checks if the given field path is an auto-embedding field (TEXT or AUTO_EMBED).
   *
   * @param fieldPath the field path to check
   * @return true if the field is an auto-embedding field, false otherwise
   */
  public boolean isAutoEmbedField(FieldPath fieldPath) {
    return this.resolver.isIndexed(fieldPath, VectorIndexFieldDefinition.Type.TEXT)
        || this.resolver.isIndexed(fieldPath, VectorIndexFieldDefinition.Type.AUTO_EMBED);
  }

  @Override
  public Optional<VectorAutoEmbedQuantization> getAutoEmbedQuantization(FieldPath fieldPath)
      throws InvalidQueryException {
    return resolveVectorFieldSpecification(fieldPath)
            instanceof VectorAutoEmbedFieldSpecification autoEmbedSpecification
        ? Optional.of(autoEmbedSpecification.autoEmbedQuantization())
        : Optional.empty();
  }

  private VectorFieldSpecification resolveVectorFieldSpecification(FieldPath fieldPath)
      throws InvalidQueryException {
    return this.resolver
        .getVectorFieldSpecification(fieldPath)
        .orElseThrow(
            () ->
                new InvalidQueryException(
                    String.format("%s is not indexed as a vector field", fieldPath)));
  }

  /**
   * Gets the vector index field definition for a given path.
   *
   * @param path the field path
   * @return Optional containing the field definition, or empty if not found
   */
  public Optional<VectorIndexFieldDefinition> getVectorIndexFieldDefinition(FieldPath path) {
    return this.resolver.getFields().stream()
        .filter(definition -> definition.getPath().equals(path))
        .findFirst();
  }

  /**
   * Returns the nested root path for embedded vector documents, if specified at the index level.
   *
   * @return Optional containing the nested root path, or empty if not an embedded vector index
   */
  public Optional<FieldPath> getNestedRoot() {
    return this.resolver.getNestedRoot();
  }
}
