package com.xgen.mongot.index.lucene.query.context;

import com.google.errorprone.annotations.ThreadSafe;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryTimeMappingChecks;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

/**
 * Provides query-time context for building Lucene queries against a specific index.
 *
 * <p>This class should wrap all parameters needed by query factories which are constant for either
 * the lifetime of Mongot or the {@link IndexDefinition}. Adding such parameters to this class
 * reduces the amount of refactoring needed as features evolve.
 *
 * <p>This class's lifecycle matches that of the users' {@link IndexDefinition}. An instance of this
 * class is intended to be re-used by all queries targeting the same index definition. As such, it
 * must be thread-safe, and ideally deeply immutable. It should <b>not</b> contain any state related
 * to a specific index snapshot.
 */
@ThreadSafe
public interface QueryFactoryContext {

  /** Returns the set of checks that enforce query-time mapping consistency */
  QueryTimeMappingChecks getQueryTimeMappingChecks();

  /** Indicates whether the index contains embedded field definitions */
  boolean isIndexWithEmbeddedFields();

  /** Provides the {@link Analyzer} used for string fields at query time */
  Analyzer getTokenFieldAnalyzer();

  /**
   * Resolves the vector similarity function configured for a given field.
   *
   * @param fieldPath path of the vector field
   * @param embeddedRoot optional root if the field is inside an embedded document
   * @return the {@link VectorSimilarity} function to use
   * @throws InvalidQueryException if the field mapping is invalid or unsupported
   */
  VectorSimilarity getIndexedVectorSimilarityFunction(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) throws InvalidQueryException;

  /**
   * Resolves the quantization type configured for a given vector field.
   *
   * @param fieldPath path of the vector field
   * @param embeddedRoot optional root if the field is inside an embedded document
   * @return the {@link VectorQuantization} strategy applied to the field
   * @throws InvalidQueryException if the field mapping is invalid or unsupported
   */
  VectorQuantization getIndexedQuantization(FieldPath fieldPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException;

  /** Returns the metrics updater tied to the index that this query targets. */
  IndexMetricsUpdater.QueryingMetricsUpdater getMetrics();

  /** Returns the set of start-up flags. These are constant for the lifetime of the application. */
  FeatureFlags getFeatureFlags();

  /** Returns the {@link IndexCapabilities} associated with the targeted index. */
  IndexCapabilities getIndexCapabilities();

  /**
   * Returns the {@link FieldDefinitionResolver} associated with the {@link IndexDefinition} that
   * the current query is targeting.
   */
  FieldDefinitionResolver getFieldDefinitionResolver();
}
