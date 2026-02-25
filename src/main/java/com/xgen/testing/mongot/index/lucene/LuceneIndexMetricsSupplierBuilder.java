package com.xgen.testing.mongot.index.lucene;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.lucene.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.LuceneIndexMetricValuesSupplier;
import com.xgen.mongot.index.lucene.LuceneIndexWriter;
import com.xgen.mongot.index.lucene.LuceneSearchIndexMetricValuesSupplier;
import com.xgen.mongot.index.lucene.LuceneVectorIndexMetricValuesSupplier;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class LuceneIndexMetricsSupplierBuilder {
  public static final String METRICS_NAMESPACE = "test";
  private Supplier<IndexStatus> indexStatusSupplier = IndexStatus::notStarted;
  private IndexBackingStrategy indexBackingStrategy = mock(IndexBackingStrategy.class);
  private Optional<IndexReader> indexReader = Optional.empty();
  private Optional<LuceneIndexWriter> indexWriter = Optional.empty();
  private IndexDefinition.Type type = IndexDefinition.Type.SEARCH;
  private boolean isEmbedded = false;
  private Optional<SearchFieldDefinitionResolver> resolver = Optional.empty();
  private final List<CustomAnalyzerDefinition> customAnalyzers = new ArrayList<>();
  private MeterRegistry meterRegistry = new SimpleMeterRegistry();
  private Duration numFieldsCacheDuration =
      LuceneSearchIndexMetricValuesSupplier.DEFAULT_NUM_FIELDS_CACHE_DURATION;
  private DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry =
      new DynamicFeatureFlagRegistry(
          Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

  public static LuceneIndexMetricsSupplierBuilder builder() {
    return new LuceneIndexMetricsSupplierBuilder();
  }

  public LuceneIndexMetricsSupplierBuilder type(IndexDefinition.Type type) {
    this.type = type;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder indexBackingStrategy(
      IndexBackingStrategy indexBackingStrategy) {
    this.indexBackingStrategy = indexBackingStrategy;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder indexStatusSupplier(
      Supplier<IndexStatus> indexStatusSupplier) {
    this.indexStatusSupplier = indexStatusSupplier;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder indexReader(IndexReader indexReader) {
    this.indexReader = Optional.of(indexReader);
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder indexWriter(LuceneIndexWriter indexWriter) {
    this.indexWriter = Optional.of(indexWriter);
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder isEmbedded(boolean isEmbedded) {
    this.isEmbedded = isEmbedded;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder resolver(SearchFieldDefinitionResolver resolver) {
    this.resolver = Optional.of(resolver);
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder customAnalyzerDefinition(
      CustomAnalyzerDefinition customAnalyzerDefinition) {
    this.customAnalyzers.add(customAnalyzerDefinition);
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder customAnalyzerDefinitions(
      List<CustomAnalyzerDefinition> customAnalyzerDefinitions) {
    this.customAnalyzers.addAll(customAnalyzerDefinitions);
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder meterRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder numFieldsCacheDuration(Duration cacheDuration) {
    this.numFieldsCacheDuration = cacheDuration;
    return this;
  }

  public LuceneIndexMetricsSupplierBuilder dynamicFeatureFlagRegistry(
      DynamicFeatureFlagRegistry registry) {
    this.dynamicFeatureFlagRegistry = registry;
    return this;
  }

  /** Creates a LuceneIndexMetricValuesSupplier. */
  public LuceneIndexMetricValuesSupplier build() {
    Check.isPresent(this.indexWriter, "indexWriter");
    return switch (this.type) {
      case SEARCH -> {
        Check.isPresent(this.indexReader, "indexReader");
        SearchIndexReader searchIndexReader =
            Check.instanceOf(this.indexReader.get(), SearchIndexReader.class);
        yield LuceneSearchIndexMetricValuesSupplier.create(
            this.indexStatusSupplier,
            this.indexBackingStrategy,
            searchIndexReader,
            this.indexWriter.get(),
            this.isEmbedded,
            this.resolver.get(),
            this.customAnalyzers,
            new PerIndexMetricsFactory(
                METRICS_NAMESPACE,
                MeterAndFtdcRegistry.createWithMeterRegistry(this.meterRegistry),
                GenerationIdBuilder.create()),
            SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
            true,
            this.numFieldsCacheDuration,
            this.dynamicFeatureFlagRegistry);
      }
      case VECTOR_SEARCH -> {
        Check.checkState(
            !this.isEmbedded,
            "LuceneVectorIndexMetricValuesSupplier doesn't support embedded documents.");
        Check.isPresent(this.indexReader, "indexReader");
        VectorIndexReader vectorIndexReader =
            Check.instanceOf(this.indexReader.get(), VectorIndexReader.class);
        Check.checkState(
            this.resolver.isEmpty(),
            "LuceneVectorIndexMetricValuesSupplier doesn't support SearchFieldDefinitionResolver.");
        Check.checkState(
            this.customAnalyzers.isEmpty(),
            "LuceneVectorIndexMetricValuesSupplier doesn't support custom analyzer definitions.");
        yield LuceneVectorIndexMetricValuesSupplier.create(
            this.indexStatusSupplier,
            this.indexBackingStrategy,
            vectorIndexReader,
            this.indexWriter.get(),
            new PerIndexMetricsFactory(
                METRICS_NAMESPACE,
                MeterAndFtdcRegistry.createWithMeterRegistry(this.meterRegistry),
                GenerationIdBuilder.create()),
            VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
            true);
      }
    };
  }
}
