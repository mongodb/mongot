package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.Optional;

public class IndexMetricsUpdaterBuilder {
  private Optional<PerIndexMetricsFactory> metricsFactory = Optional.empty();
  private Optional<IndexMetricValuesSupplier> indexMetricsSupplier = Optional.empty();
  private IndexDefinition indexDefinition =
      SearchIndexDefinitionBuilder.builder().dynamicMapping().defaultMetadata().build();

  public static IndexMetricsUpdaterBuilder builder() {
    return new IndexMetricsUpdaterBuilder();
  }

  public IndexMetricsUpdaterBuilder metricsFactory(PerIndexMetricsFactory metricsFactory) {
    this.metricsFactory = Optional.of(metricsFactory);
    return this;
  }

  public IndexMetricsUpdaterBuilder indexDefinition(IndexDefinition indexDefinition) {
    this.indexDefinition = indexDefinition;
    return this;
  }

  public IndexMetricsUpdaterBuilder indexMetricsSupplier(
      IndexMetricValuesSupplier indexMetricValuesSupplier) {
    this.indexMetricsSupplier = Optional.of(indexMetricValuesSupplier);
    return this;
  }

  public IndexMetricsUpdater build() {
    Check.isPresent(this.metricsFactory, "metricsFactory");
    Check.isPresent(this.indexMetricsSupplier, "indexMetricsSupplier");
    return new IndexMetricsUpdater(
        this.indexDefinition, this.indexMetricsSupplier.get(), this.metricsFactory.get());
  }

  public static class IndexingMetricsUpdaterBuilder {
    private Optional<PerIndexMetricsFactory> metricsFactory = Optional.empty();

    public static IndexingMetricsUpdaterBuilder builder() {
      return new IndexingMetricsUpdaterBuilder();
    }

    public IndexingMetricsUpdaterBuilder metricsFactory(PerIndexMetricsFactory metricsFactory) {
      this.metricsFactory = Optional.of(metricsFactory);
      return this;
    }

    public IndexMetricsUpdater.IndexingMetricsUpdater build() {
      Check.isPresent(this.metricsFactory, "metricsFactory");
      return new IndexMetricsUpdater.IndexingMetricsUpdater(
          this.metricsFactory.get(), IndexDefinition.Type.SEARCH);
    }
  }

  public static class QueryingMetricsUpdaterBuilder {
    private Optional<PerIndexMetricsFactory> metricsFactory = Optional.empty();

    public static QueryingMetricsUpdaterBuilder builder() {
      return new QueryingMetricsUpdaterBuilder();
    }

    public QueryingMetricsUpdaterBuilder metricsFactory(PerIndexMetricsFactory metricsFactory) {
      this.metricsFactory = Optional.of(metricsFactory);
      return this;
    }

    public IndexMetricsUpdater.QueryingMetricsUpdater build() {
      Check.isPresent(this.metricsFactory, "metricsFactory");
      return new IndexMetricsUpdater.QueryingMetricsUpdater(
          this.metricsFactory.get(),
          IndexTypeData.IndexTypeTag.TAG_SEARCH,
          SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
          true,
          /* isSearchAggregationEnabled= */ true);
    }

    public static IndexMetricsUpdater.QueryingMetricsUpdater empty() {
      return new IndexMetricsUpdater.QueryingMetricsUpdater(
          new PerIndexMetricsFactory(
              IndexMetricsUpdater.QueryingMetricsUpdater.NAMESPACE,
              MeterAndFtdcRegistry.createWithSimpleRegistries(),
              GenerationIdBuilder.create()),
          IndexTypeData.IndexTypeTag.TAG_SEARCH,
          SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
          true,
          /* isSearchAggregationEnabled= */ true);
    }

    public static class QueryFeaturesMetricsUpdaterBuilder {
      private Optional<PerIndexMetricsFactory> metricsFactory = Optional.empty();

      public static QueryFeaturesMetricsUpdaterBuilder builder() {
        return new QueryFeaturesMetricsUpdaterBuilder();
      }

      public QueryFeaturesMetricsUpdaterBuilder metricsFactory(
          PerIndexMetricsFactory metricsFactory) {
        this.metricsFactory = Optional.of(metricsFactory);
        return this;
      }

      public IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater build() {
        Check.isPresent(this.metricsFactory, "metricsFactory");
        return new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
            this.metricsFactory.get(), /* aggregationEnabled= */ true);
      }

      public static IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater empty() {
        return empty(/* aggregationEnabled= */ true);
      }

      public static IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater empty(
          boolean aggregationEnabled) {
        return new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
            new PerIndexMetricsFactory(
                IndexMetricsUpdater.QueryingMetricsUpdater.NAMESPACE,
                MeterAndFtdcRegistry.createWithSimpleRegistries(),
                GenerationIdBuilder.create()),
            aggregationEnabled);
      }
    }
  }
}
