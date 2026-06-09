package com.xgen.mongot.index.lucene.searcher;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.similarity.LuceneSimilarity;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;

public class LuceneSearcherFactory {

  protected final IndexDefinition indexDefinition;
  protected final QueryCacheProvider queryCacheProvider;
  protected final boolean enableFacetingOverTokenFields;
  protected final Optional<Integer> tokenFacetingCardinalityLimit;
  private final IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;

  public LuceneSearcherFactory(
      IndexDefinition indexDefinition,
      boolean enableFacetingOverTokenFields,
      QueryCacheProvider queryCacheProvider,
      Optional<Integer> tokenFacetingCardinalityLimit,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater) {
    this.indexDefinition = indexDefinition;
    this.queryCacheProvider = queryCacheProvider;
    this.enableFacetingOverTokenFields = enableFacetingOverTokenFields;
    this.tokenFacetingCardinalityLimit = tokenFacetingCardinalityLimit;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public LuceneIndexSearcher newSearcher(
      IndexReader newReader, Optional<LuceneIndexSearcher> previousSearcher) throws IOException {

    return switch (this.indexDefinition) {
      case SearchIndexDefinition def ->
          LuceneIndexSearcher.create(
              newReader,
              this.queryCacheProvider,
              previousSearcher,
              Optional.of(LuceneSimilarity.from(def)),
              def.isStringFacetsFieldIndexed(),
              this.enableFacetingOverTokenFields,
              this.tokenFacetingCardinalityLimit,
              this.queryingMetricsUpdater);
      case VectorIndexDefinition v ->
          LuceneIndexSearcher.create(
              newReader,
              this.queryCacheProvider,
              previousSearcher,
              Optional.empty(),
              false,
              false,
              Optional.empty(),
              this.queryingMetricsUpdater);
    };
  }

  @VisibleForTesting
  public IndexDefinition getIndexDefinition() {
    return this.indexDefinition;
  }
}
