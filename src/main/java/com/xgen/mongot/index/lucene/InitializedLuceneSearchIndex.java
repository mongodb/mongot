package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.MeteredSearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.blobstore.LuceneIndexSnapshotter;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Crash;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a runnable index with the Lucene writer and reader fully initialized. */
class InitializedLuceneSearchIndex implements InitializedSearchIndex {
  private static final Logger LOG = LoggerFactory.getLogger(InitializedLuceneSearchIndex.class);

  final LuceneSearchIndex index;
  final List<Directory> directories;
  final IndexWriter writer;
  final SearchIndexReader reader;
  final Closeable refresher;
  final ImmutableList<ReferenceManager<?>> searcherManagers;
  final IndexMetricsUpdater indexMetricsUpdater;
  final GenerationId generationId;

  @VisibleForTesting
  InitializedLuceneSearchIndex(
      GenerationId generationId,
      LuceneSearchIndex index,
      List<Directory> directories,
      IndexWriter writer,
      SearchIndexReader reader,
      Closeable refresher,
      ImmutableList<ReferenceManager<?>> searcherManagers,
      IndexMetricsUpdater indexMetricsUpdater) {
    this.generationId = generationId;
    this.index = index;
    this.directories = directories;
    this.writer = writer;
    this.reader = reader;
    this.refresher = refresher;
    this.searcherManagers = searcherManagers;
    this.indexMetricsUpdater = indexMetricsUpdater;
  }

  static InitializedLuceneSearchIndex create(
      LuceneSearchIndex index,
      GenerationId generationId,
      IndexDirectoryFactory directoryFactory,
      IndexDirectoryHelper indexDirectoryHelper,
      Optional<LuceneIndexSnapshotter> luceneIndexSnapshotter,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry)
      throws IOException {
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("Initializing index");

    try {
      var ret = create(index, generationId, directoryFactory, luceneIndexSnapshotter, featureFlags,
          dynamicFeatureFlagRegistry);
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Completed initializing index");
      return ret;
    } catch (Exception e) {
      var isIndexDropped = index.getStatus().isIndexDropped();
      if (!indexDirectoryHelper.attemptToRecoverUnreadableIndex(
          generationId,
          directoryFactory.getPath(),
          e,
          isIndexDropped,
          featureFlags.isEnabled(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK))) {
        throw e;
      }
    }
    var ret = create(index, generationId, directoryFactory, luceneIndexSnapshotter, featureFlags,
        dynamicFeatureFlagRegistry);
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("Completed initializing index");
    return ret;
  }

  private static InitializedLuceneSearchIndex create(
      LuceneSearchIndex index,
      GenerationId generationId,
      IndexDirectoryFactory directoryFactory,
      Optional<LuceneIndexSnapshotter> luceneIndexSnapshotter,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry)
      throws IOException {
    var definition = index.getDefinition();
    var searchIndexProperties = index.getSearchIndexProperties();
    var indexMetricsUpdaterBuilder =
        new IndexMetricsUpdater.Builder(
            index.getDefinition(),
            searchIndexProperties.metricsFactory,
            featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));
    Analyzer indexAnalyzer =
        LuceneAnalyzer.indexAnalyzer(definition, searchIndexProperties.analyzerRegistry);
    LuceneSearcherFactory searcherFactory =
        new LuceneSearcherFactory(
            definition,
            searchIndexProperties.enableFacetingOverTokenFields,
            searchIndexProperties.queryCacheProvider,
            searchIndexProperties.tokenFacetingCardinalityLimit,
            indexMetricsUpdaterBuilder.getQueryingMetricsUpdater());
    // TODO(CLOUDP-250488): Handle subindexes for blobstore upload.

    var indexResources =
        LuceneIndexResourcesInitializer.initialize(
            definition.getNumPartitions(),
            directoryFactory,
            (directory, indexPartitionId) ->
                SingleLuceneIndexWriter.createForSearchIndex(
                    directory,
                    searchIndexProperties.mergeScheduler.createForIndexPartition(
                        generationId, indexPartitionId, definition.getNumPartitions()),
                    searchIndexProperties.mergePolicy,
                    searchIndexProperties.ramBufferSizeMb,
                    searchIndexProperties.fieldLimit,
                    searchIndexProperties.docsLimit,
                    indexAnalyzer,
                    definition.createFieldDefinitionResolver(
                        searchIndexProperties.indexFormatVersion),
                    indexMetricsUpdaterBuilder.getIndexingMetricsUpdater(),
                    luceneIndexSnapshotter.map(
                        snapshotter -> snapshotter.getSnapshotDeletionPolicy(indexPartitionId)),
                    featureFlags),
            luceneIndexWriter ->
                new LuceneSearcherManager(
                    luceneIndexWriter.getLuceneWriter(),
                    searcherFactory,
                    searchIndexProperties.metricsFactory));

    List<LuceneSearchIndexReader> searchIndexReaders =
        new ArrayList<>(indexResources.luceneSearcherManagers.size());
    for (int i = 0; i < indexResources.luceneSearcherManagers.size(); i++) {
      searchIndexReaders.add(
          LuceneSearchIndexReader.create(
              indexResources.luceneSearcherManagers.get(i),
              definition,
              searchIndexProperties.indexFormatVersion,
              searchIndexProperties.analyzerRegistry,
              searchIndexProperties.synonymRegistry,
              indexAnalyzer,
              indexMetricsUpdaterBuilder.getQueryingMetricsUpdater(),
              searchIndexProperties.concurrentSearchExecutor,
              searchIndexProperties.concurrentVectorRescoringExecutor,
              searchIndexProperties.enableTextOperatorNewSynonymsSyntax,
              i,
              featureFlags,
              dynamicFeatureFlagRegistry));
    }
    // Only create a MultiLuceneSearchIndexReader if there are at least 2 searchIndexReaders.
    // The MultiLuceneSearchIndexReader will store the unused docs (docs that are not returned to
    // mongod) with the cursor, while the single index LuceneSearchIndexReader will discard these
    // unused docs. So we prefer the latter Reader when indexPartition = 1.
    SearchIndexReader searchIndexReader =
        searchIndexReaders.size() == 1
            ? searchIndexReaders.getFirst()
            : new MultiLuceneSearchIndexReader(searchIndexReaders, dynamicFeatureFlagRegistry);

    IndexMetricValuesSupplier indexMetricValuesSupplier =
        new LuceneSearchIndexMetricValuesSupplier(
            index.getStatusRef(),
            searchIndexProperties.indexBackingStrategy,
            searchIndexReader,
            indexResources.luceneIndexWriter,
            searchIndexProperties.indexFormatVersion,
            definition,
            searchIndexProperties.metricsFactory,
            definition.getParsedIndexFeatureVersion(),
            featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR),
            dynamicFeatureFlagRegistry);

    IndexMetricsUpdater indexMetricsUpdater =
        indexMetricsUpdaterBuilder.build(indexMetricValuesSupplier);

    SearchIndexReader reader =
        new MeteredSearchIndexReader(
            searchIndexReader, indexMetricsUpdater.getQueryingMetricsUpdater());

    IndexWriter writer =
        new MeteredIndexWriter(
            indexResources.luceneIndexWriter, indexMetricsUpdater.getIndexingMetricsUpdater());

    Closeable refresher =
        searchIndexProperties.indexBackingStrategy.createIndexRefresher(
            index.getStatusRef(), ImmutableList.copyOf(indexResources.luceneSearcherManagers));
    return new InitializedLuceneSearchIndex(
        generationId,
        index,
        indexResources.directories,
        writer,
        reader,
        refresher,
        ImmutableList.copyOf(indexResources.luceneSearcherManagers),
        indexMetricsUpdater);
  }

  @Override
  public SearchIndexReader getReader() {
    this.ensureOpen("getReader");
    return this.reader;
  }

  @Override
  public IndexWriter getWriter() {
    this.ensureOpen("getWriter");
    return this.writer;
  }

  @Override
  public IndexDefinitionGeneration.Type getType() {
    return IndexDefinitionGeneration.Type.SEARCH;
  }

  @Override
  public IndexMetricsUpdater getMetricsUpdater() {
    this.ensureOpen("getMetricsUpdater");
    return this.indexMetricsUpdater;
  }

  @Override
  public GenerationId getGenerationId() {
    return this.generationId;
  }

  @Override
  public SearchIndexDefinition getDefinition() {
    return this.index.getDefinition();
  }

  @Override
  public void setStatus(IndexStatus status) {
    this.index.setStatus(status);
  }

  @Override
  public IndexStatus getStatus() {
    return this.index.getStatus();
  }

  @Override
  public IndexMetrics getMetrics() {
    return this.indexMetricsUpdater.getMetrics();
  }

  @Override
  public long getIndexSize() {
    return this.indexMetricsUpdater.getIndexSize();
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    return this.index.isCompatibleWith(indexDefinition);
  }

  @Override
  public synchronized void drop() throws IOException {
    this.index.drop();
  }

  @Override
  public synchronized void clear(EncodedUserData userData) {
    checkState(!this.index.isClosed(), "clear was called after index close");
    try {
      this.index.getSearchIndexProperties().indexBackingStrategy.clearMetadata();
    } catch (Exception e) {
      LOG.error("Failed to delete metadata for index: {}", this.generationId, e);
    }
    // close the reader (waits until all cursors are drained)
    this.reader.close();
    Crash.because("failed to delete all docs").ifThrows(() -> this.writer.deleteAll(userData));
    // re-open and refresh the reader to allow querying
    this.reader.open();
    Crash.because("failed to refresh reader").ifThrows(this.reader::refresh);
  }

  @Override
  public synchronized boolean isClosed() {
    return this.index.isClosed();
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    this.index.throwIfUnavailableForQuerying();
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.index.isClosed()) {
      return;
    }
    LOG.info("Closing initialized index: {}", this.generationId);
    Stopwatch closeIndexTime = Stopwatch.createStarted();
    this.index.close();
    this.indexMetricsUpdater.close();

    // The LuceneIndexRefresher and IndexReader both use the SearcherManager, so close them
    // before closing the SearcherManager.
    this.refresher.close();
    this.reader.close();

    // SearcherManagers should be closed (1) after both the reader and refresher and (2) before
    // the LuceneIndexWriter and Directory.
    for (ReferenceManager<?> searcherManager : this.searcherManagers) {
      searcherManager.close();
    }

    // The LuceneIndexWriter uses the Directory, so close it before closing the Directory.
    this.writer.close();
    for (Directory directory : this.directories) {
      directory.close();
    }
    LOG.info("closed index {} in {}", getDefinition().getIndexId(), closeIndexTime);
  }

  @Override
  public SynonymRegistry getSynonymRegistry() {
    return this.index.getSynonymRegistry();
  }

  private void ensureOpen(String methodName) {
    if (this.index.isClosed()) {
      String message = String.format("Cannot call %s() after to close()", methodName);
      throw new IndexClosedException(message);
    }
  }
}
