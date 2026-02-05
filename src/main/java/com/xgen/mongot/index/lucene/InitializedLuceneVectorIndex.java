package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.MeteredVectorIndexReader;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.blobstore.LuceneIndexSnapshotter;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.query.LuceneVectorQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Crash;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a runnable index with the Lucene writer and reader fully initialized. */
class InitializedLuceneVectorIndex implements InitializedVectorIndex {
  private static final Logger LOG = LoggerFactory.getLogger(InitializedLuceneVectorIndex.class);

  final GenerationId generationId;
  final LuceneVectorIndex index;

  final List<Directory> directories;
  final IndexWriter writer;
  final VectorIndexReader reader;
  final Closeable refresher;

  final ImmutableList<ReferenceManager<?>> searcherManagers;
  final IndexMetricsUpdater indexMetricsUpdater;

  InitializedLuceneVectorIndex(
      GenerationId generationId,
      LuceneVectorIndex index,
      List<Directory> directories,
      IndexWriter writer,
      VectorIndexReader reader,
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

  static InitializedLuceneVectorIndex create(
      LuceneVectorIndex index,
      GenerationId generationId,
      IndexDirectoryFactory directoryFactory,
      IndexDirectoryHelper indexDirectoryHelper,
      Optional<LuceneIndexSnapshotter> luceneIndexSnapshotter,
      FeatureFlags featureFlags)
      throws IOException {
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("Initializing index");
    try {
      var ret = create(index, generationId, directoryFactory, luceneIndexSnapshotter, featureFlags);
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Completed initializing index");
      return ret;
    } catch (Exception e) {
      boolean isIndexDropped = index.getStatus().isIndexDropped();
      if (!indexDirectoryHelper.attemptToRecoverUnreadableIndex(
          generationId,
          directoryFactory.getPath(),
          e,
          isIndexDropped,
          featureFlags.isEnabled(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK))) {
        throw e;
      }
    }
    var ret = create(index, generationId, directoryFactory, luceneIndexSnapshotter, featureFlags);
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("Completed initializing index");
    return ret;
  }

  private static InitializedLuceneVectorIndex create(
      LuceneVectorIndex index,
      GenerationId generationId,
      IndexDirectoryFactory directoryFactory,
      Optional<LuceneIndexSnapshotter> luceneIndexSnapshotter,
      FeatureFlags featureFlags)
      throws IOException {
    var vectorIndexProperties = index.getVectorIndexProperties();
    var definition = index.getDefinition();
    var indexMetricsUpdaterBuilder =
        new IndexMetricsUpdater.Builder(
            definition,
            vectorIndexProperties.metricsFactory,
            featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));
    LuceneSearcherFactory searcherFactory =
        new LuceneSearcherFactory(
            definition,
            false,
            vectorIndexProperties.queryCacheProvider,
            Optional.empty(),
            indexMetricsUpdaterBuilder.getQueryingMetricsUpdater());
    // TODO(CLOUDP-250488): Handle subindexes for blobstore upload.

    var indexResources =
        LuceneIndexResourcesInitializer.initialize(
            definition.getNumPartitions(),
            directoryFactory,
            (directory, indexPartitionId) ->
                SingleLuceneIndexWriter.createForVectorIndex(
                    directory,
                    vectorIndexProperties.mergeScheduler.createForIndexPartition(
                        generationId, indexPartitionId, definition.getNumPartitions()),
                    vectorIndexProperties.mergePolicy,
                    vectorIndexProperties.ramBufferSizeMb,
                    vectorIndexProperties.fieldLimit,
                    vectorIndexProperties.docsLimit,
                    definition,
                    definition.getIndexCapabilities(vectorIndexProperties.indexFormatVersion),
                    indexMetricsUpdaterBuilder.getIndexingMetricsUpdater(),
                    luceneIndexSnapshotter.map(
                        snapshotter -> snapshotter.getSnapshotDeletionPolicy(indexPartitionId))),
            luceneIndexWriter ->
                new LuceneSearcherManager(
                    luceneIndexWriter.getLuceneWriter(),
                    searcherFactory,
                    vectorIndexProperties.metricsFactory));

    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        indexMetricsUpdaterBuilder.getQueryingMetricsUpdater();
    VectorFieldDefinitionResolver definitionResolver =
        new VectorFieldDefinitionResolver(definition, vectorIndexProperties.indexFormatVersion);
    VectorQueryFactoryContext factoryContext =
        new VectorQueryFactoryContext(definitionResolver, featureFlags, metricsUpdater);
    var luceneVectorQueryFactoryDistributor =
        LuceneVectorQueryFactoryDistributor.create(factoryContext);

    List<LuceneVectorIndexReader> vectorIndexReaders =
        indexResources.luceneSearcherManagers.stream()
            .map(
                searcherManager ->
                    LuceneVectorIndexReader.create(
                        searcherManager,
                        factoryContext,
                        definition,
                        luceneVectorQueryFactoryDistributor,
                        vectorIndexProperties.concurrentSearchExecutor,
                        vectorIndexProperties.concurrentVectorRescoringExecutor))
            .toList();
    // Only create a MultiLuceneVectorIndexReader if there are at least 2 vectorIndexReaders.
    // Otherwise, we could avoid the extra layer by simply using a LuceneVectorIndexReader.
    VectorIndexReader vectorIndexReader =
        vectorIndexReaders.size() == 1
            ? vectorIndexReaders.getFirst()
            : new MultiLuceneVectorIndexReader(vectorIndexReaders, metricsUpdater);

    IndexMetricValuesSupplier indexMetricValuesSupplier =
        new LuceneVectorIndexMetricValuesSupplier(
            index.getStatusRef(),
            vectorIndexProperties.indexBackingStrategy,
            vectorIndexReader,
            indexResources.luceneIndexWriter,
            vectorIndexProperties.metricsFactory,
            definition.getParsedIndexFeatureVersion(),
            featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));

    IndexMetricsUpdater indexMetricsUpdater =
        indexMetricsUpdaterBuilder.build(indexMetricValuesSupplier);

    VectorIndexReader reader =
        new MeteredVectorIndexReader(
            vectorIndexReader, indexMetricsUpdater.getQueryingMetricsUpdater());

    IndexWriter writer =
        new MeteredIndexWriter(
            indexResources.luceneIndexWriter, indexMetricsUpdater.getIndexingMetricsUpdater());

    Closeable refresher =
        vectorIndexProperties.indexBackingStrategy.createIndexRefresher(
            index.getStatusRef(), ImmutableList.copyOf(indexResources.luceneSearcherManagers));
    return new InitializedLuceneVectorIndex(
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
  public VectorIndexReader getReader() {
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
    return IndexDefinitionGeneration.Type.VECTOR;
  }

  @Override
  public IndexMetricsUpdater getMetricsUpdater() {
    return this.indexMetricsUpdater;
  }

  @Override
  public GenerationId getGenerationId() {
    return this.generationId;
  }

  @Override
  public VectorIndexDefinition getDefinition() {
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
  public synchronized void clear(EncodedUserData dropUserData) {
    checkState(!this.index.isClosed(), "clear was called after index close");
    try {
      this.index.getVectorIndexProperties().indexBackingStrategy.clearMetadata();
    } catch (Exception e) {
      LOG.atError()
          .addKeyValue("indexId", this.generationId.indexId)
          .addKeyValue("generationId", this.generationId)
          .setCause(e)
          .log("Failed to delete metadata for index");
    }
    // close the reader (waits for in-flight queries to complete)
    this.reader.close();
    Crash.because("failed to delete all docs").ifThrows(() -> this.writer.deleteAll(dropUserData));
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
    LOG.atInfo()
        .addKeyValue("indexId", this.generationId.indexId)
        .addKeyValue("generationId", this.generationId)
        .log("Closing initialized index");
    var closeIndexTime = Stopwatch.createStarted();
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
    LOG.atInfo()
        .addKeyValue("indexId", getDefinition().getIndexId())
        .addKeyValue("closeIndexTime", closeIndexTime)
        .log("closed vector index");
  }

  private void ensureOpen(String methodName) {
    if (this.index.isClosed()) {
      String message = String.format("Cannot call %s() after to close()", methodName);
      throw new IndexClosedException(message);
    }
  }
}
