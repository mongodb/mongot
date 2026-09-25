package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitialSyncIndexUnavailableException;
import com.xgen.mongot.index.VectorIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.lucene.index.MergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneVectorIndex implements VectorIndex {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneVectorIndex.class);

  private final VectorIndexDefinition definition;
  private final AtomicReference<IndexStatus> statusRef;
  private final VectorIndexProperties vectorIndexProperties;
  private volatile boolean closed;

  /** The properties used by the vector index at runtime. */
  static class VectorIndexProperties {
    IndexBackingStrategy indexBackingStrategy;
    InstrumentedConcurrentMergeScheduler mergeScheduler;
    MergePolicy mergePolicy;
    QueryCacheProvider queryCacheProvider;
    double ramBufferSizeMb;
    Optional<Integer> fieldLimit;
    Optional<Integer> docsLimit;
    IndexFormatVersion indexFormatVersion;
    PerIndexMetricsFactory metricsFactory;
    Optional<NamedExecutorService> concurrentSearchExecutor;
    Optional<NamedExecutorService> concurrentVectorRescoringExecutor;

    @VisibleForTesting
    VectorIndexProperties(
        IndexBackingStrategy indexBackingStrategy,
        InstrumentedConcurrentMergeScheduler mergeScheduler,
        MergePolicy mergePolicy,
        QueryCacheProvider queryCacheProvider,
        double ramBufferSizeMb,
        Optional<Integer> fieldLimit,
        Optional<Integer> docsLimit,
        IndexFormatVersion indexFormatVersion,
        PerIndexMetricsFactory metricsFactory,
        Optional<NamedExecutorService> concurrentSearchExecutor,
        Optional<NamedExecutorService> concurrentVectorRescoringExecutor) {
      this.indexBackingStrategy = indexBackingStrategy;
      this.mergeScheduler = mergeScheduler;
      this.mergePolicy = mergePolicy;
      this.queryCacheProvider = queryCacheProvider;
      this.ramBufferSizeMb = ramBufferSizeMb;
      this.fieldLimit = fieldLimit;
      this.docsLimit = docsLimit;
      this.indexFormatVersion = indexFormatVersion;
      this.metricsFactory = metricsFactory;
      this.concurrentSearchExecutor = concurrentSearchExecutor;
      this.concurrentVectorRescoringExecutor = concurrentVectorRescoringExecutor;
    }
  }

  LuceneVectorIndex(
      VectorIndexDefinition definition,
      AtomicReference<IndexStatus> statusRef,
      VectorIndexProperties vectorIndexProperties) {
    this.definition = definition;
    this.statusRef = statusRef;
    this.vectorIndexProperties = vectorIndexProperties;
    this.closed = false;
  }

  /** Creates a VectorIndex that is backed by the disk (as opposed to memory). */
  static LuceneVectorIndex createDiskBacked(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      QueryCacheProvider queryCacheProvider,
      ScheduledExecutorService refreshExecutor,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      VectorIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AtomicDirectoryRemover directoryRemover,
      PerIndexMetricsFactory metricsFactory)
      throws IOException {
    return create(
        mergeScheduler,
        mergePolicy,
        queryCacheProvider,
        config.ramBufferSizeMb(),
        config.fieldLimit(),
        config.docsLimit(),
        indexDefinition,
        indexFormatVersion,
        metricsFactory,
        IndexBackingStrategyFactory.diskBacked(
            refreshExecutor,
            config.refreshInterval(),
            directoryRemover,
            indexPath,
            metadataPath,
            metricsFactory,
            indexDefinition.getNumPartitions()),
        concurrentSearchExecutor,
        concurrentVectorRescoringExecutor,
        IndexStatus.unknown());
  }

  private static LuceneVectorIndex create(
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      QueryCacheProvider queryCacheProvider,
      double ramBufferSizeMb,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      VectorIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      PerIndexMetricsFactory metricsFactory,
      IndexBackingStrategy indexBackingStrategy,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      IndexStatus initialIndexStatus) {
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(initialIndexStatus);

    return new LuceneVectorIndex(
        indexDefinition,
        statusRef,
        new VectorIndexProperties(
            indexBackingStrategy,
            mergeScheduler,
            mergePolicy,
            queryCacheProvider,
            ramBufferSizeMb,
            fieldLimit,
            docsLimit,
            indexFormatVersion,
            metricsFactory,
            concurrentSearchExecutor,
            concurrentVectorRescoringExecutor));
  }

  @Override
  public VectorIndexDefinition getDefinition() {
    return this.definition;
  }

  @Override
  public void setStatus(IndexStatus status) {
    this.statusRef.set(status);
  }

  @Override
  public IndexStatus getStatus() {
    return this.statusRef.get();
  }

  @Override
  public synchronized void drop() throws IOException {
    LOG.atInfo().addKeyValue("indexId", this.definition.getIndexId()).log("Dropping index");
    checkState(this.closed, "Index must be closed prior to calling drop()");
    this.vectorIndexProperties.indexBackingStrategy.releaseResources();
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }
    LOG.atInfo().addKeyValue("indexId", this.definition.getIndexId()).log("Closing index");
    // Immediately flip the flag so other calls see that the Index is closed.
    this.closed = true;
  }

  @Override
  public synchronized boolean isClosed() {
    return this.closed;
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    return indexDefinition.getType() == IndexDefinition.Type.VECTOR_SEARCH;
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    var status = getStatus().getStatusCode();
    switch (status) {
      case INITIAL_SYNC ->
          throw new InitialSyncIndexUnavailableException(
              String.format(
                  "cannot query vector index %s while in state %s", this.definition, status));
      case UNKNOWN, NOT_STARTED, FAILED ->
          throw new IndexUnavailableException(
              String.format(
                  "cannot query vector index %s while in state %s", this.definition, status));
      case STEADY, STALE, RECOVERING_TRANSIENT, RECOVERING_NON_TRANSIENT, DOES_NOT_EXIST -> {}
    }
  }

  Supplier<IndexStatus> getStatusRef() {
    return this.statusRef::get;
  }

  VectorIndexProperties getVectorIndexProperties() {
    return this.vectorIndexProperties;
  }
}
