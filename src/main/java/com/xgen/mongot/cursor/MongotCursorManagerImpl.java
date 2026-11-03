package com.xgen.mongot.cursor;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongotCursorManagerImpl implements MongotCursorManager {

  private static final Logger LOG = LoggerFactory.getLogger(MongotCursorManagerImpl.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
  private final Lock sharedLock = this.reentrantReadWriteLock.readLock();
  private final Lock exclusiveLock = this.reentrantReadWriteLock.writeLock();

  /** NOTE: Insertions to this hash map must be done atomically using computeIfAbsent. */
  private final ConcurrentHashMap<GenerationId, IndexCursorManager> indexManagers;

  private final ConcurrentHashMap<Long, IndexCursorManager> cursorToIndex;

  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final CursorFactory cursorFactory;

  /** Used to produce empty cursors when querying indexes that do not exist in the catalog. */
  private final NullIndexCursorManager nullIndexManager;

  private final NamedExecutorService idleCursorKiller;
  private final MetricsFactory metrics;

  private boolean closed;
  private static final long REPORT_LONG_LIVED_CURSOR_RATE_S = 300;
  private static final Duration LONG_LIVED_CURSOR_DURATION = Duration.ofHours(2);
  private static final int REPORTED_LONG_LIVED_CURSORS_SIZE = 5;

  /** Used for testing only, use fromConfig instead. */
  MongotCursorManagerImpl(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      NamedScheduledExecutorService idleCursorKiller,
      MetricsFactory metrics,
      CursorIdSupplier cursorIdSupplier) {
    this.indexManagers = new ConcurrentHashMap<>();
    this.cursorToIndex = new ConcurrentHashMap<>();

    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.cursorFactory = new CursorFactory(cursorIdSupplier);
    this.nullIndexManager = new NullIndexCursorManager(this.cursorFactory);

    this.idleCursorKiller = idleCursorKiller;
    this.metrics = metrics;
    metrics.objectValueGauge("trackedCursors", this.cursorToIndex, ConcurrentHashMap::size);
    this.closed = false;
  }

  /**
   * Constructs a new MongotCursorManagerImpl.
   *
   * <p>MongotCursorManagerImpl does not take ownership of the IndexCatalog, and cannot modify it.
   */
  public static MongotCursorManagerImpl fromConfig(
      CursorConfig config,
      MeterRegistry meterRegistry,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog) {
    MetricsFactory metrics = new MetricsFactory("cursorManager", meterRegistry);
    NamedScheduledExecutorService executor =
        Executors.singleThreadScheduledExecutor("idle-cursor-killer", meterRegistry);

    var cursorIdSupplier = CursorIdSupplier.fromRange(config.cursorIdRange);

    MongotCursorManagerImpl manager =
        new MongotCursorManagerImpl(
            indexCatalog, initializedIndexCatalog, executor, metrics, cursorIdSupplier);

    executor.scheduleAtFixedRate(
        () ->
            Crash.because("failed to kill idle cursors")
                .ifThrows(() -> manager.killIdleCursors(config.cursorIdleTime)),
        config.idleCursorHandlingRate.toMillis(),
        config.idleCursorHandlingRate.toMillis(),
        TimeUnit.MILLISECONDS);

    executor.scheduleAtFixedRate(
        manager::reportLongLivedCursors,
        REPORT_LONG_LIVED_CURSOR_RATE_S,
        REPORT_LONG_LIVED_CURSOR_RATE_S,
        TimeUnit.SECONDS);

    return manager;
  }

  @Override
  public SearchCursorInfo newCursor(
      String databaseName,
      String collectionName,
      UUID collectionUuid,
      Optional<String> viewName,
      CursorQuery cursorQuery,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<SearchEnvoyMetadata> searchEnvoyMetadata)
      throws IOException,
          InvalidQueryException,
          IndexUnavailableException,
          InterruptedException,
          ReaderClosedException {
    // Lock the MongotCursorManagerImpl from dropping index cursors while we construct ours.
    // This relies on higher level owners of the MongotCursorManagerImpl removing indexes from
    // the shared IndexCatalog prior to calling MongotCursorManager::killIndexCursors.
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("newCursor");

      // Validate the input and return the proper cursor (either an actual cursor over the index, or
      // an empty cursor if the index doesn't exist), or throw a validation exception if
      // appropriate.
      IndexCursorManager indexManager =
          provideIndexManager(
              databaseName,
              collectionName,
              collectionUuid,
              viewName,
              cursorQuery.getQuery(),
              searchEnvoyMetadata);

      // if another thread is in killIndexCursors right now, the index should not be available, and
      // so we would throw or use an empty cursor.
      SearchCursorInfo cursorInfo =
          indexManager.createCursor(
              NamespaceBuilder.build(databaseName, collectionName, viewName),
              cursorQuery,
              queryCursorOptions,
              queryOptimizationFlags);

      this.cursorToIndex.put(cursorInfo.cursorId, indexManager);

      return cursorInfo;
    }
  }

  @Override
  public IntermediateSearchCursorInfo newIntermediateCursors(
      String databaseName,
      String collectionName,
      UUID collectionUuid,
      Optional<String> viewName,
      SearchQuery query,
      int intermediateVersion,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<SearchEnvoyMetadata> searchEnvoyMetadata)
      throws IOException, InvalidQueryException, IndexUnavailableException, InterruptedException {
    // Lock the MongotCursorManagerImpl from dropping index cursors while we construct ours.
    // This relies on higher level owners of the MongotCursorManagerImpl removing indexes from
    // the shared IndexCatalog prior to calling MongotCursorManager::killIndexCursors.
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("newCursor");

      // Validate the input and return the proper cursors (either actual cursors over the index, or
      // empty cursor if the index doesn't exist), or throw a validation exception if
      // appropriate.
      IndexCursorManager indexManager =
          provideIndexManager(
              databaseName, collectionName, collectionUuid, viewName, query, searchEnvoyMetadata);

      // if another thread is in killIndexCursors right now, the index should not be available, and
      // so we would throw or use an empty cursor.
      IntermediateSearchCursorInfo cursorInfo =
          indexManager.createIntermediateCursors(
              NamespaceBuilder.build(databaseName, collectionName, viewName),
              query,
              intermediateVersion,
              queryCursorOptions,
              queryOptimizationFlags);

      this.cursorToIndex.put(cursorInfo.searchCursorId, indexManager);
      this.cursorToIndex.put(cursorInfo.metaCursorId, indexManager);

      return cursorInfo;
    }
  }

  @Override
  public MongotCursorResultInfo getNextBatch(
      long cursorId, Bytes resultsSizeLimit, BatchCursorOptions queryCursorOptions)
      throws MongotCursorNotFoundException, IOException {
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("getNextBatch");

      IndexCursorManager indexManager =
          getIndexManager(cursorId).orElseThrow(() -> new MongotCursorNotFoundException(cursorId));
      // It's possible that after retrieving the cursor, but before getting the next batch,
      // doKillCursor was called on the cursor. If this happens, cursor::getNextBatch will throw a
      // MongotCursorClosedException. From the point of view of the cursor manager, this is
      // equivalent to the cursor no longer existing, so catch it and rethrow it as a
      // MongotCursorNotFoundException.
      // Note that this relies on the fact that MongotCursor::getNextBatch and
      // MongotCursor::close are both synchronized.
      MongotCursorResultInfo batch =
          indexManager.getNextBatch(cursorId, resultsSizeLimit, queryCursorOptions);

      checkCursorExhausted(batch, cursorId);

      return batch;
    }
  }

  @Override
  public QueryBatchTimerRecorder getIndexQueryBatchTimerRecorder(long cursorId)
      throws MongotCursorNotFoundException {
    try (LockGuard ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("updateIndexQueryGetMoreStatistics");

      IndexCursorManager indexManager =
          getIndexManager(cursorId).orElseThrow(() -> new MongotCursorNotFoundException(cursorId));

      return indexManager.getQueryBatchTimerRecorder();
    }
  }

  @Override
  public Optional<ExplainQueryState> getExplainQueryState(long cursorId)
      throws MongotCursorNotFoundException {
    try (LockGuard ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("getExplainQueryState");
      IndexCursorManager indexManager =
          getIndexManager(cursorId).orElseThrow(() -> new MongotCursorNotFoundException(cursorId));

      return indexManager.getExplainQueryState(cursorId);
    }
  }

  /**
   * If any cursors are open, or an explain or search batch is being gathered. In order to ensure
   * that no new cursors are opened after calling this method, the index should be in an unavailable
   * state or removed from the index catalog.
   */
  @Override
  public boolean hasOpenCursors(GenerationId indexGenerationId) {
    return this.getIndexManager(indexGenerationId)
        .map(IndexCursorManager::hasOpenCursors)
        .orElse(Boolean.FALSE);
  }

  @Override
  public void killCursor(long cursorId) {
    try (var ignored = LockGuard.with(this.sharedLock)) {
      IndexCursorManager indexManager =
          this.getIndexManager(cursorId).orElse(this.nullIndexManager);

      indexManager.killCursor(cursorId);
      this.cursorToIndex.remove(cursorId);
    }
  }

  private void killIdleCursors(Duration cursorIdleTime) {
    Instant now = Instant.now();
    Instant idleSince = now.minus(cursorIdleTime);

    killIdleCursorsSince(idleSince);
  }

  void killIdleCursorsSince(Instant idleSince) {
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("killIdleCursors");
      // invoke on all managers, we rely that managers will perform proper synchronization.
      for (IndexCursorManager manager : this.indexManagers.values()) {
        List<Long> killed = manager.killIdleCursorsSince(idleSince);
        killed.forEach(this.cursorToIndex::remove);
      }
    }
  }

  private void reportLongLivedCursors() {
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("reportLongLivedCursors");
      for (IndexCursorManager manager : this.indexManagers.values()) {
        manager.reportLongLivedCursors(
            LONG_LIVED_CURSOR_DURATION, REPORTED_LONG_LIVED_CURSORS_SIZE);
      }
    }
  }

  /**
   * In order to ensure that no new cursors are opened after this method releases the lock, the
   * index should be in an unavailable state or removed from the index catalog prior to this method.
   */
  @Override
  public void killIndexCursors(GenerationId indexGenerationId) {
    try (var ignored = LockGuard.with(this.sharedLock)) {
      ensureOpen("killIndexCursors");
      Optional<IndexCursorManager> optionalManager = getIndexManager(indexGenerationId);

      if (optionalManager.isEmpty()) {
        return;
      }

      IndexCursorManager indexManager = optionalManager.get();
      this.indexManagers.remove(indexGenerationId);
      // we do not prevent threads from creating cursors on this indexManager, we rely on
      // indexManager.killAll() to be properly synchronized.
      Collection<Long> killedIds = indexManager.killAll();
      killedIds.forEach(this.cursorToIndex::remove);
    }
  }

  @Override
  public void close() {
    LOG.info("Shutting down.");
    // we first wait for idleCursorKiller to finish as it requires the manager to be open.
    Executors.shutdownOrFail(this.idleCursorKiller);

    try (var ignored = LockGuard.with(this.exclusiveLock)) {
      for (IndexCursorManager manager : this.indexManagers.values()) {
        manager.killAll();
      }

      this.cursorToIndex.clear();
      this.metrics.close();

      this.closed = true;
    }
  }

  private void ensureOpen(String methodName) {
    checkState(
        !this.closed, "cannot call %s while %s is closed", methodName, this.getClass().getName());
  }

  private Optional<IndexCursorManager> getIndexManager(long cursorId) {
    return Optional.ofNullable(this.cursorToIndex.get(cursorId));
  }

  private Optional<IndexCursorManager> getIndexManager(GenerationId id) {
    return Optional.ofNullable(this.indexManagers.get(id));
  }

  private IndexCursorManager provideIndexManager(
      String databaseName,
      String collectionName,
      UUID collectionUuid,
      Optional<String> viewName,
      Query query,
      Optional<SearchEnvoyMetadata> searchEnvoyMetadata)
      throws InvalidQueryException {
    if (searchEnvoyMetadata.isPresent() && searchEnvoyMetadata.get().getRoutedFromAnotherShard()) {
      LOG.atTrace()
          .addKeyValue("searchEnvoyMetadata", searchEnvoyMetadata.get())
          .log("Use NullIndexManager because of SearchEnvoyMetadata");
      return this.nullIndexManager;
    }

    // first find the proper index to service the query
    Optional<InitializedIndex> optionalInitializedIndex =
        this.indexCatalog
            .getIndex(databaseName, collectionUuid, viewName, query.index())
            .flatMap(
                indexGeneration -> {
                  var initializedIndex =
                      this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId());
                  if (initializedIndex.isEmpty()) {
                    this.metrics
                        .counter(
                            "uninitializedIndexCursorRequests",
                            Tags.of(
                                "indexStatus",
                                indexGeneration.getIndex().getStatus().getStatusCode().name()))
                        .increment();
                  }
                  return initializedIndex;
                });

    if (optionalInitializedIndex.isEmpty()) {
      // the index does not exist, use empty cursors then. The result is not cached, so when this
      // index does get created, we will answer queries correctly.
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
          "No index in catalog: database=%s, collection=%s", databaseName, collectionName);
      return this.nullIndexManager;
    }

    InitializedIndex index = optionalInitializedIndex.get();
    // this method can be called by multiple threads, so we rely on the concurrent hash map to
    // instantiate one index manager atomically.
    GenerationId id = index.getGenerationId();
    return this.indexManagers.computeIfAbsent(
        id, ignored -> new IndexCursorManagerImpl(index, this.cursorFactory));
  }

  private void checkCursorExhausted(MongotCursorResultInfo batch, long cursorId) {
    if (batch.exhausted) {
      this.cursorToIndex.remove(cursorId);
      // We do not evacuate empty IndexCursorManagers. However, we do remove them in
      // killIndexCursors so there is no leak.
    }
  }
}
