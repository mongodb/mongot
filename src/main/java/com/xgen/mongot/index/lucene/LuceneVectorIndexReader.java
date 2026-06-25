package com.xgen.mongot.index.lucene;

import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.LuceneVectorQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectFactory;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectSpec;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectStage;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.bson.BsonArray;

@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class LuceneVectorIndexReader implements VectorIndexReader {

  private static final Set<String> LUCENE_ID_FIELD_NAME_SET =
      Set.of(FieldName.MetaField.ID.getLuceneFieldName());
  private static final int VECTOR_SEARCH_BATCH_SIZE = 10_000;

  private final VectorIndexDefinition indexDefinition;

  @GuardedBy("shutdownSharedLock")
  private final LuceneSearcherManager searcherManager;

  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;
  private final LuceneSearchManagerFactory luceneSearchManagerFactory;
  private final Optional<NamedExecutorService> concurrentSearchExecutor;
  private final LuceneVectorQueryFactoryDistributor queryFactory;

  private final ReentrantReadWriteLock.WriteLock shutdownExclusiveLock;
  private final ReentrantReadWriteLock.ReadLock shutdownSharedLock;

  private final FeatureFlags featureFlags;

  /**
   * In order to read closed's value you must acquire at least the shutdownSharedLock. In order to
   * flip closed's value you must acquire the shutdownExclusiveLock.
   *
   * <p>Note that @GuardedBy is not capable of expressing something being guarded by either of two
   * locks, so we do not annotate this variable.
   */
  private boolean closed;

  public LuceneVectorIndexReader(
      VectorQueryFactoryContext context,
      VectorIndexDefinition indexDefinition,
      LuceneSearcherManager searcherManager,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      LuceneVectorQueryFactoryDistributor queryFactory) {

    this.indexDefinition = indexDefinition;
    this.searcherManager = searcherManager;
    this.metricsUpdater = context.getMetrics();
    this.concurrentSearchExecutor = concurrentSearchExecutor;
    this.queryFactory = queryFactory;
    this.luceneSearchManagerFactory =
        new LuceneSearchManagerFactory(
            queryFactory.getDefinitionResolver(),
            new BinaryQuantizedVectorRescorer(concurrentVectorRescoringExecutor),
            this.metricsUpdater);

    ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock(true);
    this.shutdownExclusiveLock = shutdownLock.writeLock();
    this.shutdownSharedLock = shutdownLock.readLock();
    this.featureFlags = context.getFeatureFlags();

    this.closed = false;
  }

  public static LuceneVectorIndexReader create(
      LuceneSearcherManager searcherManager,
      VectorQueryFactoryContext factoryContext,
      VectorIndexDefinition indexDefinition,
      LuceneVectorQueryFactoryDistributor queryFactory,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor) {
    return new LuceneVectorIndexReader(
        factoryContext,
        indexDefinition,
        searcherManager,
        concurrentSearchExecutor,
        concurrentVectorRescoringExecutor,
        queryFactory);
  }

  @Override
  public BsonArray query(
      MaterializedVectorSearchQuery materializedVectorQuery, QueryExecutionContext context)
      throws ReaderClosedException, IOException, InvalidQueryException {
    return getBsonArray(queryResults(materializedVectorQuery, context), this.metricsUpdater);
  }

  @Override
  public VectorProducerAndMetaResults query(
      MaterializedVectorSearchQuery query,
      QueryExecutionContext context,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException, InterruptedException, ReaderClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("query");
      List<VectorSearchResult> allResults;
      try (var searcherReference = createSearcherReference(query.concurrent())) {
        allResults = queryResults(query, context, searcherReference);
      }
      return new VectorProducerAndMetaResults(
          new LuceneVectorSearchBatchProducer(allResults, this.metricsUpdater, batchSizeStrategy),
          MetaResults.EMPTY);
    }
  }

  /** Returns a List of {@link VectorSearchResult}. */
  public List<VectorSearchResult> queryResults(
      MaterializedVectorSearchQuery materializedVectorQuery, QueryExecutionContext context)
      throws ReaderClosedException, IOException, InvalidQueryException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("query");
      try (var searcherReference = createSearcherReference(materializedVectorQuery.concurrent())) {
        return queryResults(materializedVectorQuery, context, searcherReference);
      }
    }
  }

  /** Returns a List of {@link VectorSearchResult}. */
  public List<VectorSearchResult> queryResults(
      MaterializedVectorSearchQuery materializedVectorQuery,
      QueryExecutionContext context,
      LuceneIndexSearcherReference searcherReference)
      throws IOException, InvalidQueryException {
    var indexSearcher = searcherReference.getIndexSearcher();
    DeadlineQueryTimeout.fromDeadline(context.deadlineTimestampMs())
        .ifPresent(indexSearcher::setTimeout);

    var luceneQuery =
        Explain.isEnabled()
            ? this.queryFactory.createExplainQuery(
                materializedVectorQuery, indexSearcher.getIndexReader())
            : this.queryFactory.createQuery(
                materializedVectorQuery, indexSearcher.getIndexReader());

    LuceneSearchManager<QueryInfo> searchManager =
        this.luceneSearchManagerFactory.newVectorQueryManager(
            luceneQuery, materializedVectorQuery.materializedCriteria());

    QueryInfo queryInfo = searchManager.initialSearch(searcherReference, VECTOR_SEARCH_BATCH_SIZE);

    // Surface a timeout to the user if the deadline is exceeded
    DeadlineQueryTimeout.throwIfExceeded(Optional.ofNullable(indexSearcher.getTimeout()));

    return getResults(queryInfo.topDocs, indexSearcher, materializedVectorQuery);
  }

  public static BsonArray getBsonArray(
      List<VectorSearchResult> vectorSearchResults,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    try (var sp = Tracing.simpleSpanGuard("VectorIndexReader.getBsonArray")) {
      var stopwatch = Stopwatch.createStarted();
      var builder = BsonArrayBuilder.unlimited();

      for (var vectorSearchResult : vectorSearchResults) {
        builder.append(vectorSearchResult.toRawBson());
      }

      var results = builder.build();

      metricsUpdater.getBatchDocumentCount().record(builder.getDocumentCount());
      metricsUpdater.getBatchDataSize().record(builder.getDataSize().toBytes());
      sp.getSpan()
          .setAttribute("num vector results", results.size())
          .setAttribute("time", stopwatch.toString());

      return results;
    }
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("refresh");
      this.searcherManager.maybeRefreshBlocking();
    }
  }

  @Override
  public void open() {
    try (LockGuard ignored = LockGuard.with(this.shutdownExclusiveLock)) {
      this.closed = false;
    }
  }

  @Override
  public void close() {
    try (LockGuard ignored = LockGuard.with(this.shutdownExclusiveLock)) {
      this.closed = true;
    }
  }

  @MustBeClosed
  @GuardedBy("shutdownSharedLock")
  private LuceneIndexSearcherReference createSearcherReference(boolean concurrentQuery)
      throws IOException {
    return concurrentQuery && this.concurrentSearchExecutor.isPresent()
        ? LuceneIndexSearcherReference.create(
            this.searcherManager,
            this.concurrentSearchExecutor.get(),
            this.metricsUpdater,
            this.featureFlags)
        : LuceneIndexSearcherReference.create(
            this.searcherManager, this.metricsUpdater, this.featureFlags);
  }

  @GuardedBy("shutdownSharedLock")
  private void ensureOpen(String methodName) throws ReaderClosedException {
    if (this.closed) {
      throw ReaderClosedException.create(methodName);
    }
  }

  @GuardedBy("shutdownSharedLock")
  private List<VectorSearchResult> getResults(
      TopDocs topDocs, IndexSearcher searcher, MaterializedVectorSearchQuery query)
      throws IOException {
    try (var sp = Tracing.simpleSpanGuard("VectorIndexReader.getResults")) {

      var stopwatch = Stopwatch.createStarted();

      var scoreDocs = topDocs.scoreDocs;

      List<VectorSearchResult> vectorSearchResults = new ArrayList<>(scoreDocs.length);

      var spec =
          new ProjectSpec(query.returnStoredSource(), this.indexDefinition.getStoredSource());
      ProjectFactory pf = ProjectFactory.build(spec, searcher.getIndexReader());
      ProjectStage ps = pf.create(scoreDocs);
      var storedFields = searcher.storedFields();
      for (var scoreDoc : scoreDocs) {
        var document = storedFields.document(scoreDoc.doc, LUCENE_ID_FIELD_NAME_SET);
        var id = LuceneDocumentIdEncoder.documentIdFromLuceneDocument(document);
        vectorSearchResults.add(
            new VectorSearchResult(id, scoreDoc.score, ps.project(scoreDoc.doc)));
      }

      this.metricsUpdater.getTotalHitsLowerBoundCount().increment(topDocs.totalHits.value());
      sp.getSpan()
          .setAttribute("num vector results", vectorSearchResults.size())
          .setAttribute("time", stopwatch.toString());

      return vectorSearchResults;
    }
  }
}
