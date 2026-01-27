package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
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
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.bson.BsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class LuceneVectorIndexReader implements VectorIndexReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneVectorIndexReader.class);

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
  public BsonArray query(MaterializedVectorSearchQuery materializedVectorQuery)
      throws ReaderClosedException, IOException, InvalidQueryException {
    return getBsonArray(queryResults(materializedVectorQuery), this.metricsUpdater);
  }

  /** Returns a List of {@link VectorSearchResult}. */
  public List<VectorSearchResult> queryResults(
      MaterializedVectorSearchQuery materializedVectorQuery)
      throws ReaderClosedException, IOException, InvalidQueryException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("query");

      try (var searcherReference = createSearcherReference(materializedVectorQuery.concurrent())) {
        var indexSearcher = searcherReference.getIndexSearcher();
        var luceneQuery =
            Explain.isEnabled()
                ? this.queryFactory.createExplainQuery(
                    materializedVectorQuery, indexSearcher.getIndexReader())
                : this.queryFactory.createQuery(
                    materializedVectorQuery, indexSearcher.getIndexReader());

        LuceneSearchManager<QueryInfo> searchManager =
            this.luceneSearchManagerFactory.newVectorQueryManager(
                luceneQuery, materializedVectorQuery.materializedCriteria());

        QueryInfo queryInfo =
            searchManager.initialSearch(searcherReference, VECTOR_SEARCH_BATCH_SIZE);

        return getResults(queryInfo.topDocs, indexSearcher, materializedVectorQuery);
      }
    }
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

      this.metricsUpdater.getTotalHitsLowerBoundCount().increment(topDocs.totalHits.value);
      sp.getSpan()
          .setAttribute("num vector results", vectorSearchResults.size())
          .setAttribute("time", stopwatch.toString());

      return vectorSearchResults;
    }
  }

  @Override
  public long getRequiredMemoryForVectorData() throws ReaderClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("getRequiredMemory");

      ImmutableList<VectorIndexVectorFieldDefinition> vectorFieldDefinition =
          this.queryFactory.getDefinitionResolver().getFields().stream()
              .filter(VectorIndexFieldDefinition::isVectorField)
              .map(VectorIndexFieldDefinition::asVectorField)
              .collect(ImmutableList.toImmutableList());

      try (var searcherReference = createSearcherReference(false)) {
        List<LeafReaderContext> leaves =
            searcherReference.getIndexSearcher().getIndexReader().leaves();
        @Var long requiredMemory = 0L;
        for (VectorIndexVectorFieldDefinition vectorField : vectorFieldDefinition) {
          VectorFieldSpecification specification = vectorField.specification();
          VectorQuantization quantizationType = specification.quantization();
          for (Vector.VectorType vectorType : Vector.VectorType.values()) {
            String luceneFieldName =
                getLuceneFieldName(vectorField.getPath(), quantizationType, vectorType);

            // Get field metadata from first segment with non-empty data
            Optional<FieldInfo> fieldInfoOpt =
                leaves.stream()
                    .map(LeafReaderContext::reader)
                    .flatMap(r -> resolveFieldInfo(r, luceneFieldName).stream())
                    .findAny();

            if (fieldInfoOpt.isEmpty()) {
              continue;
            }

            FieldInfo fieldInfo = fieldInfoOpt.get();
            double bytesPerDim =
                switch (vectorType) {
                  // only float vectors can be quantized
                  case FLOAT ->
                      switch (quantizationType) {
                        case NONE -> 4;
                        case SCALAR -> 1;
                        case BINARY -> 1 / 8.0;
                      };
                  case BYTE -> 1;
                  case BIT -> 1 / 8.0;
                };

            for (LeafReaderContext leafContext : leaves) {
              LeafReader reader = leafContext.reader();
              requiredMemory += computeRequiredHeapBytes(reader, fieldInfo, bytesPerDim);
            }
          }
        }
        return Math.max(requiredMemory, 0);
      } catch (IOException e) {
        LOGGER.warn("Unable to compute requiredMemory due to unexpected Exception", e);
        return 0;
      }
    }
  }

  private Optional<FieldInfo> resolveFieldInfo(LeafReader reader, String fieldName) {
    FieldInfos fieldInfos = reader.getFieldInfos();
    return Optional.ofNullable(fieldInfos.fieldInfo(fieldName));
  }

  /**
   * Returns the amount of heap space needed in bytes to accommodate the specified vector field for
   * a single segment.
   *
   * @param reader - a reader over a single Lucene segment
   * @param fieldInfo - the metadata for the field, assumed to be a knn field
   * @param bytesPerDim - the amount of space required to store a single element of a vector. For
   *     example, this would be 0.125 for a bit vector field.
   */
  @VisibleForTesting
  static long computeRequiredHeapBytes(LeafReader reader, FieldInfo fieldInfo, double bytesPerDim)
      throws IOException {
    String fieldName = fieldInfo.getName();
    long count =
        switch (fieldInfo.getVectorEncoding()) {
          case null -> 0;
          case FLOAT32 ->
              Optional.ofNullable(reader.getFloatVectorValues(fieldName))
                  .map(FloatVectorValues::size)
                  .orElse(0);
          case BYTE ->
              Optional.ofNullable(reader.getByteVectorValues(fieldName))
                  .map(ByteVectorValues::size)
                  .orElse(0);
        };
    return (long) Math.ceil(count * bytesPerDim * fieldInfo.getVectorDimension());
  }

  // TODO(CLOUDP-333374): try to extract it from here to some utility class to unify this logic
  private String getLuceneFieldName(
      FieldPath fieldPath, VectorQuantization quantization, Vector.VectorType vectorType) {
    return switch (vectorType) {
      case FLOAT -> quantization.toTypeField().getLuceneFieldName(fieldPath, Optional.empty());
      case BYTE -> FieldName.TypeField.KNN_BYTE.getLuceneFieldName(fieldPath, Optional.empty());
      case BIT -> FieldName.TypeField.KNN_BIT.getLuceneFieldName(fieldPath, Optional.empty());
    };
  }
}
