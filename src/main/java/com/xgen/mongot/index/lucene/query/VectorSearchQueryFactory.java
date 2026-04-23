package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnByteVectorQuery;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnFloatVectorQuery;
import com.xgen.mongot.index.lucene.explain.knn.KnnInstrumentationHelper;
import com.xgen.mongot.index.lucene.explain.knn.VectorSearchExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnByteQuery;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.MetaIdRetriever;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorEmbeddedOptions;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BitVector;
import com.xgen.mongot.util.bson.ByteVector;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.bson.BsonValue;

/**
 * Translates $vectorSearch into a Lucene query. Until we disallow running $vectorSearch against
 * search indexes, this factory is used by both {@link LuceneSearchQueryFactoryDistributor} and
 * {@link LuceneVectorQueryFactoryDistributor}.
 */
class VectorSearchQueryFactory {

  private final QueryFactoryContext factoryContext;
  private final VectorSearchFilterQueryFactory vectorSearchFilterQueryFactory;

  VectorSearchQueryFactory(
      QueryFactoryContext factoryContext,
      VectorSearchFilterQueryFactory vectorSearchFilterQueryFactory) {
    this.factoryContext = factoryContext;
    this.vectorSearchFilterQueryFactory = vectorSearchFilterQueryFactory;
  }

  Query fromQuery(VectorSearchCriteria criteria, SingleQueryContext queryContext)
      throws InvalidQueryException, IOException {

    FieldPath path = criteria.path();
    Vector queryVector = Check.isPresent(criteria.queryVector(), "queryVector");

    // Check if this is an embedded vector field
    Optional<FieldPath> embeddedRoot = determineEmbeddedRoot(path, queryContext);

    if (embeddedRoot.isPresent()
        && !this.factoryContext.getFeatureFlags().isEnabled(Feature.NESTED_VECTOR)) {
      throw new InvalidQueryException(
          "Nested vector search is not supported on this cluster");
    }

    // nestedOptions is optional for embedded vector fields; defaults to scoreMode=max
    if (embeddedRoot.isEmpty() && criteria.embeddedOptions().isPresent()) {
      throw new InvalidQueryException(
          String.format(
              "\"nestedOptions\" requires a vector path within the index's nested root, but '%s'"
                  + " is outside it. Specify a path under the index's \"nestedRoot\" field, or"
                  + " remove \"nestedOptions\" to query '%s' as a standard vector field.",
              path, path));
    }

    this.factoryContext
        .getQueryTimeMappingChecks()
        .validateVectorField(path, embeddedRoot, queryVector.numDimensions());

    // Create query context with embedded root if this is an embedded vector query
    SingleQueryContext effectiveQueryContext =
        embeddedRoot.isPresent()
            ? queryContext.withEmbeddedRoot(embeddedRoot.get())
            : queryContext;

    // Create filter query (applies at leaf/embedded document level)
    Optional<Query> luceneFilter =
        criteria.filter().isPresent()
            ? Optional.of(
                this.vectorSearchFilterQueryFactory.createLuceneFilter(
                    criteria.filter().get(), effectiveQueryContext))
            : Optional.empty();

    // Create parentFilter query (applies at root/parent document level)
    // Always use a root query context (without embedded root) for parentFilter, regardless of
    // the incoming context. This ensures field names are not embedded-prefixed, which would
    // cause the filter to match nothing when AND'ed with ROOT_DOCUMENTS_QUERY.
    Optional<Query> luceneParentFilter =
        criteria.parentFilter().isPresent()
            ? Optional.of(
                this.vectorSearchFilterQueryFactory.createLuceneFilter(
                    criteria.parentFilter().get(),
                    SingleQueryContext.createQueryRoot(queryContext.getIndexReader())))
            : Optional.empty();

    VectorSimilarity indexedVectorSimilarity =
        this.factoryContext.getIndexedVectorSimilarityFunction(path, embeddedRoot);
    if (indexedVectorSimilarity == VectorSimilarity.COSINE && queryVector.isZeroVector()) {
      throw new InvalidQueryException(
          "Cosine similarity cannot be calculated against a zero vector.");
    }

    if (indexedVectorSimilarity != VectorSimilarity.EUCLIDEAN
        && queryVector.getVectorType() == Vector.VectorType.BIT) {
      throw new InvalidQueryException(
          "Binary vectors can only be used with fields indexed with Euclidean similarity but "
              + "indexed similarity is "
              + indexedVectorSimilarity);
    }

    String fieldName = getLuceneFieldName(path, embeddedRoot, queryVector.getVectorType());

    Query childQuery;
    switch (criteria) {
      case ApproximateVectorSearchCriteria approximateCriteria:
        if (!Explain.isEnabled() && approximateCriteria.explainOptions().isPresent()) {
          throw new InvalidQueryException(
              "Query must be run in explain mode, because 'explainOptions' are present.");
        }

        // Note on numCandidates for embedded vector queries:
        // For embedded vectors, numCandidates limits the number of child documents retrieved.
        // After block-join aggregation, multiple children may collapse into the same parent,
        // potentially returning fewer than `limit` parent documents. This is expected behavior,
        // consistent with how embeddedDocuments works in search indexes. Users can increase
        // numCandidates to improve recall when documents have many embedded vectors.
        int numCandidates = approximateCriteria.numCandidates();
        int limit = approximateCriteria.limit();
        ApproximateVectorQueryCreator queryCreator =
            ApproximateVectorQueryCreator.get(
                approximateCriteria,
                fieldName,
                effectiveQueryContext.getIndexReader(),
                this.factoryContext.getFeatureFlags(),
                this.factoryContext.getMetrics());
        childQuery = switch (queryVector) {
          case FloatVector floatVector ->
              queryCreator.query(floatVector, fieldName, numCandidates, limit, luceneFilter);
          case ByteVector byteVector ->
              queryCreator.query(byteVector, fieldName, numCandidates, limit, luceneFilter);
          case BitVector bitVector ->
              queryCreator.query(bitVector, fieldName, numCandidates, limit, luceneFilter);
        };
        break;

      case ExactVectorSearchCriteria unused:
        Query vectorFieldExistsQuery = new FieldExistsQuery(fieldName);
        childQuery = new com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery(
            fieldName,
            queryVector,
            indexedVectorSimilarity.getLuceneSimilarityFunction(),
            luceneFilter.isPresent()
                ? new BooleanQuery.Builder()
                    .add(vectorFieldExistsQuery, BooleanClause.Occur.FILTER)
                    .add(luceneFilter.get(), BooleanClause.Occur.FILTER)
                    .build()
                : vectorFieldExistsQuery);
        break;
    }

    // If this is an embedded vector query, wrap with ToParentBlockJoinQuery.
    // This relies on the vector indexing path (VectorIndexDocumentWrapper,
    // VectorEmbeddedDocumentBuilder) emitting the required meta fields and Lucene block structure:
    // - Root documents have $meta/embeddedRoot: "T" (via addEmbeddedRootField)
    // - Child documents have $meta/embeddedPath: "<path>" (via addEmbeddedPathField)
    // - Documents are written as Lucene blocks with children preceding the parent
    if (embeddedRoot.isPresent()) {
      ScoreMode scoreMode = criteria.embeddedOptions()
          .map(this::getScoreMode)
          .orElse(ScoreMode.Max);
      BitSetProducer parentFilterBitSet = new QueryBitSetProducer(parentFilter());

      Query blockJoinQuery =
          new WrappedToParentBlockJoinQuery(childQuery, parentFilterBitSet, scoreMode);

      // If parentFilter is present, combine it with the block join query using AND
      if (luceneParentFilter.isPresent()) {
        // Ensure parentFilter only matches root documents by combining it with ROOT_DOCUMENTS_QUERY
        // This is necessary because in embedded vector indexes, both parent and child documents
        // are indexed in the same Lucene index, and we need to ensure the parentFilter only
        // applies to root documents (those with $meta/embeddedRoot: "T"), not child documents.
        Query scopedParentFilter =
            new BooleanQuery.Builder()
                .add(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY, BooleanClause.Occur.FILTER)
                .add(luceneParentFilter.get(), BooleanClause.Occur.FILTER)
                .build();

        return new BooleanQuery.Builder()
            .add(blockJoinQuery, BooleanClause.Occur.MUST)
            .add(scopedParentFilter, BooleanClause.Occur.FILTER)
            .build();
      }

      return blockJoinQuery;
    }

    // For non-embedded vectors, if parentFilter is present, combine it with the child query
    if (luceneParentFilter.isPresent()) {
      return new BooleanQuery.Builder()
          .add(childQuery, BooleanClause.Occur.MUST)
          .add(luceneParentFilter.get(), BooleanClause.Occur.FILTER)
          .build();
    }

    return childQuery;
  }

  private interface ApproximateVectorQueryCreator {

    Query query(FloatVector vector, String field, int k, int limit, Optional<Query> filter);

    Query query(ByteVector vector, String field, int k, int limit, Optional<Query> filter);

    Query query(BitVector vector, String field, int k, int limit, Optional<Query> filter);

    static ApproximateVectorQueryCreator get(
        ApproximateVectorSearchCriteria criteria,
        String fieldName,
        IndexReader indexReader,
        FeatureFlags flags,
        IndexMetricsUpdater.QueryingMetricsUpdater metrics)
        throws IOException {
      if (Explain.getQueryInfo().isPresent()) {
        Optional<ApproximateVectorSearchCriteria.ExplainOptions> explainOptions =
            criteria.explainOptions();

        if (explainOptions.isEmpty()) {
          return new InstrumentableApproximateVectorQueryCreator(
              Explain.getQueryInfo().get(), List.of(), flags, metrics);
        }

        List<VectorSearchExplainer.TracingTarget> targets =
            resolveTraceTargets(explainOptions.get().traceDocuments(), fieldName, indexReader);
        return new InstrumentableApproximateVectorQueryCreator(
            Explain.getQueryInfo().get(), targets, flags, metrics);
      }

      return new RegularApproximateVectorQueryCreator(metrics, flags);
    }

    private static List<VectorSearchExplainer.TracingTarget> resolveTraceTargets(
        List<BsonValue> traceIds, String fieldName, IndexReader indexReader) throws IOException {

      IndexSearcher freshSearcher = new IndexSearcher(indexReader);
      BooleanQuery.Builder idQueryBuilder = new BooleanQuery.Builder();
      for (BsonValue traceId : traceIds) {
        TermQuery idQuery =
            new TermQuery(
                LuceneDocumentIdEncoder.documentIdTerm(
                    LuceneDocumentIdEncoder.encodeDocumentId(traceId)));
        idQueryBuilder.add(new BooleanClause(idQuery, BooleanClause.Occur.SHOULD));
      }

      BooleanQuery.Builder queryBuilder =
          new BooleanQuery.Builder()
              .add(new ConstantScoreQuery(idQueryBuilder.build()), BooleanClause.Occur.MUST)
              .add(
                  new ConstantScoreQuery(new FieldExistsQuery(fieldName)),
                  BooleanClause.Occur.SHOULD);

      MetaIdRetriever metaIdRetriever = MetaIdRetriever.create(indexReader);

      TopDocs res = freshSearcher.search(queryBuilder.build(), traceIds.size());
      List<VectorSearchExplainer.TracingTarget> targets = new ArrayList<>(res.scoreDocs.length);
      for (int i = 0; i < res.scoreDocs.length; i++) {
        int docId = res.scoreDocs[i].doc;
        boolean hasNoVector = res.scoreDocs[i].score < 2;
        VectorSearchExplainer.TracingTarget target =
            new VectorSearchExplainer.TracingTarget(
                metaIdRetriever.getRootMetaId(docId), docId, hasNoVector);
        targets.add(target);
      }
      return targets;
    }

    class RegularApproximateVectorQueryCreator implements ApproximateVectorQueryCreator {

      private final IndexMetricsUpdater.QueryingMetricsUpdater metrics;
      private final FeatureFlags flags;

      private RegularApproximateVectorQueryCreator(
          IndexMetricsUpdater.QueryingMetricsUpdater metrics, FeatureFlags flags) {
        this.metrics = metrics;
        this.flags = flags;
      }

      @Override
      public Query query(
          FloatVector vector, String field, int k, int limit, Optional<Query> filter) {
        float[] target = vector.getFloatVector();
        return new MongotKnnFloatQuery(
            this.metrics, this.flags, field, target, k, filter.orElse(null));
      }

      @Override
      public Query query(
          ByteVector vector, String field, int k, int limit, Optional<Query> filter) {
        byte[] target = vector.getByteVector();
        return new MongotKnnByteQuery(this.metrics, field, target, k, filter.orElse(null));
      }

      @Override
      public Query query(BitVector vector, String field, int k, int limit, Optional<Query> filter) {
        byte[] target = vector.getBitVector();
        return new MongotKnnByteQuery(this.metrics, field, target, k, filter.orElse(null));
      }
    }

    class InstrumentableApproximateVectorQueryCreator implements ApproximateVectorQueryCreator {

      private final VectorSearchExplainer tracingExplainer;
      private final IndexMetricsUpdater.QueryingMetricsUpdater metrics;
      private final FeatureFlags flags;

      private InstrumentableApproximateVectorQueryCreator(
          Explain.QueryInfo explainQueryInfo,
          List<VectorSearchExplainer.TracingTarget> tracingTargets,
          FeatureFlags flags,
          IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
        this.flags = flags;
        this.tracingExplainer =
            explainQueryInfo.getFeatureExplainer(
                VectorSearchExplainer.class, () -> new VectorSearchExplainer(tracingTargets));
        this.metrics = metrics;
      }

      @Override
      public Query query(
          FloatVector vector, String field, int k, int limit, Optional<Query> filter) {
        float[] target = vector.getFloatVector();
        boolean filterPresent = filter.isPresent();
        KnnInstrumentationHelper instrumentationHelper =
            new KnnInstrumentationHelper(this.tracingExplainer, field, limit, filterPresent);

        return new InstrumentableKnnFloatVectorQuery(
            this.metrics, this.flags, instrumentationHelper, field, target, k, filter.orElse(null));
      }

      @Override
      public Query query(
          ByteVector vector, String field, int k, int limit, Optional<Query> filter) {
        byte[] target = vector.getByteVector();
        boolean filterPresent = filter.isPresent();
        KnnInstrumentationHelper instrumentationHelper =
            new KnnInstrumentationHelper(this.tracingExplainer, field, limit, filterPresent);

        return filterPresent
            ? new InstrumentableKnnByteVectorQuery(
                this.metrics, instrumentationHelper, field, target, k, filter.get())
            : new InstrumentableKnnByteVectorQuery(
                this.metrics, instrumentationHelper, field, target, k);
      }

      @Override
      public Query query(BitVector vector, String field, int k, int limit, Optional<Query> filter) {
        byte[] target = vector.getBitVector();
        boolean filterPresent = filter.isPresent();
        KnnInstrumentationHelper instrumentationHelper =
            new KnnInstrumentationHelper(this.tracingExplainer, field, limit, filterPresent);

        return filterPresent
            ? new InstrumentableKnnByteVectorQuery(
                this.metrics, instrumentationHelper, field, target, k, filter.get())
            : new InstrumentableKnnByteVectorQuery(
                this.metrics, instrumentationHelper, field, target, k);
      }
    }
  }

  private String getLuceneFieldName(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot, Vector.VectorType vectorType)
      throws InvalidQueryException {
    return switch (vectorType) {
      case FLOAT ->
          this.factoryContext
              .getIndexedQuantization(fieldPath, embeddedRoot)
              .toTypeField()
              .getLuceneFieldName(fieldPath, embeddedRoot);
      case BYTE -> FieldName.TypeField.KNN_BYTE.getLuceneFieldName(fieldPath, embeddedRoot);
      case BIT -> FieldName.TypeField.KNN_BIT.getLuceneFieldName(fieldPath, embeddedRoot);
    };
  }

  /**
   * Determines the embedded root for a vector field path.
   *
   * <p>A vector field is considered embedded if:
   *
   * <ol>
   *   <li>The query context already has an embedded root, OR
   *   <li>The index has a nestedRoot defined AND the vector field path is a child of that
   *       nestedRoot
   * </ol>
   *
   * <p>Example of an embedded vector field: Given a document structure:
   *
   * <pre>{@code
   * {
   *   "title": "My Document",
   *   "sections": [
   *     { "text": "Introduction", "embedding": [0.1, 0.2, ...] },
   *     { "text": "Conclusion", "embedding": [0.3, 0.4, ...] }
   *   ]
   * }
   * }</pre>
   *
   * <p>With an index definition specifying {@code nestedRoot: "sections"}, the vector field path
   * "sections.embedding" is considered embedded because it resides within the nested array.
   *
   * <p>By design, the index-level {@code nestedRoot} is always used as the embedded root for all
   * vector fields under it, regardless of nesting depth. For example, if {@code nestedRoot} is
   * "sections" and the vector path is "sections.paragraphs.vector", the embedded root is still
   * "sections" (not "sections.paragraphs"). This matches the indexing behavior where all array
   * elements at the nestedRoot level are indexed as separate Lucene documents in a block, and
   * deeper nested structures are flattened within each child document.
   *
   * @param path the vector field path
   * @param queryContext the query context
   * @return Optional containing the embedded root path, or empty if not embedded
   */
  private Optional<FieldPath> determineEmbeddedRoot(
      FieldPath path, SingleQueryContext queryContext) {
    // If already in an embedded context, use that
    if (queryContext.getEmbeddedRoot().isPresent()) {
      return queryContext.getEmbeddedRoot();
    }

    // Check if the index has a nestedRoot defined and the path is a child of it.
    // The nestedRoot is always used as the embedded root - deeper nesting is flattened.
    Optional<FieldPath> nestedRoot = this.factoryContext.getNestedRoot();
    if (nestedRoot.isPresent() && path.isChildOf(nestedRoot.get())) {
      return nestedRoot;
    }

    return Optional.empty();
  }

  /**
   * Converts VectorEmbeddedOptions.ScoreMode to Lucene's ScoreMode.
   *
   * @param embeddedOptions the embedded options containing the score mode
   * @return the corresponding Lucene ScoreMode
   */
  private ScoreMode getScoreMode(VectorEmbeddedOptions embeddedOptions) {
    return switch (embeddedOptions.scoreMode()) {
      case MAX -> ScoreMode.Max;
      case AVG -> ScoreMode.Avg;
    };
  }

  /**
   * Creates a parent filter query for embedded documents.
   *
   * <p>For vector indexes with nested vectors, the parent is always the root document. Vector
   * indexes use a single-level block structure where:
   *
   * <ul>
   *   <li>Each element of the {@code nestedRoot} array becomes a child Lucene document
   *   <li>The MongoDB source document becomes the parent (root) Lucene document
   *   <li>Child documents are joined back to the root document via ToParentBlockJoinQuery
   * </ul>
   *
   * <p>This differs from search indexes which support multi-level embedded document hierarchies.
   * For vector indexes, the {@code nestedRoot} must be set to the array level containing the
   * vectors (e.g., if vectors are at "sections.paragraphs.vector", use {@code nestedRoot:
   * "sections.paragraphs"}).
   *
   * @return a Query that matches root (parent) documents
   */
  private Query parentFilter() {
    // Vector indexes use single-level nesting: child documents always join to root documents.
    // The nestedRoot defines the array level for child documents, and the parent is always root.
    return EmbeddedDocumentQueryFactory.parentFilter(Optional.empty());
  }
}
