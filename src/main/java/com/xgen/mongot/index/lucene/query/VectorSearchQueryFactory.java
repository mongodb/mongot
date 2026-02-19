package com.xgen.mongot.index.lucene.query;

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
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
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
    Optional<FieldPath> embeddedRoot = queryContext.getEmbeddedRoot();

    this.factoryContext
        .getQueryTimeMappingChecks()
        .validateVectorField(path, embeddedRoot, queryVector.numDimensions());

    Optional<Query> luceneFilter =
        criteria.filter().isPresent()
            ? Optional.of(
                this.vectorSearchFilterQueryFactory.createLuceneFilter(
                    criteria.filter().get(), queryContext))
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
    switch (criteria) {
      case ApproximateVectorSearchCriteria approximateCriteria:
        if (!Explain.isEnabled() && approximateCriteria.explainOptions().isPresent()) {
          throw new InvalidQueryException(
              "Query must be run in explain mode, because 'explainOptions' are present.");
        }

        int numCandidates = approximateCriteria.numCandidates();
        int limit = approximateCriteria.limit();
        ApproximateVectorQueryCreator queryCreator =
            ApproximateVectorQueryCreator.get(
                approximateCriteria,
                fieldName,
                queryContext.getIndexReader(),
                this.factoryContext.getMetrics());
        return switch (queryVector) {
          case FloatVector floatVector ->
              queryCreator.query(floatVector, fieldName, numCandidates, limit, luceneFilter);
          case ByteVector byteVector ->
              queryCreator.query(byteVector, fieldName, numCandidates, limit, luceneFilter);
          case BitVector bitVector ->
              queryCreator.query(bitVector, fieldName, numCandidates, limit, luceneFilter);
        };

      case ExactVectorSearchCriteria unused:
        Query vectorFieldExistsQuery = new FieldExistsQuery(fieldName);
        return new com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery(
            fieldName,
            queryVector,
            indexedVectorSimilarity.getLuceneSimilarityFunction(),
            luceneFilter.isPresent()
                ? new BooleanQuery.Builder()
                    .add(vectorFieldExistsQuery, BooleanClause.Occur.FILTER)
                    .add(luceneFilter.get(), BooleanClause.Occur.FILTER)
                    .build()
                : vectorFieldExistsQuery);
    }
  }

  private interface ApproximateVectorQueryCreator {

    Query query(FloatVector vector, String field, int k, int limit, Optional<Query> filter);

    Query query(ByteVector vector, String field, int k, int limit, Optional<Query> filter);

    Query query(BitVector vector, String field, int k, int limit, Optional<Query> filter);

    static ApproximateVectorQueryCreator get(
        ApproximateVectorSearchCriteria criteria,
        String fieldName,
        IndexReader indexReader,
        IndexMetricsUpdater.QueryingMetricsUpdater metrics)
        throws IOException {
      if (Explain.getQueryInfo().isPresent()) {
        Optional<ApproximateVectorSearchCriteria.ExplainOptions> explainOptions =
            criteria.explainOptions();

        if (explainOptions.isEmpty()) {
          return new InstrumentableApproximateVectorQueryCreator(
              Explain.getQueryInfo().get(), List.of(), metrics);
        }

        List<VectorSearchExplainer.TracingTarget> targets =
            resolveTraceTargets(explainOptions.get().traceDocuments(), fieldName, indexReader);
        return new InstrumentableApproximateVectorQueryCreator(
            Explain.getQueryInfo().get(), targets, metrics);
      }

      return new RegularApproximateVectorQueryCreator(metrics);
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

      private RegularApproximateVectorQueryCreator(
          IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
        this.metrics = metrics;
      }

      @Override
      public Query query(
          FloatVector vector, String field, int k, int limit, Optional<Query> filter) {
        float[] target = vector.getFloatVector();
        return new MongotKnnFloatQuery(this.metrics, field, target, k, filter.orElse(null));
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

      private InstrumentableApproximateVectorQueryCreator(
          Explain.QueryInfo explainQueryInfo,
          List<VectorSearchExplainer.TracingTarget> tracingTargets,
          IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
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

        return filterPresent
            ? new InstrumentableKnnFloatVectorQuery(
                this.metrics, instrumentationHelper, field, target, k, filter.get())
            : new InstrumentableKnnFloatVectorQuery(
                this.metrics, instrumentationHelper, field, target, k);
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
}
