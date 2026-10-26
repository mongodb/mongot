package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.VectorSearchUtil;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.FloatCollector;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.bson.BsonNumber;

public class KnnQueryFactory {

  private final SearchQueryFactoryContext context;

  KnnQueryFactory(SearchQueryFactoryContext context) {
    this.context = context;
  }

  Query fromOperator(
      KnnBetaOperator operator, Optional<Query> filter, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    for (var path : operator.paths()) {
      this.context
          .getQueryTimeMappingChecks()
          .validateKnnVectorField(
              path, singleQueryContext.getEmbeddedRoot(), operator.vector().size());
    }

    var vector =
        operator.vector().stream()
            .mapToDouble(BsonNumber::doubleValue)
            .collect(FloatCollector::new, FloatCollector::add, FloatCollector::addAll)
            .toArray();

    for (var path : operator.paths()) {
      this.context
          .getQueryTimeMappingChecks()
          .validateKnnSimilarityIsCalculable(path, singleQueryContext.getEmbeddedRoot(), vector);
    }

    return BooleanComposer.StreamUtils.from(operator.paths())
        .map(
            path -> {
              var field =
                  FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(
                      path, singleQueryContext.getEmbeddedRoot());

              return new MongotKnnFloatQuery(
                  this.context.getMetrics(),
                  this.context.getFeatureFlags(),
                  field,
                  queryVectorFor(path, singleQueryContext, vector),
                  operator.k(),
                  filter.orElse(null));
            },
            BooleanClause.Occur.SHOULD);
  }

  /**
   * Returns the vector to query {@code path} with. Cosine knnVector fields in newer indexes store
   * unit vectors and score with dot product, so the query vector must be normalized too.
   */
  private float[] queryVectorFor(
      FieldPath path, SingleQueryContext singleQueryContext, float[] vector) {
    Optional<VectorSimilarity> similarity =
        this.context
            .getKnnVectorFieldDefinition(path, singleQueryContext.getEmbeddedRoot())
            .map(definition -> definition.specification().similarity());
    if (similarity.isEmpty()) {
      return vector;
    }
    Vector normalized =
        VectorSearchUtil.normalizeIfNeeded(
                Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
                similarity.get(),
                this.context.getIndexCapabilities())
            .vector();
    return ((FloatVector) normalized).getFloatVector();
  }
}
