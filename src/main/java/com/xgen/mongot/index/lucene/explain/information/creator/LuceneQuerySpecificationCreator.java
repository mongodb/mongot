package com.xgen.mongot.index.lucene.explain.information.creator;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.lucene.explain.information.DocAndScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.DrillSidewaysQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LatLonPointDistanceQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LatLonShapeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LongDistanceFeatureQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MatchAllDocsQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.MatchNoDocsQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.SortedNumericDocValuesRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.SortedSetDocValuesRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnByteVectorQuery;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnFloatVectorQuery;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToChildBlockJoinQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;

public class LuceneQuerySpecificationCreator {
  static LuceneQuerySpecification querySpecFor(
      QueryExecutionContextNode node, Explain.Verbosity verbosity) {
    Query query = node.getQuery();
    return tryCreate(query, node.getChildren(), verbosity)
        .orElseGet(() -> DefaultQuerySpecCreator.fromQuery(query));
  }

  static <
          T extends Query,
          R extends Optional<? extends QueryChildren<? extends QueryExecutionContextNode>>>
      Optional<LuceneQuerySpecification> tryCreate(
          T query, R children, Explain.Verbosity verbosity) {
    LuceneQuerySpecification.Type type = typeFor(query);
    return switch (type) {
      case BOOLEAN_QUERY ->
          tryCast(query, org.apache.lucene.search.BooleanQuery.class)
              .map(q -> BooleanQuerySpecCreator.fromQuery(q, children, verbosity));
      case BOOST_QUERY ->
          tryCast(query, org.apache.lucene.search.BoostQuery.class)
              .map(q -> BoostQuerySpecCreator.fromQuery(q, getOnlyChild(children), verbosity));
      case TERM_QUERY -> {
        Check.isEmpty(children, "children");
        yield tryCast(query, org.apache.lucene.search.TermQuery.class)
            .map(TermQuerySpecCreator::fromQuery);
      }
      case CONSTANT_SCORE_QUERY ->
          tryCast(query, org.apache.lucene.search.ConstantScoreQuery.class)
              .map(
                  q ->
                      ConstantScoreQuerySpecCreator.fromQuery(
                          q, getOnlyChild(children), verbosity));
      case DISABLE_BULK_SCORER_QUERY ->
          tryCast(query, DisableBulkScorerQuery.class)
              .map(
                  q ->
                      DisableBulkScorerQuerySpecCreator.fromQuery(
                          getOnlyChild(children), verbosity));
      case DISJUNCTION_MAX_QUERY ->
          tryCast(query, org.apache.lucene.search.DisjunctionMaxQuery.class)
              .map(q -> DisjunctionMaxQuerySpecCreator.fromQuery(q, children, verbosity));
      case DOC_AND_SCORE_QUERY -> Optional.of(new DocAndScoreQuerySpec());
      case DRILL_SIDEWAYS_QUERY -> Optional.of(new DrillSidewaysQuerySpec());
      case EXACT_VECTOR_SEARCH_QUERY ->
          tryCast(query, ExactVectorSearchQuery.class)
              .map(
                  q ->
                      ExactVectorSearchQuerySpecCreator.fromQuery(
                          q, getOnlyChild(children), verbosity));
      case FUNCTION_SCORE_QUERY ->
          tryCast(query, org.apache.lucene.queries.function.FunctionScoreQuery.class)
              .map(
                  q ->
                      FunctionScoreQuerySpecCreator.fromQuery(
                          q, getOnlyChild(children), verbosity));
      case INDEX_OR_DOC_VALUES_QUERY ->
          tryCast(query, org.apache.lucene.search.IndexOrDocValuesQuery.class)
              .map(q -> IndexOrDocValuesQuerySpecCreator.fromQuery(q, children, verbosity));
      case LONG_DISTANCE_FEATURE_QUERY -> Optional.of(new LongDistanceFeatureQuerySpec());
      case LAT_LON_POINT_DISTANCE_QUERY -> Optional.of(new LatLonPointDistanceQuerySpec());
      case LAT_LON_SHAPE_QUERY -> Optional.of(new LatLonShapeQuerySpec());
      case MATCH_ALL_DOCS_QUERY -> Optional.of(new MatchAllDocsQuerySpec());
      case MATCH_NO_DOCS_QUERY -> Optional.of(new MatchNoDocsQuerySpec());
      case MULTI_PHRASE_QUERY -> {
        Check.isEmpty(children, "children");
        yield tryCast(query, org.apache.lucene.search.MultiPhraseQuery.class)
            .map(MultiPhraseQuerySpecCreator::fromQuery);
      }
      case SORTED_NUMERIC_DOC_VALUES_RANGE_QUERY ->
          Optional.of(new SortedNumericDocValuesRangeQuerySpec());
      case SORTED_SET_DOC_VALUES_RANGE_QUERY -> Optional.of(new SortedSetDocValuesRangeQuerySpec());
      case POINT_RANGE_QUERY -> {
        Check.isEmpty(children, "children");
        yield tryCast(query, org.apache.lucene.search.PointRangeQuery.class)
            .map(PointRangeQuerySpecCreator::fromQuery);
      }
      case POINT_IN_SET_QUERY -> {
        Check.isEmpty(children, "children");
        yield tryCast(query, org.apache.lucene.search.PointInSetQuery.class)
            .map(PointInSetQuerySpecCreator::fromQuery);
      }
      case PHRASE_QUERY -> {
        Check.isEmpty(children, "children");
        yield tryCast(query, org.apache.lucene.search.PhraseQuery.class)
            .map(PhraseQuerySpecCreator::fromQuery);
      }
      case MULTI_TERM_QUERY_CONSTANT_SCORE_BLENDED_WRAPPER ->
          Optional.of(
              MultiTermQueryConstantScoreBlendedWrapperSpecCreator.fromQuery(children, verbosity));
      case SYNONYM_QUERY ->
          tryCast(query, org.apache.lucene.search.SynonymQuery.class)
              .map(SynonymQuerySpecCreator::fromQuery);
      case DEFAULT_QUERY -> Optional.of(DefaultQuerySpecCreator.fromQuery(query));
      case WILDCARD_QUERY ->
          tryCast(query, WildcardQuery.class).map(WildcardQuerySpecCreator::fromQuery);
      case PREFIX_QUERY -> tryCast(query, PrefixQuery.class).map(PrefixQuerySpecCreator::fromQuery);
      case TERM_IN_SET_QUERY ->
          tryCast(query, TermInSetQuery.class).map(TermInSetQuerySpecCreator::fromQuery);
      case TERM_RANGE_QUERY ->
          tryCast(query, TermRangeQuery.class).map(TermRangeQuerySpecCreator::fromQuery);
      case REGEX_QUERY -> tryCast(query, RegexpQuery.class).map(RegexQuerySpecCreator::fromQuery);
      case FUZZY_QUERY -> tryCast(query, FuzzyQuery.class).map(FuzzyQuerySpecCreator::fromQuery);
      case INSTRUMENTABLE_KNN_BYTE_VECTOR_QUERY ->
          tryCast(query, InstrumentableKnnByteVectorQuery.class)
              .map(InstrumentableKnnByteVectorQuerySpecCreator::fromQuery);
      case INSTRUMENTABLE_KNN_FLOAT_VECTOR_QUERY ->
          tryCast(query, InstrumentableKnnFloatVectorQuery.class)
              .map(InstrumentableKnnFloatVectorQuerySpecCreator::fromQuery);
      case WRAPPED_KNN_QUERY ->
          tryCast(query, WrappedKnnQuery.class)
              .map(q -> WrappedKnnQuerySpecCreator.fromQuery(children, verbosity));
      case WRAPPED_TO_PARENT_BLOCK_JOIN_QUERY ->
          tryCast(query, WrappedToParentBlockJoinQuery.class)
              .map(
                  q ->
                      WrappedToParentBlockJoinQuerySpecCreator.fromQuery(
                          getOnlyChild(children), verbosity));
      case WRAPPED_TO_CHILD_BLOCK_JOIN_QUERY ->
          tryCast(query, WrappedToChildBlockJoinQuery.class)
              .map(
                  q ->
                      WrappedToChildBlockJoinQuerySpecCreator.fromQuery(
                          getOnlyChild(children), verbosity));
    };
  }

  private static LuceneQuerySpecification.Type typeFor(Query query) {
    String simpleClassName = query.getClass().getSimpleName();
    String fullClassName = query.getClass().getName();
    return LuceneQuerySpecification.Type.classFor(simpleClassName, fullClassName);
  }

  private static QueryExecutionContextNode getOnlyChild(
      Optional<? extends QueryChildren<?>> children) {
    var kids = Check.isPresent(children, "children");

    List<QueryExecutionContextNode> childNodes =
        Stream.of(kids.filter(), kids.should(), kids.mustNot(), kids.must())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    checkState(childNodes.size() == 1, "constant score query must have exactly one child");
    return childNodes.get(0);
  }

  private static <T extends Query> Optional<T> tryCast(Query q, Class<T> klass) {
    T typedQuery;
    try {
      typedQuery = klass.cast(q);
    } catch (ClassCastException e) {
      return Optional.empty();
    }

    return Optional.of(typedQuery);
  }

  /** Converts a lucene field name to a FieldPath representing the dotted path the user provides. */
  protected static FieldPath strip(String luceneFieldName) {
    return FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(luceneFieldName));
  }
}
