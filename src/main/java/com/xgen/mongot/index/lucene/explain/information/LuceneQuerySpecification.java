package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.query.util.WrappedToChildBlockJoinQuery;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.collections4.Equator;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;

public interface LuceneQuerySpecification extends DocumentEncodable {
  enum Type {
    BOOLEAN_QUERY("BooleanQuery"),
    BOOST_QUERY("BoostQuery"),
    CONSTANT_SCORE_QUERY("ConstantScoreQuery"),
    DEFAULT_QUERY(Optional.empty()),
    DISABLE_BULK_SCORER_QUERY("DisableBulkScorerQuery"),
    DISJUNCTION_MAX_QUERY(DisjunctionMaxQuery.class.getSimpleName()),
    DOC_AND_SCORE_QUERY("DocAndScoreQuery"),
    DRILL_SIDEWAYS_QUERY("DrillSidewaysQuery"),
    EXACT_VECTOR_SEARCH_QUERY("ExactVectorSearchQuery"),
    FUNCTION_SCORE_QUERY("FunctionScoreQuery"),
    FUZZY_QUERY(FuzzyQuery.class.getSimpleName()),
    INDEX_OR_DOC_VALUES_QUERY("IndexOrDocValuesQuery"),
    INSTRUMENTABLE_KNN_BYTE_VECTOR_QUERY("InstrumentableKnnByteVectorQuery"),
    INSTRUMENTABLE_KNN_FLOAT_VECTOR_QUERY("InstrumentableKnnFloatVectorQuery"),
    LAT_LON_POINT_DISTANCE_QUERY("LatLonPointDistanceQuery"),
    LAT_LON_SHAPE_QUERY("LatLonShapeQuery"),
    LONG_DISTANCE_FEATURE_QUERY("LongDistanceFeatureQuery"),
    MATCH_ALL_DOCS_QUERY(MatchAllDocsQuery.class.getSimpleName()),
    MATCH_NO_DOCS_QUERY(MatchNoDocsQuery.class.getSimpleName()),
    MULTI_PHRASE_QUERY(MultiPhraseQuery.class.getSimpleName()),
    MULTI_TERM_QUERY_CONSTANT_SCORE_BLENDED_WRAPPER("MultiTermQueryConstantScoreBlendedWrapper"),
    SORTED_NUMERIC_DOC_VALUES_RANGE_QUERY("SortedNumericDocValuesRangeQuery"),
    SORTED_SET_DOC_VALUES_RANGE_QUERY("SortedSetDocValuesRangeQuery"),
    PHRASE_QUERY("PhraseQuery"),
    POINT_IN_SET_QUERY(Optional.empty(), Optional.of("org.apache.lucene.document.LongPoint$3")),
    POINT_RANGE_QUERY(Optional.empty(), Optional.of("org.apache.lucene.document.LongPoint$1")),
    PREFIX_QUERY(PrefixQuery.class.getSimpleName()),
    REGEX_QUERY(RegexpQuery.class.getSimpleName()),
    SYNONYM_QUERY("SynonymQuery"),
    TERM_QUERY("TermQuery"),
    TERM_IN_SET_QUERY(TermInSetQuery.class.getSimpleName()),
    TERM_RANGE_QUERY(TermRangeQuery.class.getSimpleName()),
    WILDCARD_QUERY(WildcardQuery.class.getSimpleName()),
    WRAPPED_KNN_QUERY("WrappedKnnQuery"),
    WRAPPED_TO_PARENT_BLOCK_JOIN_QUERY("WrappedToParentBlockJoinQuery"),
    WRAPPED_TO_CHILD_BLOCK_JOIN_QUERY(WrappedToChildBlockJoinQuery.class.getSimpleName()),
    ;

    private final Optional<String> simpleClassName;
    private final Optional<String> fullClassName;

    Type(String simpleClassName) {
      this(Optional.of(simpleClassName));
    }

    Type(Optional<String> simpleClassName) {
      this(simpleClassName, Optional.empty());
    }

    Type(Optional<String> simpleClassName, Optional<String> fullClassName) {
      this.simpleClassName = simpleClassName;
      this.fullClassName = fullClassName;
    }

    private static Optional<Type> ofSimpleClass(String simpleClassName) {
      return ofClass(type -> type.simpleClassName.map(simpleClassName::equals).orElse(false));
    }

    private static Optional<Type> ofFullClass(String fullClassName) {
      return ofClass(type -> type.fullClassName.map(fullClassName::equals).orElse(false));
    }

    private static Optional<Type> ofClass(Predicate<Type> predicate) {
      return Arrays.stream(Type.values()).filter(predicate).findFirst();
    }

    public static Type classFor(String simpleClassName, String fullClassName) {
      return Stream.of(ofSimpleClass(simpleClassName), ofFullClass(fullClassName))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst()
          .orElse(DEFAULT_QUERY);
    }
  }

  Type getType();

  /**
   * Constructs a Field.Required for args depending on the LuceneQuerySpecification subclass type.
   */
  static Field.Required<LuceneQuerySpecification> argsFieldForType(Type type) {
    return builderForType(type).disallowUnknownFields().required();
  }

  /**
   * Test if this LuceneQuerySpecification is equal to another.
   *
   * <p>We specify an {@link Equator} to use when testing equality of {@link
   * QueryExplainInformation} children of this LuceneQuerySpecification because there are fields of
   * LuceneExplainInformation that do not implement {@code equals()}. We defer to the user to decide
   * what fields of child LuceneExplainInformation specifications they are interested in when
   * testing equality. Consider, for example, a test that is disinterested in the timing stats of
   * its children.
   *
   * <p>We specify a {@link Comparator} for {@code LuceneExplainInformation}s to be able to impose
   * total order on any lists of clauses. This allows us to directly compare elements of unordered
   * lists.
   *
   * <p>Implementing {@code equals} in this fashion allows us to write equality testing logic
   * specific to implementers of LuceneQuerySpecification directly in those classes, while letting
   * the caller decide how strictly it wants to test for equality (e.g. does the caller care about
   * operator equality? or how closely timings should be to be considered equal?).
   */
  boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter);

  private static ClassField.DocumentFieldBuilder<LuceneQuerySpecification> builderForType(
      Type type) {
    Field.TypeBuilder builder = Field.builder("args");

    return switch (type) {
      case TERM_QUERY -> builder.classField(TermQuerySpec::fromBson);
      case BOOLEAN_QUERY -> builder.classField(BooleanQuerySpec::fromBson);
      case BOOST_QUERY -> builder.classField(BoostQuerySpec::fromBson);
      case CONSTANT_SCORE_QUERY -> builder.classField(ConstantScoreQuerySpec::fromBson);
      case DISABLE_BULK_SCORER_QUERY ->
          builder.classField(DisableBulkScorerQuerySpec::fromBson);
      case DISJUNCTION_MAX_QUERY -> builder.classField(DisjunctionMaxQuerySpec::fromBson);
      case DOC_AND_SCORE_QUERY -> builder.classField(DocAndScoreQuerySpec::fromBson);
      case DRILL_SIDEWAYS_QUERY -> builder.classField(DrillSidewaysQuerySpec::fromBson);
      case EXACT_VECTOR_SEARCH_QUERY -> builder.classField(ExactVectorSearchQuerySpec::fromBson);
      case FUNCTION_SCORE_QUERY -> builder.classField(FunctionScoreQuerySpec::fromBson);
      case FUZZY_QUERY -> builder.classField(FuzzyQuerySpec::fromBson);
      case INDEX_OR_DOC_VALUES_QUERY -> builder.classField(IndexOrDocValuesQuerySpec::fromBson);
      case LONG_DISTANCE_FEATURE_QUERY ->
          builder.classField(LongDistanceFeatureQuerySpec::fromBson);
      case LAT_LON_POINT_DISTANCE_QUERY ->
          builder.classField(LatLonPointDistanceQuerySpec::fromBson);
      case LAT_LON_SHAPE_QUERY -> builder.classField(LatLonShapeQuerySpec::fromBson);
      case MATCH_ALL_DOCS_QUERY -> builder.classField(MatchAllDocsQuerySpec::fromBson);
      case MATCH_NO_DOCS_QUERY -> builder.classField(MatchNoDocsQuerySpec::fromBson);
      case MULTI_PHRASE_QUERY -> builder.classField(MultiPhraseQuerySpec::fromBson);
      case SORTED_NUMERIC_DOC_VALUES_RANGE_QUERY ->
          builder.classField(SortedNumericDocValuesRangeQuerySpec::fromBson);
      case SORTED_SET_DOC_VALUES_RANGE_QUERY ->
          builder.classField(SortedSetDocValuesRangeQuerySpec::fromBson);
      case POINT_IN_SET_QUERY -> builder.classField(PointInSetQuerySpec::fromBson);
      case POINT_RANGE_QUERY -> builder.classField(PointRangeQuerySpec::fromBson);
      case PHRASE_QUERY -> builder.classField(PhraseQuerySpec::fromBson);
      case PREFIX_QUERY -> builder.classField(PrefixQuerySpec::fromBson);
      case REGEX_QUERY -> builder.classField(RegexQuerySpec::fromBson);
      case MULTI_TERM_QUERY_CONSTANT_SCORE_BLENDED_WRAPPER ->
          builder.classField(MultiTermQueryConstantScoreBlendedWrapperSpec::fromBson);
      case SYNONYM_QUERY -> builder.classField(SynonymQuerySpec::fromBson);
      case TERM_IN_SET_QUERY -> builder.classField(TermInSetQuerySpec::fromBson);
      case TERM_RANGE_QUERY -> builder.classField(TermRangeQuerySpec::fromBson);
      case WILDCARD_QUERY -> builder.classField(WildcardQuerySpec::fromBson);
      case INSTRUMENTABLE_KNN_BYTE_VECTOR_QUERY ->
          builder.classField(InstrumentableKnnByteVectorQuerySpec::fromBson);
      case INSTRUMENTABLE_KNN_FLOAT_VECTOR_QUERY ->
          builder.classField(InstrumentableKnnFloatVectorQuerySpec::fromBson);
      case WRAPPED_KNN_QUERY -> builder.classField(WrappedKnnQuerySpec::fromBson);
      case WRAPPED_TO_PARENT_BLOCK_JOIN_QUERY ->
          builder.classField(WrappedToParentBlockJoinQuerySpec::fromBson);
      case WRAPPED_TO_CHILD_BLOCK_JOIN_QUERY ->
          builder.classField(WrappedToChildBlockJoinQuerySpec::fromBson);
      case DEFAULT_QUERY -> builder.classField(DefaultQuerySpec::fromBson);
    };
  }
}
