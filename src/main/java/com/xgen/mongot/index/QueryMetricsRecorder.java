package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.RegexOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermFuzzyOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.WildcardOperator;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.CompoundClause;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.NotOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import io.micrometer.core.instrument.Counter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Obtains all the relevant counters for a query, and increments each unique counter once. Each
 * counter is intended to record the existence of its corresponding feature in a query, rather than
 * the total number of times the feature exists in a query.
 *
 * <p>For example, if a compound query contains 2 text operators, the counter tracking the text
 * operator count will be incremented by 1, not 2. This is because from a BI and product perspective
 * it is more helpful to track "the number of queries that used text" than "the number of times the
 * text operator was used across all queries". In the latter case, you wouldn't be able to calculate
 * "the percentage of queries using text".
 */
public class QueryMetricsRecorder {
  private final IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
      queryFeaturesMetricsUpdater;

  @VisibleForTesting
  QueryMetricsRecorder(
      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
          queryFeaturesMetricsUpdater) {
    this.queryFeaturesMetricsUpdater = queryFeaturesMetricsUpdater;
  }

  public void record(Query query) {
    Stream.concat(getQueryCounters(query), getExplainCounter())
        .distinct()
        .forEach(Counter::increment);
  }

  public void record(Query query, QueryCursorOptions cursorOptions) {
    // increment each unique metric counter once
    Streams.concat(
            getQueryCounters(query), getCursorOptionsCounter(cursorOptions), getExplainCounter())
        .distinct()
        .forEach(Counter::increment);
  }

  private Stream<Counter> getExplainCounter() {
    return Explain.isEnabled()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getExplainCounter())
        : Stream.empty();
  }

  private Stream<Counter> getCursorOptionsCounter(QueryCursorOptions cursorOptions) {
    if (cursorOptions.requireSequenceTokens()) {
      return Stream.of(this.queryFeaturesMetricsUpdater.getRequireSequenceTokensCounter());
    }
    return Stream.empty();
  }

  private Stream<Counter> getQueryCounters(Query query) {
    return switch (query) {
      case SearchQuery searchQuery -> getQueryCounters(searchQuery);
      case VectorSearchQuery vectorSearchQuery ->
          getVectorSearchOperatorCounters(vectorSearchQuery.criteria());
    };
  }

  private Stream<Counter> getQueryCounters(SearchQuery query) {
    return Streams.concat(
        getQueryTypeSpecificCounters(query),
        getConcurrentCounter(query),
        getReturnStoredSourceCounter(query),
        getScoreDetailsCounter(query),
        getPaginationCounter(query),
        getSortCounters(query),
        getTrackingCounter(query),
        getQueryHighlightCounters(query),
        getReturnScopeCounter(query));
  }

  private Stream<? extends Counter> getScoreDetailsCounter(SearchQuery query) {
    return query.scoreDetails()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getScoreDetailsCounter())
        : Stream.empty();
  }

  private Stream<Counter> getConcurrentCounter(Query query) {
    return query.concurrent()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getConcurrentCounter())
        : Stream.empty();
  }

  private Stream<Counter> getReturnStoredSourceCounter(Query query) {
    return query.returnStoredSource()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getReturnStoredSourceCounter())
        : Stream.empty();
  }

  private Stream<Counter> getPaginationCounter(SearchQuery query) {
    return query
        .pagination()
        .map((pagination) -> this.queryFeaturesMetricsUpdater.getSequenceTokenCounter())
        .stream();
  }

  private Stream<Counter> getSortCounters(SearchQuery query) {
    if (query.sortSpec().isPresent()) {
      Stream<Counter> sortCounter = Stream.of(this.queryFeaturesMetricsUpdater.getSortCounter());
      Stream<Counter> noDataCounter =
          query.sortSpec().get().getSortFields().stream()
              .map(sortField -> sortField.options())
              .filter(sortOptions -> sortOptions instanceof UserFieldSortOptions)
              .map(sortOptions -> ((UserFieldSortOptions) sortOptions).nullEmptySortPosition())
              .map(this.queryFeaturesMetricsUpdater::getNoDataSortPositionCounter);
      return Stream.concat(sortCounter, noDataCounter);
    }
    return Stream.empty();
  }

  private Stream<Counter> getTrackingCounter(SearchQuery query) {
    return query.tracking().isPresent()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getTrackingCounter())
        : Stream.empty();
  }

  private Stream<Counter> getQueryHighlightCounters(SearchQuery query) {
    return query.highlight().isPresent()
        ? Stream.concat(
            Stream.of(this.queryFeaturesMetricsUpdater.getHighlightingCounter()),
            maybeGetWildcardPathCounter(query.highlight().get().paths()))
        : Stream.empty();
  }

  private Stream<Counter> getReturnScopeCounter(SearchQuery query) {
    return query.returnScope().isPresent()
        ? Stream.of(this.queryFeaturesMetricsUpdater.getReturnScopeCounter())
        : Stream.empty();
  }

  private Stream<Counter> getQueryTypeSpecificCounters(SearchQuery query) {
    return switch (query) {
      case CollectorQuery collectorQuery -> getCollectorCounters(collectorQuery.collector());
      case OperatorQuery operatorQuery -> getOperatorCounters(operatorQuery.operator());
    };
  }

  private Stream<Counter> getCollectorCounters(Collector collector) {
    Stream<Counter> collectorTypeCounter =
        Stream.of(this.queryFeaturesMetricsUpdater.getCollectorTypeCounter(collector.getType()));

    return switch (collector) {
      case FacetCollector facetCollector ->
          Stream.concat(getOperatorCounters(facetCollector.operator()), collectorTypeCounter);
    };
  }

  private Stream<Counter> getOperatorCounters(Operator operator) {
    Stream<Counter> operatorTypeCounters =
        Stream.of(this.queryFeaturesMetricsUpdater.getOperatorTypeCounter(operator.getType()));
    Stream<Counter> scoreTypeCounters =
        Stream.of(this.queryFeaturesMetricsUpdater.getScoreTypeCounter(operator.score().getType()));
    Stream<Counter> operatorStatCounters = getDetailedOperatorStatCounters(operator);

    return Stream.of(operatorTypeCounters, scoreTypeCounters, operatorStatCounters)
        .flatMap(counterStream -> counterStream);
  }

  /**
   * Retrieves detailed counters related to the specific type of the given operator. For unsupported
   * operator types (e.g., RangeOperator, ExistsOperator), an empty stream is returned.
   */
  private Stream<Counter> getDetailedOperatorStatCounters(Operator operator) {
    return switch (operator) {
      case AutocompleteOperator autocompleteOperator ->
          getAutocompleteStatCounters(autocompleteOperator);
      case CompoundOperator compoundOperator -> getCompoundStatCounters(compoundOperator);
      case PhraseOperator phraseOperator -> getPhraseStatCounters(phraseOperator);
      case RegexOperator regexOperator -> getRegexStatCounters(regexOperator);
      case TermFuzzyOperator termFuzzyOperator ->
          Stream.of(this.queryFeaturesMetricsUpdater.getFuzzyCounter());
      case TextOperator textOperator -> getTextStatCounters(textOperator);
      case WildcardOperator wildcardOperator -> getWildcardStatCounters(wildcardOperator);
      case VectorSearchOperator vectorSearchOperator ->
          getVectorSearchOperatorCounters(vectorSearchOperator.criteria());
      case KnnBetaOperator knnBetaOperator -> getKnnBetaStatCounters(knnBetaOperator);

      case AllDocumentsOperator allDocumentsOperator -> Stream.empty();
      case EqualsOperator equalsOperator -> Stream.empty();
      case ExistsOperator existsOperator -> Stream.empty();
      case EmbeddedDocumentOperator embeddedDocumentOperator -> Stream.empty();
      case GeoShapeOperator geoShapeOperator -> Stream.empty();
      case GeoWithinOperator geoWithinOperator -> Stream.empty();
      case HasAncestorOperator hasAncestorOperator -> Stream.empty();
      case HasRootOperator hasRootOperator -> Stream.empty();
      case InOperator inOperator -> Stream.empty();
      case MoreLikeThisOperator moreLikeThisOperator -> Stream.empty();
      case NearOperator nearOperator -> Stream.empty();
      case QueryStringOperator queryStringOperator -> Stream.empty();
      case RangeOperator rangeOperator -> Stream.empty();
      case SearchOperator searchOperator -> Stream.empty();
      case SpanOperator spanOperator -> Stream.empty();
      case TermOperator termOperator -> Stream.empty();
    };
  }

  @VisibleForTesting
  static Stream<Operator> getAllLeafOperators(Operator operator) {
    return (operator instanceof CompoundOperator o)
        ? o.getOperators().flatMap(QueryMetricsRecorder::getAllLeafOperators)
        : Stream.of(operator);
  }

  static Stream<MqlFilterOperator.Category> getAllLeafOperators(Clause operator) {
    return switch (operator) {
      case SimpleClause f ->
          f.mqlFilterOperators().stream().flatMap(QueryMetricsRecorder::getAllLeafOperators);
      case CompoundClause c ->
          c.getClauses().stream().flatMap(QueryMetricsRecorder::getAllLeafOperators);
    };
  }

  static Stream<MqlFilterOperator.Category> getAllLeafOperators(MqlFilterOperator operator) {
    return switch (operator) {
      case NotOperator f ->
          Stream.concat(
              f.negateValues().mqlFilterOperators().stream().map(MqlFilterOperator::getCategory),
              Stream.of(operator.getCategory()));
      default -> Stream.of(operator.getCategory());
    };
  }

  private Stream<Counter> getKnnBetaStatCounters(KnnBetaOperator knnBetaOperator) {
    return knnBetaOperator.filter().stream()
        .flatMap(QueryMetricsRecorder::getAllLeafOperators)
        .map(Operator::getType)
        .map(this.queryFeaturesMetricsUpdater::getKnnBetaFilterOperatorTypeCounterMap);
  }

  /** Retrieve counters related to both $vectorSearch and $search.vectorSearch. */
  private Stream<Counter> getVectorSearchOperatorCounters(VectorSearchCriteria criteria) {

    // Exact vs Approximate
    Stream<Counter> vectorSearchTypeCounter =
        Stream.of(
            this.queryFeaturesMetricsUpdater.getVectorSearchQueryTypeCounter(
                criteria.getVectorSearchType()));
    Optional<VectorSearchFilter> maybeFilter = criteria.filter();
    if (maybeFilter.isEmpty()) {
      return vectorSearchTypeCounter;
    }

    var vectorSearchWithFilterCounter =
        criteria
            .filter()
            .map(e -> this.queryFeaturesMetricsUpdater.getFilteredVectorSearchCounter())
            .stream();

    VectorSearchFilter filter = maybeFilter.get();
    Stream<Counter> filterTypeCounters =
        switch (filter) {
          case VectorSearchFilter.ClauseFilter clauseFilter ->
              QueryMetricsRecorder.getAllLeafOperators(clauseFilter.clause())
                  .map(
                      this.queryFeaturesMetricsUpdater
                          ::getVectorSearchFilterOperatorTypeCounterMap);
          case VectorSearchFilter.OperatorFilter operatorFilter ->
              Stream.of(operatorFilter.operator())
                  .flatMap(QueryMetricsRecorder::getAllLeafOperators)
                  .map(Operator::getType)
                  .map(
                      this.queryFeaturesMetricsUpdater
                          ::getSearchVectorSearchFilterOperatorTypeCounterMap);
        };

    return Streams.concat(
        vectorSearchTypeCounter, filterTypeCounters, vectorSearchWithFilterCounter);
  }

  private Stream<Counter> getCompoundStatCounters(CompoundOperator compoundOperator) {
    return compoundOperator.getOperators().flatMap(this::getOperatorCounters);
  }

  private Stream<Counter> getAutocompleteStatCounters(AutocompleteOperator autocomplete) {
    return autocomplete
        .fuzzy()
        .map(ignored -> this.queryFeaturesMetricsUpdater.getFuzzyCounter())
        .stream();
  }

  private Stream<Counter> getTextStatCounters(TextOperator text) {
    Stream<Counter> textMatchCriteriaCounter =
        text
            .matchCriteria()
            .map(this.queryFeaturesMetricsUpdater::getTextMatchCriteriaCounter)
            .stream();
    Stream<Counter> deprecatedSynonymCounter =
        text.matchCriteria().isEmpty()
            ? text.synonyms().isPresent()
                ? Stream.of(this.queryFeaturesMetricsUpdater.getTextDeprecatedSynonymsCounter())
                : Stream.empty()
            : Stream.empty();
    Stream<Counter> fuzzyCounter =
        text.fuzzy().map(ignored -> this.queryFeaturesMetricsUpdater.getFuzzyCounter()).stream();
    Stream<Counter> textSynonymCounter =
        text
            .synonyms()
            .map(ignored -> this.queryFeaturesMetricsUpdater.getTextSynonymsCounter())
            .stream();
    Stream<Counter> synonymCounter =
        text
            .synonyms()
            .map(ignored -> this.queryFeaturesMetricsUpdater.getSynonymsCounter())
            .stream();
    Stream<Counter> wildcardPathCounter = maybeGetWildcardPathCounter(text.paths());

    return Stream.of(
            textMatchCriteriaCounter,
            fuzzyCounter,
            deprecatedSynonymCounter,
            textSynonymCounter,
            wildcardPathCounter,
            synonymCounter)
        .flatMap(counterStream -> counterStream);
  }

  private Stream<Counter> getPhraseStatCounters(PhraseOperator phrase) {
    Stream<Counter> phraseSynonymCounter =
        phrase
            .synonyms()
            .map(ignored -> this.queryFeaturesMetricsUpdater.getPhraseSynonymsCounter())
            .stream();
    Stream<Counter> synonymCounter =
        phrase
            .synonyms()
            .map(ignored -> this.queryFeaturesMetricsUpdater.getSynonymsCounter())
            .stream();
    Stream<Counter> wildcardPathCounter = maybeGetWildcardPathCounter(phrase.paths());
    return Stream.of(phraseSynonymCounter, synonymCounter, wildcardPathCounter)
        .flatMap(counterStream -> counterStream);
  }

  private Stream<Counter> getWildcardStatCounters(WildcardOperator wildcard) {
    return maybeGetWildcardPathCounter(wildcard.paths());
  }

  private Stream<Counter> getRegexStatCounters(RegexOperator regex) {
    return maybeGetWildcardPathCounter(regex.paths());
  }

  private Stream<Counter> maybeGetWildcardPathCounter(List<UnresolvedStringPath> paths) {
    return paths.stream()
        .filter(path -> path instanceof UnresolvedStringWildcardPath)
        .map(ignored -> this.queryFeaturesMetricsUpdater.getWildcardPathsCounter());
  }
}
