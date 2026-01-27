package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.query.InvalidQueryException.Type;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.analyzer.wrapper.QueryAnalyzerWrapper;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
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
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.WildcardOperator;
import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * This class is used for $search queries running against search index type. It can also handle
 * $vectorSearch queries as we still support running them against search indexes for legacy reasons.
 */
public class LuceneSearchQueryFactoryDistributor {
  private final AllDocsQueryFactory allDocsQueryFactory;
  private final AutocompleteQueryFactory autocompleteQueryFactory;
  private final CompoundQueryFactory compoundQueryFactory;
  private final EmbeddedDocumentQueryFactory embeddedDocumentQueryFactory;
  private final ExistsQueryFactory existsQueryFactory;
  private final GeoQueryFactory geoQueryFactory;
  private final HasAncestorQueryFactory hasAncestorQueryFactory;
  private final HasRootQueryFactory hasRootQueryFactory;
  private final KnnQueryFactory knnQueryFactory;
  private final NearQueryFactory nearQueryFactory;
  private final PhraseQueryFactory phraseQueryFactory;
  private final SearchQueryFactory searchQueryFactory;
  private final SpanQueryFactory spanQueryFactory;
  private final TermLevelQueryFactory termLevelQueryFactory;
  private final TermQueryFactory termQueryFactory;
  private final TextQueryFactory textQueryFactory;
  private final MoreLikeThisQueryFactory moreLikeThisQueryFactory;
  private final EqualsQueryFactory equalsQueryFactory;
  private final InQueryFactory inQueryFactory;
  private final RangeQueryFactory rangeQueryFactory;
  private final VectorSearchFilterQueryFactory vectorSearchFilterQueryFactory;
  private final VectorSearchQueryFactory vectorSearchQueryFactory;

  private final LuceneQueryScorer luceneQueryScorer;
  private final LuceneSortFactory luceneSortFactory;
  private final SearchQueryFactoryContext queryFactoryContext;

  /** Generates Lucene Queries from Operators, IndexDefinitions and Analyzers. */
  private LuceneSearchQueryFactoryDistributor(
      AllDocsQueryFactory allDocsQueryFactory,
      AutocompleteQueryFactory autocompleteQueryFactory,
      EmbeddedDocumentQueryFactory embeddedDocumentQueryFactory,
      ExistsQueryFactory existsQueryFactory,
      GeoQueryFactory geoQueryFactory,
      HasAncestorQueryFactory hasAncestorQueryFactory,
      HasRootQueryFactory hasRootQueryFactory,
      KnnQueryFactory knnQueryFactory,
      NearQueryFactory nearQueryFactory,
      PhraseQueryFactory phraseQueryFactory,
      SearchQueryFactory searchQueryFactory,
      SpanQueryFactory spanQueryFactory,
      TermLevelQueryFactory termLevelQueryFactory,
      TermQueryFactory termQueryFactory,
      TextQueryFactory textQueryFactory,
      MoreLikeThisQueryFactory moreLikeThisQueryFactory,
      EqualsQueryFactory equalsQueryFactory,
      InQueryFactory inQueryFactory,
      RangeQueryFactory rangeQueryFactory,
      LuceneQueryScorer luceneQueryScorer,
      LuceneSortFactory luceneSortFactory,
      SearchQueryFactoryContext queryFactoryContext) {
    this.allDocsQueryFactory = allDocsQueryFactory;
    this.autocompleteQueryFactory = autocompleteQueryFactory;
    this.embeddedDocumentQueryFactory = embeddedDocumentQueryFactory;
    this.existsQueryFactory = existsQueryFactory;
    this.compoundQueryFactory =
        new CompoundQueryFactory(
            queryFactoryContext, this::createQuery, this::createSearchExplainQuery);
    this.geoQueryFactory = geoQueryFactory;
    this.hasAncestorQueryFactory = hasAncestorQueryFactory;
    this.hasRootQueryFactory = hasRootQueryFactory;
    this.knnQueryFactory = knnQueryFactory;
    this.nearQueryFactory = nearQueryFactory;
    this.phraseQueryFactory = phraseQueryFactory;
    this.searchQueryFactory = searchQueryFactory;
    this.spanQueryFactory = spanQueryFactory;
    this.termLevelQueryFactory = termLevelQueryFactory;
    this.termQueryFactory = termQueryFactory;
    this.textQueryFactory = textQueryFactory;
    this.moreLikeThisQueryFactory = moreLikeThisQueryFactory;
    this.equalsQueryFactory = equalsQueryFactory;
    this.inQueryFactory = inQueryFactory;
    this.rangeQueryFactory = rangeQueryFactory;
    this.vectorSearchFilterQueryFactory =
        new VectorSearchFilterFromOperatorQueryFactory(
            queryFactoryContext,
            rangeQueryFactory,
            inQueryFactory,
            existsQueryFactory,
            equalsQueryFactory,
            this::createQuery);
    this.vectorSearchQueryFactory =
        new VectorSearchQueryFactory(queryFactoryContext, this.vectorSearchFilterQueryFactory);
    this.luceneQueryScorer = luceneQueryScorer;
    this.luceneSortFactory = luceneSortFactory;
    this.queryFactoryContext = queryFactoryContext;
  }

  /**
   * Use this method to create a LuceneSearchQueryFactoryDistributor.
   *
   * @param indexDefinition indexDefinition needed to lookup analyzers
   * @param analyzerRegistry analyzerRegistry used in SearchQuery
   * @param synonymRegistry synonymRegistry used in queries
   * @return LuceneQueryFactory
   */
  public static LuceneSearchQueryFactoryDistributor create(
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      SynonymRegistry synonymRegistry,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      boolean enableTextOperatorNewSynonymsSyntax,
      FeatureFlags featureFlags) {

    QueryAnalyzerWrapper analyzer = LuceneAnalyzer.queryAnalyzer(indexDefinition, analyzerRegistry);

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            analyzerRegistry,
            analyzer,
            indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
            synonymRegistry,
            queryingMetricsUpdater,
            featureFlags);
    TermQueryFactory termQueryFactory = new TermQueryFactory(queryFactoryContext);
    EqualsQueryFactory equalsQueryFactory = new EqualsQueryFactory(queryFactoryContext);
    ExistsQueryFactory existsQueryFactory = new ExistsQueryFactory(queryFactoryContext);
    RangeQueryFactory rangeQueryFactory =
        new RangeQueryFactory(queryFactoryContext, equalsQueryFactory);
    InQueryFactory inQueryFactory = new InQueryFactory(queryFactoryContext);

    AllDocsQueryFactory allDocsQueryFactory = new AllDocsQueryFactory(queryFactoryContext);
    return new LuceneSearchQueryFactoryDistributor(
        allDocsQueryFactory,
        new AutocompleteQueryFactory(queryFactoryContext),
        new EmbeddedDocumentQueryFactory(queryFactoryContext),
        existsQueryFactory,
        new GeoQueryFactory(queryFactoryContext),
        new HasAncestorQueryFactory(queryFactoryContext),
        new HasRootQueryFactory(queryFactoryContext),
        new KnnQueryFactory(queryFactoryContext),
        new NearQueryFactory(queryFactoryContext),
        new PhraseQueryFactory(queryFactoryContext),
        new SearchQueryFactory(queryFactoryContext, allDocsQueryFactory),
        new SpanQueryFactory(termQueryFactory),
        new TermLevelQueryFactory(queryFactoryContext),
        termQueryFactory,
        new TextQueryFactory(queryFactoryContext, enableTextOperatorNewSynonymsSyntax),
        new MoreLikeThisQueryFactory(queryFactoryContext, analyzer),
        equalsQueryFactory,
        inQueryFactory,
        rangeQueryFactory,
        LuceneQueryScorer.create(queryFactoryContext),
        new LuceneSortFactory(queryFactoryContext),
        queryFactoryContext);
  }

  /**
   * Creates a Lucene Query from an operator.
   *
   * @param operator Operator
   * @param indexReader IndexReader
   * @param returnScope embeddedRoot at which to return documents
   * @param queryOptimizationFlags flags used to optimize search efficiency
   * @return Lucene Query
   * @throws InvalidQueryException represents a parsing exception
   */
  public Query createQuery(
      Operator operator,
      IndexReader indexReader,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException {
    validateReturnScope(returnScope);
    var scoredQuery =
        createQuery(
            operator, SingleQueryContext.createQueryRootWithReturnScope(indexReader, returnScope));
    return queryOptimizationFlags.omitSearchDocumentResults()
        ? new ConstantScoreQuery(scoredQuery)
        : scoredQuery;
  }

  @VisibleForTesting
  Query createQuery(
      Operator operator, IndexReader indexReader, QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException {
    var scoredQuery = createQuery(operator, SingleQueryContext.createQueryRoot(indexReader));
    return queryOptimizationFlags.omitSearchDocumentResults()
        ? new ConstantScoreQuery(scoredQuery)
        : scoredQuery;
  }

  Query createQuery(Operator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    return this.luceneQueryScorer.score(
        operator,
        createUnscoredQueryFromOperator(operator, singleQueryContext),
        singleQueryContext);
  }

  public Query createVectorQuery(
      MaterializedVectorSearchQuery materializedVectorSearchQuery, IndexReader indexReader)
      throws InvalidQueryException, IOException {

    return this.vectorSearchQueryFactory.fromQuery(
        materializedVectorSearchQuery.materializedCriteria(),
        SingleQueryContext.createQueryRoot(indexReader));
  }

  /**
   * Creates a WrappedQuery from an operator to be used for explain queries.
   *
   * @param operator Operator
   * @param indexReader indexReader
   * @param returnScope embeddedRoot at which to return documents
   * @param queryOptimizationFlags flags used to optimize search efficiency
   * @return Lucene Query
   * @throws InvalidQueryException represents a parsing exception
   */
  public Query createSearchExplainQuery(
      Operator operator,
      IndexReader indexReader,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException {
    validateReturnScope(returnScope);
    var explainQuery =
        createSearchExplainQuery(
            operator,
            SingleQueryContext.createExplainRootWithReturnScope(indexReader, returnScope));
    return queryOptimizationFlags.omitSearchDocumentResults()
        ? new ConstantScoreQuery(explainQuery)
        : explainQuery;
  }

  @VisibleForTesting
  Query createSearchExplainQuery(
      Operator operator, IndexReader indexReader, QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException {
    var explainQuery =
        createSearchExplainQuery(operator, SingleQueryContext.createExplainRoot(indexReader));
    return queryOptimizationFlags.omitSearchDocumentResults()
        ? new ConstantScoreQuery(explainQuery)
        : explainQuery;
  }

  private Query createSearchExplainQuery(Operator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    if (operator instanceof VectorSearchOperator vectorSearchOperator) {
      return createVectorSearchExplainQuery(vectorSearchOperator.criteria(), singleQueryContext);
    }

    return new WrappedQuery(
        this.luceneQueryScorer.score(
            operator,
            createUnscoredQueryFromOperator(operator, singleQueryContext),
            singleQueryContext),
        singleQueryContext.getOperatorPath());
  }

  public Query createVectorSearchExplainQuery(
      MaterializedVectorSearchQuery query, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return createVectorSearchExplainQuery(
        query.materializedCriteria(), SingleQueryContext.createExplainRoot(indexReader));
  }

  private Query createVectorSearchExplainQuery(
      VectorSearchCriteria vectorSearchOperator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    return new WrappedKnnQuery(
        this.vectorSearchQueryFactory.fromQuery(vectorSearchOperator, singleQueryContext));
  }

  public Optional<Sort> createSort(
      Optional<SortSpec> sortSpec,
      Optional<SequenceToken> sequenceToken,
      FieldToSortableTypesMapping fieldsToSortableTypesMapping,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<org.apache.lucene.search.Sort> indexSort)
      throws InvalidQueryException {
    if (sortSpec.isEmpty() || queryOptimizationFlags.omitSearchDocumentResults()) {
      return Optional.empty();
    }

    Optional<FieldPath> embeddedRoot = returnScope.map(ReturnScope::path);
    this.luceneSortFactory.validateSortSpec(sortSpec.get(), embeddedRoot);
    return Optional.of(
        this.luceneSortFactory.createLuceneSort(
            sortSpec.get(), sequenceToken, fieldsToSortableTypesMapping, embeddedRoot, indexSort));
  }

  private Query createUnscoredQueryFromOperator(
      Operator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    try {
      return switch (operator) {
        case AllDocumentsOperator allDocumentsOperator ->
            this.allDocsQueryFactory.fromAllDocuments(singleQueryContext);
        case AutocompleteOperator autocompleteOperator ->
            this.autocompleteQueryFactory.fromCompletion(autocompleteOperator, singleQueryContext);
        case CompoundOperator compoundOperator ->
            this.compoundQueryFactory.fromCompound(compoundOperator, singleQueryContext);
        case EmbeddedDocumentOperator embeddedDocumentOperator ->
            this.embeddedDocumentQueryFactory.fromEmbeddedDocument(
                embeddedDocumentOperator, singleQueryContext, this);
        case EqualsOperator equalsOperator ->
            this.equalsQueryFactory.fromValue(
                equalsOperator.value(),
                equalsOperator.path(),
                singleQueryContext.getEmbeddedRoot());
        case ExistsOperator existsOperator ->
            this.existsQueryFactory.fromExistsOperator(existsOperator, singleQueryContext);
        case GeoShapeOperator geoShapeOperator ->
            this.geoQueryFactory.geoShape(geoShapeOperator, singleQueryContext);
        case GeoWithinOperator geoWithinOperator ->
            this.geoQueryFactory.geoWithin(geoWithinOperator, singleQueryContext);
        case HasAncestorOperator hasAncestorOperator ->
            this.hasAncestorQueryFactory.fromHasAncestor(
                hasAncestorOperator, singleQueryContext, this);
        case HasRootOperator hasRootOperator ->
            this.hasRootQueryFactory.fromHasRoot(hasRootOperator, singleQueryContext, this);
        case InOperator inOperator ->
            // delegating to equals under these conditions achieves better performance
            (inOperator.values().size() == 1
                    && (inOperator.getValueType() == NonNullValueType.STRING
                        || inOperator.getValueType() == NonNullValueType.UUID
                        || inOperator.getValueType() == NonNullValueType.OBJECT_ID))
                ? this.equalsQueryFactory.fromValue(
                    inOperator.values().getFirst(),
                    inOperator.paths().get(0),
                    singleQueryContext.getEmbeddedRoot())
                : this.inQueryFactory.fromValues(
                    inOperator.values(),
                    inOperator.getValueType(),
                    inOperator.paths(),
                    singleQueryContext);

        case VectorSearchOperator vectorSearchOperator ->
            this.vectorSearchQueryFactory.fromQuery(
                vectorSearchOperator.criteria(), singleQueryContext);
        case KnnBetaOperator knnBetaOperator -> {
          Optional<Query> filter =
              knnBetaOperator.filter().isPresent()
                  ? Optional.of(
                      this.vectorSearchFilterQueryFactory.createLuceneFilter(
                          new VectorSearchFilter.OperatorFilter(knnBetaOperator.filter().get()),
                          singleQueryContext))
                  : Optional.empty();
          yield this.knnQueryFactory.fromOperator(knnBetaOperator, filter, singleQueryContext);
        }
        case MoreLikeThisOperator moreLikeThisOperator ->
            this.moreLikeThisQueryFactory.fromOperator(moreLikeThisOperator, singleQueryContext);
        case NearOperator nearOperator ->
            this.nearQueryFactory.fromOperator(nearOperator, singleQueryContext);
        case PhraseOperator phraseOperator ->
            this.phraseQueryFactory.fromPhrase(phraseOperator, singleQueryContext);
        case QueryStringOperator queryStringOperator ->
            this.searchQueryFactory.fromQueryString(queryStringOperator, singleQueryContext);
        case RangeOperator rangeOperator ->
            this.rangeQueryFactory.fromOperator(rangeOperator, singleQueryContext);
        case RegexOperator regexOperator ->
            this.termLevelQueryFactory.fromRegex(regexOperator, singleQueryContext);
        case SearchOperator searchOperator ->
            this.searchQueryFactory.fromSearch(searchOperator, singleQueryContext);
        case SpanOperator spanOperator ->
            this.spanQueryFactory.fromSpan(spanOperator, singleQueryContext);
        case TermOperator termOperator ->
            this.termQueryFactory.fromTerm(termOperator, singleQueryContext);
        case TextOperator textOperator ->
            this.textQueryFactory.fromText(textOperator, singleQueryContext);
        case WildcardOperator wildcardOperator ->
            this.termLevelQueryFactory.fromWildcard(wildcardOperator, singleQueryContext);
      };
    } catch (InvalidQueryException e) {
      if (e.getType() == Type.LENIENT) {
        // in case of a lenient error, we replace the query with the one which always returns
        // empty result and track statistics to know when we can switch to the strict mode:
        // CLOUDP-125626
        this.queryFactoryContext.getMetrics().getLenientFailureCounter().increment();
        return new MatchNoDocsQuery();
      }
      throw e;
    } catch (IndexSearcher.TooManyClauses e) {
      throw new InvalidQueryException(
          String.format(
              "query has expanded into too many sub-queries internally: %s", e.getMessage()));
    } catch (TooComplexToDeterminizeException e) {
      // check for null since the exception is created by Lucene and not mongot
      throw new InvalidQueryException(
          String.format(
              "Determinizing the automaton would require too much work: %s",
              e.getMessage() != null ? e.getMessage() : ""));
    }
  }

  private void validateReturnScope(Optional<ReturnScope> returnScope) throws InvalidQueryException {
    if (returnScope.isPresent()) {
      InvalidQueryException.validate(
          this.queryFactoryContext
              .getFeatureFlags()
              .isEnabled(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES),
          "returnScope is not currently supported on this cluster");
      Optional<EmbeddedDocumentsFieldDefinition> embeddedDocumentsFieldDefinition =
          this.queryFactoryContext.getEmbeddedDocumentsFieldDefinition(returnScope.get().path());
      // TODO(CLOUDP-320167) Handle searchMeta special case to omit documents
      InvalidQueryException.validate(
          embeddedDocumentsFieldDefinition
              .flatMap(EmbeddedDocumentsFieldDefinition::storedSourceDefinition)
              .map(storedSourceDefinition -> !storedSourceDefinition.isAllExcluded())
              .orElse(false),
          "returnScope path '%s' must be indexed as "
              + "embeddedDocument with a non-empty storedSource definition",
          returnScope.get().path());
    }
  }
}
