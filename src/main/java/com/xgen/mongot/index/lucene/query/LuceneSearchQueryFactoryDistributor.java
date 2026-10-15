package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.query.InvalidQueryException.Type;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.analyzer.wrapper.QueryAnalyzerWrapper;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.index.lucene.query.sort.EmbeddedScopedDateSort;
import com.xgen.mongot.index.lucene.query.sort.EmbeddedScopedDoubleSort;
import com.xgen.mongot.index.lucene.query.sort.EmbeddedScopedLongSort;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.query.sort.MqlSortedSetSortField;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentScope;
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
import com.xgen.mongot.index.query.operators.OperatorEmbeddedRootValidator;
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
import com.xgen.mongot.index.query.optimizer.QueryOptimizer;
import com.xgen.mongot.index.query.optimizer.rules.DefaultRuleRegistry;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.bson.BsonDocument;

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
  private final QueryOptimizer queryOptimizer;

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
      SearchQueryFactoryContext queryFactoryContext,
      QueryOptimizer queryOptimizer,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.allDocsQueryFactory = allDocsQueryFactory;
    this.autocompleteQueryFactory = autocompleteQueryFactory;
    this.embeddedDocumentQueryFactory = embeddedDocumentQueryFactory;
    this.existsQueryFactory = existsQueryFactory;
    this.compoundQueryFactory =
        new CompoundQueryFactory(
            queryFactoryContext,
            this::createQuery,
            this::createSearchExplainQuery,
            dynamicFeatureFlagRegistry);
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
            queryOptimizer,
            this::createQuery);
    this.vectorSearchQueryFactory =
        new VectorSearchQueryFactory(
            queryFactoryContext, this.vectorSearchFilterQueryFactory, dynamicFeatureFlagRegistry);
    this.luceneQueryScorer = luceneQueryScorer;
    this.luceneSortFactory = luceneSortFactory;
    this.queryFactoryContext = queryFactoryContext;
    this.queryOptimizer = queryOptimizer;
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
    return create(
        indexDefinition,
        indexFormatVersion,
        analyzerRegistry,
        synonymRegistry,
        queryingMetricsUpdater,
        enableTextOperatorNewSynonymsSyntax,
        featureFlags,
        DynamicFeatureFlagRegistry.empty());
  }

  public static LuceneSearchQueryFactoryDistributor create(
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      SynonymRegistry synonymRegistry,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      boolean enableTextOperatorNewSynonymsSyntax,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {

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
    DateRangeQueryFactory dateRangeQueryFactory =
        new DateRangeQueryFactory(queryFactoryContext, dynamicFeatureFlagRegistry);
    NumericRangeQueryFactory numericRangeQueryFactory =
        new NumericRangeQueryFactory(queryFactoryContext, dynamicFeatureFlagRegistry);
    EqualsQueryFactory equalsQueryFactory =
        new EqualsQueryFactory(
            queryFactoryContext, dateRangeQueryFactory, numericRangeQueryFactory);
    ExistsQueryFactory existsQueryFactory = new ExistsQueryFactory(queryFactoryContext);
    RangeQueryFactory rangeQueryFactory =
        new RangeQueryFactory(
            queryFactoryContext,
            equalsQueryFactory,
            dateRangeQueryFactory,
            numericRangeQueryFactory);
    InQueryFactory inQueryFactory =
        new InQueryFactory(queryFactoryContext, dynamicFeatureFlagRegistry);

    AllDocsQueryFactory allDocsQueryFactory = new AllDocsQueryFactory(queryFactoryContext);
    QueryOptimizer queryOptimizer =
        new QueryOptimizer(
            DefaultRuleRegistry.get(), queryFactoryContext, dynamicFeatureFlagRegistry);
    return new LuceneSearchQueryFactoryDistributor(
        allDocsQueryFactory,
        new AutocompleteQueryFactory(queryFactoryContext),
        new EmbeddedDocumentQueryFactory(queryFactoryContext, dynamicFeatureFlagRegistry),
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
        queryFactoryContext,
        queryOptimizer,
        dynamicFeatureFlagRegistry);
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
    Operator optimized = this.queryOptimizer.optimize(operator).tree();
    var scoredQuery =
        createQuery(
            optimized, SingleQueryContext.createQueryRootWithReturnScope(indexReader, returnScope));
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

  /**
   * Builds a Lucene query that matches the <b>child</b> documents selected by an
   * embeddedDocument-scoped facet's {@code operator}, evaluated with {@code embeddedPath} as the
   * embedded root. Unlike a top-level embeddedDocument operator query, this does not join matches
   * up to their parents: the caller (an embedded-scoped facet counter) needs the matching child
   * docs themselves so it can group them by parent and deduplicate.
   */
  public Query createEmbeddedChildQuery(
      Operator operator, FieldPath embeddedPath, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return createQuery(
        operator, SingleQueryContext.createQueryRoot(indexReader).withEmbeddedRoot(embeddedPath));
  }

  /**
   * Returns a {@link BitSetProducer} identifying root (top-level) documents, used to group an
   * embedded-scoped facet's matching child docs up to their root for per-root deduplication.
   */
  public BitSetProducer rootParentBitSetProducer() {
    return new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY);
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
    Operator optimized = this.queryOptimizer.optimize(operator).tree();
    var explainQuery =
        createSearchExplainQuery(
            optimized,
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

  /**
   * The query's Lucene sort, or empty when it has none or does not need one.
   *
   * @param sortDrivesAggregation whether a {@code first}/{@code last} aggregation metric reads the
   *     sort, in which case it is built even when document results are omitted. See {@link
   *     com.xgen.mongot.index.query.SearchQuery#sortDrivesAggregation}.
   */
  public Optional<Sort> createSort(
      Optional<SortSpec> sortSpec,
      Optional<SequenceToken> sequenceToken,
      FieldToSortableTypesMapping fieldsToSortableTypesMapping,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<org.apache.lucene.search.Sort> indexSort,
      boolean sortDrivesAggregation,
      IndexReader reader,
      boolean scopedSortEnabled)
      throws InvalidQueryException, IOException {
    // A sort exists to order the returned documents, so $searchMeta normally has no use for one.
    // first/last are the exception: there the sort picks each bucket's winner, which is meta
    // output and has to be built either way (CLOUDP-425978).
    if (sortSpec.isEmpty()) {
      return Optional.empty();
    }

    Optional<FieldPath> embeddedRoot = returnScope.map(ReturnScope::path);
    Map<FieldPath, SortField> scopedOverrides =
        buildScopedSortOverrides(
            sortSpec.get(), reader, fieldsToSortableTypesMapping, scopedSortEnabled, embeddedRoot);
    if (queryOptimizationFlags.omitSearchDocumentResults() && !sortDrivesAggregation) {
      return Optional.empty();
    }
    this.luceneSortFactory.validateSortSpec(sortSpec.get(), embeddedRoot);
    return Optional.of(
        this.luceneSortFactory.createLuceneSort(
            sortSpec.get(),
            sequenceToken,
            fieldsToSortableTypesMapping,
            embeddedRoot,
            indexSort,
            scopedOverrides));
  }

  /**
   * Builds the block-join sort field for each embeddedDoc-scoped sort key, keyed by field path. The
   * scope operator is recovered from the raw {@code embeddedDocument} carried on the sort option
   * (kept unparsed so it survives serialization; see CLOUDP-441316), compiled to a child query, and
   * combined with the root parent bitset. Scoped sort is gated behind {@code
   * ENABLE_EMBEDDED_SCOPED_SORT} and supports the mixed engine's sortable BSON types.
   */
  private Map<FieldPath, SortField> buildScopedSortOverrides(
      SortSpec sortSpec,
      IndexReader reader,
      FieldToSortableTypesMapping fieldsToSortableTypesMapping,
      boolean scopedSortEnabled,
      Optional<FieldPath> returnScope)
      throws InvalidQueryException, IOException {
    Map<FieldPath, SortField> overrides = new HashMap<>();
    for (MongotSortField field : sortSpec.getSortFields()) {
      if (!(field.options() instanceof UserFieldSortOptions userOptions)
          || userOptions.embeddedScope().isEmpty()) {
        continue;
      }
      if (!scopedSortEnabled) {
        throw new InvalidQueryException(
            "embeddedDocument-scoped sort is not currently supported on this cluster");
      }
      InvalidQueryException.validate(
          returnScope.isEmpty(), "embeddedDocument-scoped sort cannot be used with returnScope");
      FieldPath scopePath = userOptions.embeddedScope().orElseThrow().path();
      InvalidQueryException.validate(
          this.queryFactoryContext.getEmbeddedDocumentsFieldDefinition(scopePath).isPresent(),
          "Sort scope '%s' must be indexed as embeddedDocuments",
          scopePath);
      InvalidQueryException.validate(
          this.queryFactoryContext.getIndexCapabilities().supportsEmbeddedNumericAndDateV2(),
          "embeddedDocument-scoped numeric sort requires index feature version 4 or later");
      var numericDefinition =
          this.queryFactoryContext.getNumericFieldDefinition(field.field(), Optional.of(scopePath));
      var dateDefinition =
          this.queryFactoryContext.getDateFieldDefinition(field.field(), Optional.of(scopePath));
      var tokenDefinition =
          this.queryFactoryContext.getTokenFieldDefinition(field.field(), Optional.of(scopePath));
      var booleanDefinition =
          this.queryFactoryContext.getBooleanFieldDefinition(field.field(), Optional.of(scopePath));
      var uuidDefinition =
          this.queryFactoryContext.getUuidFieldDefinition(field.field(), Optional.of(scopePath));
      var objectIdDefinition =
          this.queryFactoryContext.getObjectIdFieldDefinition(
              field.field(), Optional.of(scopePath));
      // A field declared as more than one sortable type sorts by MongoDB's cross-type bracket
      // order, via the mixed engine. Single number/date/token fields keep specialized engines.
      int declaredTypes =
          (numericDefinition.isPresent() ? 1 : 0)
              + (dateDefinition.isPresent() ? 1 : 0)
              + (tokenDefinition.isPresent() ? 1 : 0)
              + (booleanDefinition.isPresent() ? 1 : 0)
              + (uuidDefinition.isPresent() ? 1 : 0)
              + (objectIdDefinition.isPresent() ? 1 : 0);
      InvalidQueryException.validate(
          declaredTypes > 0,
          "embeddedDocument-scoped sort supports number, date, token, boolean, uuid, and objectId;"
              + " field '%s' is not indexed as sortable under scope '%s'",
          field.field(),
          scopePath);
      // Types without a specialized scoped engine also use the mixed comparator on their own.
      boolean multiType = declaredTypes > 1;
      boolean useMixed =
          multiType
              || booleanDefinition.isPresent()
              || uuidDefinition.isPresent()
              || objectIdDefinition.isPresent();
      Optional<FieldName.TypeField> sortType;
      if (multiType) {
        sortType = Optional.empty();
      } else if (numericDefinition.isPresent()) {
        sortType =
            Optional.of(
                switch (numericDefinition.orElseThrow().options().representation()) {
                  case INT64 -> FieldName.TypeField.NUMBER_INT64_V2;
                  case DOUBLE -> FieldName.TypeField.NUMBER_DOUBLE_V2;
                });
      } else if (dateDefinition.isPresent()) {
        sortType = Optional.of(FieldName.TypeField.DATE_V2);
      } else if (tokenDefinition.isPresent()) {
        sortType = Optional.of(FieldName.TypeField.TOKEN);
      } else if (booleanDefinition.isPresent()) {
        sortType = Optional.of(FieldName.TypeField.BOOLEAN);
      } else if (uuidDefinition.isPresent()) {
        sortType = Optional.of(FieldName.TypeField.UUID);
      } else {
        sortType = Optional.of(FieldName.TypeField.OBJECT_ID);
      }
      ImmutableSet<FieldName.TypeField> types =
          fieldsToSortableTypesMapping
              .getFieldToSortableTypes(Optional.of(scopePath))
              .get(field.field());
      // FieldInfos may be empty on a shard with no values. The schema still determines the
      // comparator. Nulls are missing values. A single-type field must not have indexed values of
      // some other type; a multi-type field is handled by the mixed engine's bracket ordering.
      InvalidQueryException.validate(
          multiType
              || types.stream()
                  .allMatch(
                      type -> type == sortType.orElseThrow() || type == FieldName.TypeField.NULL),
          "embeddedDocument-scoped sort does not support mixed sortable types"
              + " for field '%s' under scope '%s': %s",
          field.field(),
          scopePath,
          types);
      // Recover the scope operator from the raw operator BSON carried on the sort option (it
      // survives query serialization, unlike a query-level side channel) and compile it against
      // child docs (needs the reader) only after the cheap validations above pass. The sort option
      // keeps the operator unparsed to avoid a sort -> operators dependency cycle; reassemble the
      // {path, operator} block the operators-layer scope parser expects.
      EmbeddedSortScope sortScope = userOptions.embeddedScope().orElseThrow();
      EmbeddedDocumentScope scope = parseSortScope(field.field(), sortScope.toBson());
      Query childQuery = createEmbeddedChildQuery(scope.operator(), scope.path(), reader);
      BitSetProducer childFilter = new QueryBitSetProducer(childQuery);
      BitSetProducer parentFilter = rootParentBitSetProducer();
      SortField scopedSort;
      if (useMixed) {
        scopedSort = MqlMixedSort.scoped(field, scopePath, parentFilter, childFilter);
      } else {
        scopedSort =
            switch (sortType.orElseThrow()) {
              case NUMBER_INT64_V2 ->
                  new EmbeddedScopedLongSort(field, scopePath, parentFilter, childFilter);
              case NUMBER_DOUBLE_V2 ->
                  new EmbeddedScopedDoubleSort(field, scopePath, parentFilter, childFilter);
              case DATE_V2 ->
                  new EmbeddedScopedDateSort(field, scopePath, parentFilter, childFilter);
              default ->
                  MqlSortedSetSortField.scopedTokenSort(
                      field, scopePath, parentFilter, childFilter);
            };
      }
      overrides.put(field.field(), scopedSort);
    }
    return overrides;
  }

  /** Parses a scoped sort key and validates its operator against the scope's embedded root. */
  private static EmbeddedDocumentScope parseSortScope(
      FieldPath field, BsonDocument embeddedDocument) throws InvalidQueryException {
    try (var parser = BsonDocumentParser.fromRoot(embeddedDocument).build()) {
      EmbeddedDocumentScope scope = EmbeddedDocumentScope.fromBson(parser);
      new OperatorEmbeddedRootValidator(parser.getContext()).validate(scope);
      return scope;
    } catch (BsonParseException e) {
      throw new InvalidQueryException(
          String.format(
              "Could not parse embeddedDocument scope for sort field '%s': %s",
              field, e.getMessage()));
    }
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
                    && inOperator.paths().size() == 1
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
