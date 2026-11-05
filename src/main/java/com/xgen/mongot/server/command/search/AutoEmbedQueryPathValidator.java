package com.xgen.mongot.server.command.search;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationFacetDefinition;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.MetricSpec;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
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
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Rejects $search queries that read an auto-embed source path anywhere other than the path of a
 * vectorSearch operator. The materialized view consumes the source text of a path mapped only as
 * autoEmbed, so the served index holds no lexical, numeric, or field-name data for it: without this
 * check such queries silently match nothing, or everything when negated.
 *
 * <p>A path stays queryable when the served index also maps it with another type, for example
 * {@code [{type: "string"}, {type: "autoEmbed", ...}]}, or when the served index does not embed it
 * at all. The served definition decides this, not the catalog definition, because the two can
 * differ while a definition change is being built.
 */
final class AutoEmbedQueryPathValidator {

  /** Reserved for materialized-view fields; index definitions cannot map paths under it. */
  private static final FieldPath RESERVED_AUTO_EMBED_ROOT = FieldPath.newRoot("_autoEmbed");

  // A served definition never changes, so its materialized-view mapping is built once. Weak keys
  // compare by identity and let the entry of a replaced definition be collected.
  private static final Cache<SearchIndexDefinition, SearchIndexAutoEmbedFieldMapping>
      MATERIALIZED_VIEW_MAPPINGS = Caffeine.newBuilder().weakKeys().build();

  private AutoEmbedQueryPathValidator() {}

  /**
   * Validates that {@code query} reads auto-embed source paths only where the served index can
   * answer.
   *
   * @param autoEmbedSourcePaths user-facing paths declared as autoEmbed, each mapped to the
   *     materialized-view path of its vector
   * @param servedDefinition the definition of the index generation executing the query
   * @param omitSearchDocumentResults whether the query returns no documents, as for $searchMeta, in
   *     which case highlighting and a sort that orders only documents are never built
   * @throws InvalidQueryException if the query reads a path under the reserved {@code _autoEmbed}
   *     namespace, or a source path the served index embeds and does not map with another type
   */
  static void validate(
      SearchQuery query,
      Map<FieldPath, FieldPath> autoEmbedSourcePaths,
      SearchIndexDefinition servedDefinition,
      boolean omitSearchDocumentResults)
      throws InvalidQueryException {
    List<FieldPath> queriedPaths = queriedPaths(query, omitSearchDocumentResults);
    for (FieldPath path : queriedPaths) {
      validateNotReservedAutoEmbedPath(path);
    }
    Set<FieldPath> embedOnlyPaths =
        autoEmbedSourcePaths.entrySet().stream()
            .filter(entry -> isEmbedded(servedDefinition, entry.getKey(), entry.getValue()))
            .map(Map.Entry::getKey)
            .filter(path -> !isServedWithOtherType(servedDefinition, path))
            .collect(Collectors.toSet());
    if (embedOnlyPaths.isEmpty()) {
      return;
    }
    Set<FieldPath> emptiedDocumentPaths =
        emptiedDocumentPaths(embedOnlyPaths, queriedPaths, servedDefinition);
    for (FieldPath path : queriedPaths) {
      if (embedOnlyPaths.contains(path)) {
        throw new InvalidQueryException(
            String.format(
                "Path '%s' is a type:autoEmbed field and can only be queried with the"
                    + " vectorSearch operator. To use it in other operators, filters, sort,"
                    + " highlight, or facets, also index it with another type, such as string.",
                path));
      }
      if (emptiedDocumentPaths.contains(path)) {
        throw new InvalidQueryException(
            String.format(
                "Path '%s' only contains type:autoEmbed fields, which can only be queried with"
                    + " the vectorSearch operator. To query it, also index one of its fields with"
                    + " another type, such as string.",
                path));
      }
    }
  }

  private static void validateNotReservedAutoEmbedPath(FieldPath path)
      throws InvalidQueryException {
    if (path.equals(RESERVED_AUTO_EMBED_ROOT) || path.isChildOf(RESERVED_AUTO_EMBED_ROOT)) {
      throw new InvalidQueryException(
          String.format(
              "Path '%s' is reserved for internal automated embeddings fields. Query the"
                  + " field by its source path instead.",
              path));
    }
  }

  /**
   * Returns the documents that hold auto-embed-only fields and nothing the materialized view
   * copies, such as a document whose only other field is an empty static document. The materialized
   * view omits such a document, so it has no field-name data either. Under a dynamic root mapping
   * the document may still be indexed dynamically, so none are returned. Only documents the query
   * reads are considered, so the mapping is built only for queries that read one.
   */
  private static Set<FieldPath> emptiedDocumentPaths(
      Set<FieldPath> embedOnlyPaths,
      List<FieldPath> queriedPaths,
      SearchIndexDefinition servedDefinition) {
    if (servedDefinition.getMappings().dynamic().isEnabled()) {
      return Set.of();
    }
    Set<FieldPath> queriedAncestors =
        embedOnlyPaths.stream()
            .flatMap(FieldPath::ancestorPaths)
            .filter(queriedPaths::contains)
            .collect(Collectors.toSet());
    if (queriedAncestors.isEmpty()) {
      return Set.of();
    }
    SearchIndexAutoEmbedFieldMapping materializedViewMapping =
        MATERIALIZED_VIEW_MAPPINGS.get(
            servedDefinition, AutoEmbedFieldMappingCreator::createAutoEmbedMapping);
    return queriedAncestors.stream()
        .filter(path -> !materializedViewMapping.isPassthrough(path))
        .collect(Collectors.toSet());
  }

  /**
   * Whether the served index embeds {@code path}: a derived definition holds its vector at the
   * materialized-view path, and a source definition declares it autoEmbed. A path the catalog
   * declares autoEmbed but the served generation does not embed is still indexed as before.
   */
  private static boolean isEmbedded(
      SearchIndexDefinition servedDefinition, FieldPath path, FieldPath materializedViewPath) {
    return servedDefinition
            .getStaticFieldDefinition(materializedViewPath)
            .flatMap(FieldDefinition::searchIndexVectorFieldDefinition)
            .isPresent()
        || servedDefinition
            .getStaticFieldDefinition(path)
            .flatMap(FieldDefinition::searchAutoEmbedFieldDefinition)
            .isPresent();
  }

  private static boolean isServedWithOtherType(
      SearchIndexDefinition servedDefinition, FieldPath path) {
    return servedDefinition
        .getStaticFieldDefinition(path)
        .map(
            definition ->
                definition.hasFieldDefinitionsOtherThan(FieldTypeDefinition.Type.AUTO_EMBED))
        .orElse(false);
  }

  /** Returns every field path {@code query} reads, except the paths of vectorSearch operators. */
  static List<FieldPath> queriedPaths(SearchQuery query) {
    return queriedPaths(query, false);
  }

  private static List<FieldPath> queriedPaths(
      SearchQuery query, boolean omitSearchDocumentResults) {
    List<FieldPath> paths = new ArrayList<>();
    switch (query) {
      case OperatorQuery operatorQuery -> collectOperatorPaths(operatorQuery.operator(), paths);
      case CollectorQuery collectorQuery ->
          collectCollectorPaths(collectorQuery.collector(), paths);
    }
    // Without documents to return, query execution skips highlighting and builds the sort only for
    // first/last metrics.
    if (!omitSearchDocumentResults) {
      query.highlight().map(UnresolvedHighlight::paths).ifPresent(p -> addUnresolved(p, paths));
    }
    if (!omitSearchDocumentResults || query.sortDrivesAggregation()) {
      query.rawSortSpec().ifPresent(sort -> collectSortPaths(sort, paths));
    }
    return paths;
  }

  private static void collectCollectorPaths(Collector collector, List<FieldPath> paths) {
    switch (collector) {
      case FacetCollector facetCollector -> {
        collectOperatorPaths(facetCollector.operator(), paths);
        for (FacetDefinition facet : facetCollector.facetDefinitions().values()) {
          paths.add(FieldPath.parse(facet.path()));
          facet.embeddedDocument().ifPresent(scope -> collectScopePaths(scope, paths));
        }
      }
      case AggregationCollector aggregationCollector -> {
        collectOperatorPaths(aggregationCollector.operator(), paths);
        aggregationCollector
            .facets()
            .values()
            .forEach(facet -> collectAggregationFacetPaths(facet, paths));
        aggregationCollector.sortSpec().ifPresent(sort -> collectSortPaths(sort, paths));
      }
    }
  }

  private static void collectAggregationFacetPaths(
      AggregationFacetDefinition facet, List<FieldPath> paths) {
    paths.add(FieldPath.parse(facet.group().path()));
    paths.add(facet.path());
    facet.aggregations().values().stream()
        .map(MetricSpec::path)
        .flatMap(Optional::stream)
        .forEach(paths::add);
    facet.facets().values().forEach(child -> collectAggregationFacetPaths(child, paths));
  }

  private static void collectSortPaths(SortSpec sort, List<FieldPath> paths) {
    for (MongotSortField field : sort.getSortFields()) {
      // A $meta sort key is only a label; it reads no field.
      if (field.options() instanceof UserFieldSortOptions options) {
        paths.add(field.field());
        options.embeddedScope().ifPresent(scope -> collectSortScopePaths(scope, paths));
      }
    }
  }

  /**
   * Sort options keep the scope operator as raw BSON, so parse it the way query execution does. A
   * scope that does not parse is left for query execution to reject.
   */
  private static void collectSortScopePaths(EmbeddedSortScope scope, List<FieldPath> paths) {
    try (var parser = BsonDocumentParser.fromRoot(scope.toBson()).build()) {
      collectScopePaths(EmbeddedDocumentScope.fromBson(parser), paths);
    } catch (BsonParseException e) {
      paths.add(scope.path());
    }
  }

  private static void collectScopePaths(EmbeddedDocumentScope scope, List<FieldPath> paths) {
    paths.add(scope.path());
    collectOperatorPaths(scope.operator(), paths);
  }

  /**
   * Adds every path {@code operator} and its children read. The switch has no default, so a new
   * {@link Operator} type does not compile until it gets a case here; when adding a path-bearing
   * field to an existing operator, add it to that operator's case too.
   */
  private static void collectOperatorPaths(Operator operator, List<FieldPath> paths) {
    paths.addAll(operator.score().getChildPaths());
    switch (operator) {
      case AllDocumentsOperator allDocumentsOperator -> {
        // No path.
      }
      case AutocompleteOperator autocompleteOperator -> paths.add(autocompleteOperator.path());
      case CompoundOperator compoundOperator ->
          compoundOperator.getOperators().forEach(child -> collectOperatorPaths(child, paths));
      case EmbeddedDocumentOperator embeddedDocumentOperator -> {
        paths.add(embeddedDocumentOperator.path());
        collectOperatorPaths(embeddedDocumentOperator.operator(), paths);
      }
      case EqualsOperator equalsOperator -> paths.add(equalsOperator.path());
      case ExistsOperator existsOperator -> paths.add(FieldPath.parse(existsOperator.path()));
      case GeoShapeOperator geoShapeOperator -> paths.addAll(geoShapeOperator.paths());
      case GeoWithinOperator geoWithinOperator -> paths.addAll(geoWithinOperator.paths());
      case HasAncestorOperator hasAncestorOperator ->
          collectOperatorPaths(hasAncestorOperator.operator(), paths);
      case HasRootOperator hasRootOperator ->
          collectOperatorPaths(hasRootOperator.operator(), paths);
      case InOperator inOperator -> paths.addAll(inOperator.paths());
      case KnnBetaOperator knnBetaOperator -> {
        paths.addAll(knnBetaOperator.paths());
        knnBetaOperator.filter().ifPresent(filter -> collectOperatorPaths(filter, paths));
      }
      case MoreLikeThisOperator moreLikeThisOperator ->
          moreLikeThisOperator.like().forEach(like -> collectDocumentPaths(like, "", paths));
      case NearOperator nearOperator -> paths.addAll(nearOperator.paths());
      case PhraseOperator phraseOperator -> addUnresolved(phraseOperator.paths(), paths);
      case QueryStringOperator queryStringOperator ->
          paths.addAll(
              queryStringFieldPaths(
                  queryStringOperator.query(), queryStringOperator.defaultPath()));
      case RangeOperator rangeOperator -> paths.addAll(rangeOperator.paths());
      case SearchOperator searchOperator ->
          paths.addAll(StringPath.toFieldPaths(searchOperator.paths()));
      case SpanOperator spanOperator ->
          paths.addAll(StringPath.toFieldPaths(spanOperator.getPaths()));
      case TermLevelOperator termLevelOperator -> addUnresolved(termLevelOperator.paths(), paths);
      case TermOperator termOperator -> paths.addAll(StringPath.toFieldPaths(termOperator.paths()));
      case TextOperator textOperator -> addUnresolved(textOperator.paths(), paths);
      case VectorSearchOperator vectorSearchOperator -> {
        // The criteria path is the auto-embed target itself; only its filters read other data.
        collectFilterPaths(vectorSearchOperator.criteria().filter(), paths);
        collectFilterPaths(vectorSearchOperator.criteria().parentFilter(), paths);
      }
    }
  }

  private static void collectFilterPaths(
      Optional<VectorSearchFilter> filter, List<FieldPath> paths) {
    // $search vectorSearch filters are search operators; MQL clause filters belong to the
    // $vectorSearch stage, which rejects auto-embed filter paths itself.
    if (filter.isPresent()
        && filter.get() instanceof VectorSearchFilter.OperatorFilter operatorFilter) {
      collectOperatorPaths(operatorFilter.operator(), paths);
    }
  }

  /**
   * Returns the fields a queryString query reads, such as {@code description} in {@code
   * description:laptop}. The default path counts only when an unqualified clause reads it, so
   * {@code *:*} and fully field-qualified queries skip it. A query that does not parse returns only
   * the default path and is left for query execution to reject.
   */
  private static List<FieldPath> queryStringFieldPaths(String query, String defaultPath) {
    FieldRecordingQueryParser parser = new FieldRecordingQueryParser(defaultPath);
    try {
      parser.parse(query);
    } catch (ParseException e) {
      return List.of(FieldPath.parse(defaultPath));
    }
    // "$"-prefixed fields are rejected at execution.
    return parser.fields.stream()
        .filter(field -> !field.startsWith("$"))
        .map(FieldPath::parse)
        .toList();
  }

  /**
   * Records the field of every clause the classic query parser builds; these are the same hooks
   * through which query execution resolves queryString fields.
   */
  private static final class FieldRecordingQueryParser extends QueryParser {
    // Clauses are recorded, not built: building them can throw on input, such as an invalid
    // regular expression, that query execution reports as an invalid query.
    private static final Query RECORDED_CLAUSE = new MatchNoDocsQuery();

    private final Set<String> fields = new LinkedHashSet<>();

    FieldRecordingQueryParser(String defaultField) {
      super(defaultField, new StandardAnalyzer());
      setAllowLeadingWildcard(true);
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, boolean quoted) {
      this.fields.add(field);
      return RECORDED_CLAUSE;
    }

    @Override
    protected Query getRangeQuery(
        String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
      this.fields.add(field);
      return RECORDED_CLAUSE;
    }

    @Override
    protected Query getWildcardQuery(String field, String termStr) {
      // Query execution reads "*:*" as match-all; any other "*" clause names a field called "*".
      if (!field.equals("*") || !termStr.equals("*")) {
        this.fields.add(field);
      }
      return RECORDED_CLAUSE;
    }

    @Override
    protected Query getPrefixQuery(String field, String termStr) {
      this.fields.add(field);
      return RECORDED_CLAUSE;
    }

    @Override
    protected Query getRegexpQuery(String field, String termStr) {
      this.fields.add(field);
      return RECORDED_CLAUSE;
    }

    @Override
    protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) {
      this.fields.add(field);
      return RECORDED_CLAUSE;
    }
  }

  private static void collectDocumentPaths(
      BsonDocument document, String prefix, List<FieldPath> paths) {
    for (var entry : document.entrySet()) {
      collectValuePaths(entry.getValue(), prefix + entry.getKey(), paths);
    }
  }

  /**
   * Mirrors moreLikeThis execution, which searches only string values, and searches array elements
   * under the array's own path.
   */
  private static void collectValuePaths(BsonValue value, String path, List<FieldPath> paths) {
    if (value.isString()) {
      paths.add(FieldPath.parse(path));
    } else if (value.isDocument()) {
      collectDocumentPaths(value.asDocument(), path + ".", paths);
    } else if (value.isArray()) {
      value.asArray().forEach(element -> collectValuePaths(element, path, paths));
    }
  }

  private static void addUnresolved(List<UnresolvedStringPath> unresolved, List<FieldPath> paths) {
    // Wildcards expand at execution to string-indexed fields only, which never include an
    // autoEmbed-only field, so they name no path to check.
    paths.addAll(
        UnresolvedStringPath.toFieldPaths(
            unresolved.stream()
                .filter(path -> !(path instanceof UnresolvedStringWildcardPath))
                .toList()));
  }
}
