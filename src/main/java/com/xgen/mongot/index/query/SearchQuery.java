package com.xgen.mongot.index.query;

import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.OperatorEmbeddedRootValidator;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortBetaV1;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public sealed interface SearchQuery extends Query permits CollectorQuery, OperatorQuery {

  class Fields {
    public static final Field.WithDefault<Count> COUNT =
        Field.builder("count")
            .classField(Count::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(Count.DEFAULT);

    public static final Field.Optional<UnresolvedHighlight> HIGHLIGHT =
        Field.builder("highlight")
            .classField(UnresolvedHighlight::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.WithDefault<Boolean> STORED_SOURCE =
        Field.builder("returnStoredSource").booleanField().optional().withDefault(false);

    public static final Field.Optional<ReturnScope> RETURN_SCOPE =
        Field.builder("returnScope")
            .classField(ReturnScope::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.WithDefault<Boolean> SCORE_DETAILS =
        Field.builder("scoreDetails").booleanField().optional().withDefault(false);

    /** Reverses the sort order and returns documents appearing before the specified token. */
    public static final Field.Optional<SequenceToken> SEARCH_BEFORE =
        Field.builder("searchBefore").classField(SequenceToken::fromBson).optional().noDefault();

    /** Return only results ordered after the specified sequence token. */
    public static final Field.Optional<SequenceToken> SEARCH_AFTER =
        Field.builder("searchAfter").classField(SequenceToken::fromBson).optional().noDefault();

    public static final Field.Optional<SortSpec> SORT_BETA_V1 =
        Field.builder("sortBetaV1").classField(SortBetaV1::fromBson).optional().noDefault();

    public static final Field.Optional<SortSpec> SORT =
        Field.builder("sort").classField(Sort::fromBson).optional().noDefault();

    public static final Field.Optional<Tracking> TRACKING =
        Field.builder("tracking")
            .classField(Tracking::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.WithDefault<Boolean> CONCURRENT =
        Field.builder("concurrent").booleanField().optional().withDefault(false);
  }

  static SearchQuery fromBson(BsonDocument document) throws BsonParseException {
    return fromBson(document, QueryOptimizationFlags.DEFAULT_OPTIONS);
  }

  static SearchQuery fromBson(BsonDocument document, QueryOptimizationFlags queryOptimizationFlags)
      throws BsonParseException {
    return fromBson(document, queryOptimizationFlags, false);
  }

  private static SearchQuery fromBson(
      DocumentParser parser, QueryOptimizationFlags queryOptimizationFlags, boolean allow10k)
      throws BsonParseException {
    var operator = Operator.atMostOneFromBson(parser);
    var collector = Collector.atMostOneFromBson(parser, allow10k);
    if (operator.isPresent() && collector.isPresent()) {
      parser
          .getContext()
          .handleSemanticError("Top-level operator is not allowed to be used with collector");
    }

    Optional<SortSpec> sortSpec =
        parser
            .getGroup()
            .atMostOneOf(parser.getField(Fields.SORT_BETA_V1), parser.getField(Fields.SORT));

    var searchBefore = parser.getField(Fields.SEARCH_BEFORE);
    var searchAfter = parser.getField(Fields.SEARCH_AFTER);
    var sequenceToken = parser.getGroup().atMostOneOf(searchAfter, searchBefore);
    var returnScope = parser.getField(Fields.RETURN_SCOPE);
    var returnStoredSource = parser.getField(Fields.STORED_SOURCE);
    // Don't care about the value, just want to validate deserialization
    parser.getField(Query.Fields.SEARCH_NODE_PREFERENCE).unwrap();

    var pagination =
        sequenceToken.map(
            token ->
                searchAfter.unwrap().isPresent()
                    ? new Pagination(token, Pagination.Type.SEARCH_AFTER)
                    : new Pagination(token, Pagination.Type.SEARCH_BEFORE));

    validateReturnScopeAndStoredSource(
        parser, returnScope.unwrap(), returnStoredSource.unwrap(), queryOptimizationFlags);

    if (operator.isPresent()) {
      validateKnnBetaOperator(operator.get(), parser);
      validateVectorSearchOperator(operator.get(), parser);

      OperatorEmbeddedRootValidator validator =
          new OperatorEmbeddedRootValidator(parser.getContext());
      validator.validate(operator.get(), returnScope.unwrap().map(ReturnScope::path));

      return new OperatorQuery(
          operator.get(),
          parser.getField(Query.Fields.INDEX).unwrap(),
          parser.getField(Fields.COUNT).unwrap(),
          parser.getField(Fields.HIGHLIGHT).unwrap(),
          pagination,
          returnStoredSource.unwrap(),
          parser.getField(Fields.SCORE_DETAILS).unwrap(),
          parser.getField(Fields.CONCURRENT).unwrap(),
          sortSpec,
          parser.getField(Fields.TRACKING).unwrap(),
          returnScope.unwrap());
    }

    if (collector.isPresent()) {
      if (collector.get().getOperator().isPresent()) {
        OperatorEmbeddedRootValidator validator =
            new OperatorEmbeddedRootValidator(parser.getContext());
        validator.validate(
            collector.get().getOperator().get(), returnScope.unwrap().map(ReturnScope::path));
      }
      return new CollectorQuery(
          collector.get(),
          parser.getField(Query.Fields.INDEX).unwrap(),
          parser.getField(Fields.COUNT).unwrap(),
          parser.getField(Fields.HIGHLIGHT).unwrap(),
          pagination,
          returnStoredSource.unwrap(),
          parser.getField(Fields.SCORE_DETAILS).unwrap(),
          parser.getField(Fields.CONCURRENT).unwrap(),
          sortSpec,
          parser.getField(Fields.TRACKING).unwrap(),
          returnScope.unwrap());
    }
    String errorDescription =
        String.format(
            "Query should contain either an operator [%s] " + "or a collector [%s]",
            Operator.ALL_OPERATORS, Collector.ALL_COLLECTORS);
    return parser.getContext().handleSemanticError(errorDescription);
  }

  static SearchQuery fromBson(
      BsonDocument document, QueryOptimizationFlags queryOptimizationFlags, boolean allow10k)
      throws BsonParseException {
    try (var guard = Tracing.simpleSpanGuard("Query.fromBson")) {
      try (var parser = BsonDocumentParser.fromRoot(document).build()) {
        return fromBson(parser, queryOptimizationFlags, allow10k);
      }
    }
  }

  Count count();

  Optional<UnresolvedHighlight> highlight();

  boolean scoreDetails();

  /**
   * Returns the final sort criteria for the query, or {@link Optional#empty()} if we should use the
   * default Lucene sort. <br>
   * <br>
   *
   * <p>"Final" here means after considering the presence of a "searchBefore" token which
   * effectively inverts the sort order. It includes any implicit SortFields.
   */
  default Optional<SortSpec> sortSpec() {
    if (this.pagination().isPresent()
        && Pagination.Type.SEARCH_BEFORE == this.pagination().get().type()) {
      return this.rawSortSpec().map(SortSpec::invert).or(() -> Optional.of(Sort.REVERSE_RELEVANCE));
    }
    return this.rawSortSpec();
  }

  /**
   * Returns the {@link SortSpec} exactly as entered by the user.
   *
   * <p>This sort spec does not consider any implicit sort fields added by pagination. As a result,
   * it's probably the wrong method to use for any use case other than copying a Query object. See
   * {@link #sortSpec()}
   */
  Optional<SortSpec> rawSortSpec();

  Optional<Pagination> pagination();

  Optional<Tracking> tracking();

  @Override
  Optional<ReturnScope> returnScope();

  @Override
  default BsonDocument toBson() {
    var documentBuilder =
        BsonDocumentBuilder.builder()
            .field(Query.Fields.INDEX, this.index())
            .field(Fields.COUNT, this.count())
            .field(Fields.HIGHLIGHT, this.highlight())
            .field(Fields.STORED_SOURCE, this.returnStoredSource())
            .field(Fields.SCORE_DETAILS, this.scoreDetails())
            .field(Fields.CONCURRENT, this.concurrent())
            .field(Fields.TRACKING, this.tracking())
            .field(Fields.RETURN_SCOPE, this.returnScope());

    if (this.rawSortSpec().isPresent()) {
      var sortField = getSortBsonField(this.rawSortSpec().get());
      documentBuilder.field(sortField, this.rawSortSpec());
    }

    var document = documentBuilder.build();

    document.putAll(this.queryToBson());

    return document;
  }

  private static Field.Optional<SortSpec> getSortBsonField(SortSpec sortSpec) {
    return switch (sortSpec) {
      case Sort sort -> Fields.SORT;
      case SortBetaV1 sortBetaV1 -> Fields.SORT_BETA_V1;
    };
  }

  BsonDocument queryToBson();

  private static void validateKnnBetaOperator(Operator operator, DocumentParser parser)
      throws BsonParseException {
    if (operator.getType() != Operator.Type.KNN_BETA) {
      return;
    }

    if (parser.getField(Fields.SORT_BETA_V1).unwrap().isPresent()) {
      parser
          .getContext()
          .handleSemanticError("knnBeta is not allowed to be used with the sort option");
    }
  }

  private static void validateVectorSearchOperator(Operator operator, DocumentParser parser)
      throws BsonParseException {
    if (operator.getType() != Operator.Type.VECTOR_SEARCH) {
      return;
    }

    BsonParseContext context = parser.getContext();
    if (parser.getField(Fields.SORT_BETA_V1).unwrap().isPresent()
        || parser.getField(Fields.SORT).unwrap().isPresent()) {
      context.handleSemanticError(
          "Sort option is not supported with the 'vectorSearch' operator. "
              + "Use $sort stage instead.");
    }

    if (parser.getField(Fields.SEARCH_BEFORE).unwrap().isPresent()
        || parser.getField(Fields.SEARCH_AFTER).unwrap().isPresent()) {
      context.handleSemanticError(
          "Pagination is not supported with the 'vectorSearch' operator. "
              + "Use $skip and $limit stages instead.");
    }

    if (parser.getField(Fields.HIGHLIGHT).unwrap().isPresent()) {
      context.handleSemanticError(
          "Highlighting is not supported with the 'vectorSearch' operator.");
    }

    if (parser.getField(Fields.TRACKING).unwrap().isPresent()) {
      context.handleSemanticError(
          "Term tracking is not supported with the 'vectorSearch' operator.");
    }
  }

  private static void validateReturnScopeAndStoredSource(
      DocumentParser parser,
      Optional<ReturnScope> returnScope,
      boolean returnStoredSource,
      QueryOptimizationFlags queryOptimizationFlags)
      throws BsonParseException {
    if (!queryOptimizationFlags.omitSearchDocumentResults()
        && returnScope.isPresent()
        && !returnStoredSource) {
      parser
          .getContext()
          .handleSemanticError(
              String.format(
                  "Set %s to true if %s is non-empty",
                  Fields.STORED_SOURCE.getName(), Fields.RETURN_SCOPE.getName()));
    }
  }
}
