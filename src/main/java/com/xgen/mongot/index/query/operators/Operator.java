package com.xgen.mongot.index.query.operators;

import static java.util.stream.Collectors.joining;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Extend Operator to create new Query Operators. Constructor validations should be limited to
 * unchecked development-related exceptions. Expected user validations should be placed into the
 * validate() method - these methods do not throw and will be presented to the user by the calling
 * application.
 */
public sealed interface Operator extends DocumentEncodable
    permits AllDocumentsOperator,
        AutocompleteOperator,
        CompoundOperator,
        EmbeddedDocumentOperator,
        EqualsOperator,
        ExistsOperator,
        GeoShapeOperator,
        GeoWithinOperator,
        HasAncestorOperator,
        HasRootOperator,
        InOperator,
        KnnBetaOperator,
        MoreLikeThisOperator,
        NearOperator,
        PhraseOperator,
        QueryStringOperator,
        RangeOperator,
        SearchOperator,
        SpanOperator,
        TermLevelOperator,
        TermOperator,
        TextOperator,
        VectorSearchOperator {
  class Fields {
    private static final Field.Optional<AutocompleteOperator> AUTOCOMPLETE =
        Fields.build("autocomplete", AutocompleteOperator::fromBson);

    private static final Field.Optional<CompoundOperator> COMPOUND =
        Fields.build("compound", CompoundOperator::fromBson);

    private static final Field.Optional<EmbeddedDocumentOperator> EMBEDDED_DOCUMENT =
        Fields.build("embeddedDocument", EmbeddedDocumentOperator::fromBson);

    private static final Field.Optional<EqualsOperator> EQUALS =
        Fields.build("equals", EqualsOperator::fromBson);

    private static final Field.Optional<ExistsOperator> EXISTS =
        Fields.build("exists", ExistsOperator::fromBson);

    private static final Field.Optional<GeoShapeOperator> GEO_SHAPE =
        Fields.build("geoShape", GeoShapeOperator::fromBson);

    private static final Field.Optional<GeoWithinOperator> GEO_WITHIN =
        Fields.build("geoWithin", GeoWithinOperator::fromBson);

    private static final Field.Optional<HasAncestorOperator> HAS_ANCESTOR =
        Fields.build("hasAncestor", HasAncestorOperator::fromBson);

    private static final Field.Optional<HasRootOperator> HAS_ROOT =
        Fields.build("hasRoot", HasRootOperator::fromBson);

    private static final Field.Optional<InOperator> IN = Fields.build("in", InOperator::fromBson);

    private static final Field.Optional<KnnBetaOperator> KNN_BETA =
        Fields.build("knnBeta", KnnBetaOperator::fromBson);

    private static final Field.Optional<MoreLikeThisOperator> MORE_LIKE_THIS =
        Fields.build("moreLikeThis", MoreLikeThisOperator::fromBson);

    private static final Field.Optional<NearOperator> NEAR =
        Fields.build("near", NearOperator::fromBson);

    private static final Field.Optional<PhraseOperator> PHRASE =
        Fields.build("phrase", PhraseOperator::fromBson);

    private static final Field.Optional<QueryStringOperator> QUERY_STRING =
        Fields.build("queryString", QueryStringOperator::fromBson);

    private static final Field.Optional<RangeOperator> RANGE =
        Fields.build("range", RangeOperator::fromBson);

    private static final Field.Optional<RegexOperator> REGEX =
        Fields.build("regex", RegexOperator::fromBson);

    private static final Field.Optional<SearchOperator> SEARCH =
        Fields.build("search", SearchOperator::fromBson);

    private static final Field.Optional<SpanOperator> SPAN =
        Fields.build("span", SpanOperator::fromBson);

    private static final Field.Optional<TermOperator> TERM =
        Fields.build("term", TermOperator::fromBson);

    private static final Field.Optional<TextOperator> TEXT =
        Fields.build("text", TextOperator::fromBson);

    private static final Field.Optional<VectorSearchOperator> VECTOR_SEARCH =
        Fields.build("vectorSearch", VectorSearchOperator::fromBson);

    private static final Field.Optional<WildcardOperator> WILDCARD =
        Fields.build("wildcard", WildcardOperator::fromBson);

    private static <T extends Operator> Field.Optional<T> build(
        String name, ClassField.FromDocumentParser<T> parser) {
      return Field.builder(name)
          .classField(parser, Operator::operatorToBson)
          .disallowUnknownFields()
          .optional()
          .noDefault();
    }

    static final List<Field.Optional<? extends Operator>> ALL =
        List.of(
            AUTOCOMPLETE,
            COMPOUND,
            EMBEDDED_DOCUMENT,
            EQUALS,
            EXISTS,
            GEO_SHAPE,
            GEO_WITHIN,
            HAS_ANCESTOR,
            HAS_ROOT,
            IN,
            KNN_BETA,
            MORE_LIKE_THIS,
            NEAR,
            PHRASE,
            QUERY_STRING,
            RANGE,
            REGEX,
            SEARCH,
            SPAN,
            TERM,
            TEXT,
            VECTOR_SEARCH,
            WILDCARD);
  }

  String ALL_OPERATORS = Fields.ALL.stream().map(Field.Optional::getName).collect(joining(", "));

  /**
   * Operator Type Enumeration.
   *
   * <p>Please keep Type enumeration in alphabetical order.
   *
   * <p>Most types should be as flat as possible which allows more obvious parameter validation and
   * fewer Optional parameters in definitions. This will also make Lucene Query creation simpler and
   * less bug-prone.
   */
  enum Type {
    ALL_DOCUMENTS("allDocuments"),
    AUTOCOMPLETE("autocomplete"),
    COMPOUND("compound"),
    EMBEDDED_DOCUMENT("embeddedDocument"),
    EQUALS("equals"),
    EXISTS("exists"),
    GEO_SHAPE("geoShape"),
    GEO_WITHIN("geoWithin"),
    HAS_ANCESTOR("hasAncestor"),
    HAS_ROOT("hasRoot"),
    IN("in"),
    KNN_BETA("knnBeta"),
    MORE_LIKE_THIS("moreLikeThis"),
    NEAR("near"),
    PHRASE("phrase"),
    QUERY_STRING("queryString"),
    RANGE("range"),
    REGEX("regex"),
    SEARCH("search"),
    SEARCH_PHRASE("searchPhrase"),
    SEARCH_PHRASE_PREFIX("searchPhrasePrefix"),
    SPAN_CONTAINS("spanContains"),
    SPAN_FIRST("spanFirst"),
    SPAN_NEAR("spanNear"),
    SPAN_OR("spanOr"),
    SPAN_SUBTRACT("spanSubtract"),
    SPAN_TERM("spanTerm"),
    TERM("term"),
    TERM_FUZZY("term_fuzzy"),
    TERM_PREFIX("term_prefix"),
    TERM_REGEX("term_regex"),
    TERM_WILDCARD("term_wildcard"),
    TEXT("text"),
    VECTOR_SEARCH("vectorSearch"),
    WILDCARD("wildcard");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  Type getType();

  static Operator parseForFacetCollector(DocumentParser parser) throws BsonParseException {
    if (parser.hasField(Fields.VECTOR_SEARCH)) {
      return parser
          .getContext()
          .handleSemanticError("Facets are not supported with the 'vectorSearch' operator.");
    }
    return exactlyOneFromBson(parser);
  }

  static Operator exactlyOneFromBson(DocumentParser parser) throws BsonParseException {
    return parser.getGroup().exactlyOneOf(parseAllFields(parser));
  }

  static Optional<Operator> atMostOneFromBson(DocumentParser parser) throws BsonParseException {
    return parser.getGroup().atMostOneOf(parseAllFields(parser));
  }

  /**
   * Concrete classes should implement this instead of overriding Operator::toBson. Operator::toBson
   * will add the proper field for the concrete operator to a BsonDocument, then delegate to
   * operatorToBson to encode the operator document.
   */
  BsonValue operatorToBson();

  @Override
  default BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();
    return switch (this) {
      case AllDocumentsOperator ignored -> new BsonDocument("allDocuments", operatorToBson());
      case AutocompleteOperator autocompleteOperator ->
          builder.field(Fields.AUTOCOMPLETE, Optional.of(autocompleteOperator)).build();
      case CompoundOperator compoundOperator ->
          builder.field(Fields.COMPOUND, Optional.of(compoundOperator)).build();
      case EmbeddedDocumentOperator embeddedDocumentOperator ->
          builder.field(Fields.EMBEDDED_DOCUMENT, Optional.of(embeddedDocumentOperator)).build();
      case EqualsOperator equalsOperator ->
          builder.field(Fields.EQUALS, Optional.of(equalsOperator)).build();
      case ExistsOperator existsOperator ->
          builder.field(Fields.EXISTS, Optional.of(existsOperator)).build();
      case GeoShapeOperator geoShapeOperator ->
          builder.field(Fields.GEO_SHAPE, Optional.of(geoShapeOperator)).build();
      case GeoWithinOperator geoWithinOperator ->
          builder.field(Fields.GEO_WITHIN, Optional.of(geoWithinOperator)).build();
      case HasAncestorOperator hasAncestorOperator ->
          builder.field(Fields.HAS_ANCESTOR, Optional.of(hasAncestorOperator)).build();
      case HasRootOperator hasRootOperator ->
          builder.field(Fields.HAS_ROOT, Optional.of(hasRootOperator)).build();
      case InOperator inOperator -> builder.field(Fields.IN, Optional.of(inOperator)).build();
      case KnnBetaOperator knnBetaOperator ->
          builder.field(Fields.KNN_BETA, Optional.of(knnBetaOperator)).build();
      case MoreLikeThisOperator moreLikeThisOperator ->
          builder.field(Fields.MORE_LIKE_THIS, Optional.of(moreLikeThisOperator)).build();
      case NearOperator nearOperator ->
          builder.field(Fields.NEAR, Optional.of(nearOperator)).build();
      case PhraseOperator phraseOperator ->
          builder.field(Fields.PHRASE, Optional.of(phraseOperator)).build();
      case QueryStringOperator queryStringOperator ->
          builder.field(Fields.QUERY_STRING, Optional.of(queryStringOperator)).build();
      case RangeOperator rangeOperator ->
          builder.field(Fields.RANGE, Optional.of(rangeOperator)).build();
      case RegexOperator regexOperator ->
          builder.field(Fields.REGEX, Optional.of(regexOperator)).build();
      case SearchOperator searchOperator ->
          builder.field(Fields.SEARCH, Optional.of(searchOperator)).build();
      case SpanOperator spanOperator ->
          builder.field(Fields.SPAN, Optional.of(spanOperator)).build();
      case TermOperator termOperator ->
          builder.field(Fields.TERM, Optional.of(termOperator)).build();
      case TextOperator textOperator ->
          builder.field(Fields.TEXT, Optional.of(textOperator)).build();
      case VectorSearchOperator vectorSearchOperator ->
          builder.field(Fields.VECTOR_SEARCH, Optional.of(vectorSearchOperator)).build();
      case WildcardOperator wildcardOperator ->
          builder.field(Fields.WILDCARD, Optional.of(wildcardOperator)).build();
    };
  }

  Score score();

  // RedundantSuppression required due to https://youtrack.jetbrains.com/issue/IDEA-284759
  @SuppressWarnings({"unchecked", "rawtypes", "RedundantSuppression"})
  private static ParsedField.Optional<? extends Operator>[] parseAllFields(DocumentParser parser)
      throws BsonParseException {
    var result = new ParsedField.Optional[Fields.ALL.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = parser.getField(Fields.ALL.get(i));
    }
    return result;
  }
}
