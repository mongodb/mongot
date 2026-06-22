package com.xgen.mongot.index.query.operators;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringMultiFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonString;
import org.bson.BsonValue;

class Operators {

  public static class Fields {
    public static final Field.Required<List<FieldPath>> FIELD_PATH =
        Field.builder("path")
            .classField(
                Operators::fieldPathFromBson, fieldPath -> new BsonString(fieldPath.toString()))
            .asSingleValueOrList()
            .mustNotBeEmpty()
            .required();

    public static final Field.Required<List<StringPath>> STRING_PATH =
        Field.builder("path")
            .classField(StringPath::fromBson)
            .asSingleValueOrList()
            .mustNotBeEmpty()
            .required();

    public static final Field.Required<List<UnresolvedStringPath>> UNRESOLVED_STRING_PATH =
        Field.builder("path")
            .classField(UnresolvedStringPath::fromBson)
            .asSingleValueOrList()
            .mustNotBeEmpty()
            .required();

    public static final Field.Required<List<String>> QUERY =
        Field.builder("query")
            .singleValueOrListOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .mustNotBeEmpty()
            .required();

    public static final Field.WithDefault<Score> SCORE =
        Field.builder("score")
            .classField(Score::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(Score.defaultScore());
  }

  private Operators() {}

  static List<FieldPath> parseFieldPath(DocumentParser parser) throws BsonParseException {
    return parser.getField(Fields.FIELD_PATH).unwrap();
  }

  static List<StringPath> parseStringPath(DocumentParser parser) throws BsonParseException {
    List<UnresolvedStringPath> paths = parser.getField(Fields.UNRESOLVED_STRING_PATH).unwrap();

    ensureNoWildcardsInPath(paths, parser.getContext());
    return fromUnresolvedPaths(paths);
  }

  static List<UnresolvedStringPath> parseUnresolvedStringPath(DocumentParser parser)
      throws BsonParseException {
    return parser.getField(Fields.UNRESOLVED_STRING_PATH).unwrap();
  }

  static List<String> parseQuery(DocumentParser parser) throws BsonParseException {
    return parser.getField(Fields.QUERY).unwrap();
  }

  static Score parseScore(DocumentParser parser) throws BsonParseException {
    return parser.getField(Fields.SCORE).unwrap();
  }

  static BsonDocumentBuilder documentBuilder(Score score) {
    return documentBuilderForScore(score);
  }

  static BsonDocumentBuilder documentBuilder(Score score, List<FieldPath> paths) {
    return documentBuilderForScore(score).field(Fields.FIELD_PATH, paths);
  }

  static BsonDocumentBuilder documentBuilderWithUnresolvedStringPath(
      Score score, List<UnresolvedStringPath> paths, List<String> query) {
    return documentBuilderForScore(score)
        .field(Fields.UNRESOLVED_STRING_PATH, paths)
        .field(Fields.QUERY, query);
  }

  static BsonDocumentBuilder documentBuilderWithStringPath(
      Score score, List<StringPath> paths, List<String> query) {
    return documentBuilderForScore(score)
        .field(Fields.STRING_PATH, paths)
        .field(Fields.QUERY, query);
  }

  private static BsonDocumentBuilder documentBuilderForScore(Score score) {
    return BsonDocumentBuilder.builder().field(Fields.SCORE, score);
  }

  private static FieldPath fieldPathFromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    StringPath stringPath = StringPath.fromBson(context, value);
    return switch (stringPath.getType()) {
      case FIELD -> stringPath.asField().getValue();
      case MULTI_FIELD ->
          context.handleSemanticError("multi field not permitted for this operator");
    };
  }

  private static void ensureNoWildcardsInPath(
      List<UnresolvedStringPath> paths, BsonParseContext context) throws BsonParseException {
    if (paths.stream().anyMatch(path -> path instanceof UnresolvedStringWildcardPath)) {
      context.handleSemanticError("cannot have wildcards in path definition for this operator");
    }
  }

  private static List<StringPath> fromUnresolvedPaths(List<UnresolvedStringPath> paths) {
    return paths.stream().map(Operators::fromUnresolvedPath).collect(Collectors.toList());
  }

  private static StringPath fromUnresolvedPath(UnresolvedStringPath path) {
    return switch (path) {
      case UnresolvedStringFieldPath fieldPath -> new StringFieldPath(fieldPath.getValue());
      case UnresolvedStringMultiFieldPath multiFieldPath ->
          new StringMultiFieldPath(multiFieldPath.getFieldPath(), multiFieldPath.getMulti());
      case UnresolvedStringWildcardPath ignored ->
          Check.unreachable("UnresolvedStringWildcardPath should not be converted to StringPath");
    };
  }

  // TODO(CLOUDP-327217): Remove this method once validation is moved to OperatorValidator.
  static ImmutableList<FieldPath> getAdjacentChildPaths(Operator operator) {
    // Get paths of child operators and paths referenced in scores of child operators.
    return CollectionUtils.concat(getOperatorPaths(operator), operator.score().getChildPaths());
  }

  // TODO(CLOUDP-327217): Remove this method once validation is moved to OperatorValidator.
  private static List<FieldPath> getOperatorPaths(Operator operator) {
    return switch (operator) {
      case AllDocumentsOperator allDocumentsOperator ->
          throw new UnsupportedOperationException(
              String.format(
                  "Operator: %s not allowed in embeddedDocs", operator.getType().getName()));
      case MoreLikeThisOperator moreLikeThisOperator ->
          throw new UnsupportedOperationException(
              String.format(
                  "Operator: %s not allowed in embeddedDocs", operator.getType().getName()));
      case AutocompleteOperator autocompleteOperator -> List.of(autocompleteOperator.path());
      case CompoundOperator compoundOperator ->
          compoundOperator
              .getOperators()
              .flatMap(childOperator -> getAdjacentChildPaths(childOperator).stream())
              .collect(Collectors.toList());
      case EmbeddedDocumentOperator embeddedDocumentOperator ->
          Collections.singletonList(embeddedDocumentOperator.path());
      case EqualsOperator equalsOperator -> List.of(equalsOperator.path());
      case ExistsOperator existsOperator -> List.of(FieldPath.parse(existsOperator.path()));
      case GeoShapeOperator geoShapeOperator -> geoShapeOperator.paths();
      case GeoWithinOperator geoWithinOperator -> geoWithinOperator.paths();
      case HasAncestorOperator hasAncestorOperator -> List.of();
      case HasRootOperator hasRootOperator -> List.of();
      case VectorSearchOperator vectorSearchOperator ->
          List.of(vectorSearchOperator.criteria().path());
      case InOperator inOperator -> inOperator.paths();
      case KnnBetaOperator knnBetaOperator -> knnBetaOperator.paths();
      case NearOperator nearOperator -> nearOperator.paths();
      case PhraseOperator phraseOperator ->
          UnresolvedStringPath.toFieldPaths(phraseOperator.paths());
      case QueryStringOperator queryStringOperator ->
          List.of(FieldPath.parse(queryStringOperator.defaultPath()));
      case RangeOperator rangeOperator -> rangeOperator.paths();
      case RegexOperator regexOperator -> UnresolvedStringPath.toFieldPaths(regexOperator.paths());
      case SearchOperator searchOperator -> StringPath.toFieldPaths(searchOperator.paths());
      case SpanOperator spanOperator -> StringPath.toFieldPaths(spanOperator.getPaths());
      case TermOperator termOperator -> StringPath.toFieldPaths(termOperator.paths());
      case TextOperator textOperator -> UnresolvedStringPath.toFieldPaths(textOperator.paths());
      case WildcardOperator wildcardOperator ->
          UnresolvedStringPath.toFieldPaths(wildcardOperator.paths());
    };
  }
}
