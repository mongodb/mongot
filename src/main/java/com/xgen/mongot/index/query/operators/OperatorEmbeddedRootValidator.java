package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;

public class OperatorEmbeddedRootValidator {
  private final BsonParseContext bsonParseContext;

  private static class ErrorMessages {
    private static String invalidQueryPath(
        Operator.Type operatorType, FieldPath path, ValidatorContext context) {
      var embeddedRoot = Check.isPresent(context.embeddedRoot, "embeddedRoot");
      var embeddedRootSource = Check.isPresent(context.embeddedRootSource, "embeddedRootSource");
      return String.format(
          "%s operator path '%s' must be a descendant of %s '%s'",
          operatorType.getName(), path, embeddedRootSource, embeddedRoot);
    }

    private static String invalidScorePath(
        Operator.Type operatorType, FieldPath path, ValidatorContext context) {
      var embeddedRoot = Check.isPresent(context.embeddedRoot, "embeddedRoot");
      var embeddedRootSource = Check.isPresent(context.embeddedRootSource, "embeddedRootSource");
      return invalidScorePath(operatorType, path, embeddedRootSource, embeddedRoot);
    }

    private static String invalidScorePath(
        Operator.Type operatorType,
        FieldPath path,
        EmbeddedRootSource embeddedRootSource,
        FieldPath embeddedRoot) {
      return String.format(
          "%s operator score path expression '%s' must be a descendant of %s '%s'",
          operatorType.getName(), path, embeddedRootSource, embeddedRoot);
    }

    private static String invalidHasAncestorOperatorAncestorPath(
        FieldPath ancestorPath, ValidatorContext context) {
      var embeddedRoot = Check.isPresent(context.embeddedRoot, "embeddedRoot");
      var embeddedRootSource = Check.isPresent(context.embeddedRootSource, "embeddedRootSource");
      return String.format(
          "hasAncestor.ancestorPath '%s' must be a parent of %s '%s'",
          ancestorPath, embeddedRootSource, embeddedRoot);
    }

    private static String maxJoinsExceeded() {
      return String.format(
          "Query must contain less than %d occurrences of "
              + "hasAncestor + hasRoot + embeddedDocuments.",
          ValidatorContext.MAX_JOINS - 1);
    }

    private static String missingEmbeddedRoot(Operator.Type operatorType) {
      return String.format(
          "%s operator requires %s, %s, or %s to be set.",
          operatorType.getName(),
          EmbeddedRootSource.EMBEDDED_DOCUMENT,
          EmbeddedRootSource.HAS_ANCESTOR,
          EmbeddedRootSource.RETURN_SCOPE);
    }

    private static String notAllowedInEmbeddedDocs(Operator.Type operatorType) {
      return String.format(
          "Operator: %s not allowed in embeddedDocument operator", operatorType.getName());
    }
  }

  enum EmbeddedRootSource {
    EMBEDDED_DOCUMENT("embeddedDocument.path"),
    HAS_ANCESTOR("hasAncestor.ancestorPath"),
    RETURN_SCOPE("returnScope.path");

    private final String source;

    EmbeddedRootSource(String source) {
      this.source = source;
    }

    @Override
    public String toString() {
      return this.source;
    }
  }

  // Empty embeddedRoot indicates root.

  private record ValidatorContext(
      int numJoins,
      Optional<FieldPath> embeddedRoot,
      Operator.Type operatorType,
      Optional<EmbeddedRootSource> embeddedRootSource) {
    public static final int MAX_JOINS = 5;

    public ValidatorContext of(Operator operator) {
      return switch (operator) {
        case EmbeddedDocumentOperator embeddedDocumentOperator ->
            // EmbeddedDocumentOperator changes embeddedRoot and performs a parent join.
            new ValidatorContext(
                this.numJoins + 1,
                Optional.of(embeddedDocumentOperator.path()),
                operator.getType(),
                Optional.of(EmbeddedRootSource.EMBEDDED_DOCUMENT));
        case HasAncestorOperator hasAncestorOperator ->
            // These operators do not change embeddedRoot and perform a child join.
            new ValidatorContext(
                this.numJoins + 1,
                Optional.of(hasAncestorOperator.ancestorPath()),
                operator.getType(),
                Optional.of(EmbeddedRootSource.HAS_ANCESTOR));
        case HasRootOperator hasRootOperator ->
            new ValidatorContext(
                this.numJoins + 1, Optional.empty(), operator.getType(), Optional.empty());
        default ->
            // All other operators: do not change embedded root.
            // use default here since full list of operators pattern matching check are done in
            // validate method.
            new ValidatorContext(
                this.numJoins, this.embeddedRoot(), operator.getType(), this.embeddedRootSource);
      };
    }
  }

  public OperatorEmbeddedRootValidator(BsonParseContext bsonParseContext) {
    this.bsonParseContext = bsonParseContext;
  }

  private void validateEmbeddedOperator(
      EmbeddedDocumentOperator embeddedDocumentOperator, ValidatorContext context)
      throws BsonParseException {
    validateEmbeddedDocsAllowedOperator(embeddedDocumentOperator.operator());
    if (embeddedDocumentOperator.operator() instanceof AllDocumentsOperator
        || embeddedDocumentOperator.operator() instanceof MoreLikeThisOperator) {
      this.bsonParseContext.handleSemanticError(
          ErrorMessages.notAllowedInEmbeddedDocs(embeddedDocumentOperator.operator().getType()));
    }
    if (context.embeddedRoot().isPresent()
        && !embeddedDocumentOperator.path().isChildOf(context.embeddedRoot.get())) {
      this.bsonParseContext.handleSemanticError(
          ErrorMessages.invalidQueryPath(
              Operator.Type.EMBEDDED_DOCUMENT, embeddedDocumentOperator.path(), context));
    }
    for (FieldPath childPath : embeddedDocumentOperator.score().getChildPaths()) {
      if (context.embeddedRoot().isPresent() && !childPath.isChildOf(context.embeddedRoot.get())) {
        this.bsonParseContext.handleSemanticError(
            ErrorMessages.invalidScorePath(Operator.Type.EMBEDDED_DOCUMENT, childPath, context));
      }
    }
  }

  private void validateHasAncestorOperator(
      HasAncestorOperator hasAncestorOperator, ValidatorContext context) throws BsonParseException {
    validateEmbeddedDocsAllowedOperator(hasAncestorOperator.operator());
    validateEmbeddedRootPresence(context, hasAncestorOperator);
    if (!context.embeddedRoot.get().isChildOf(hasAncestorOperator.ancestorPath())) {
      this.bsonParseContext.handleSemanticError(
          ErrorMessages.invalidHasAncestorOperatorAncestorPath(
              hasAncestorOperator.ancestorPath(), context));
    }
    for (FieldPath childPath : hasAncestorOperator.score().getChildPaths()) {
      if (!childPath.isChildOf(hasAncestorOperator.ancestorPath())) {
        // Score expression for hasAncestor can be any child path of ancestorPath, not restricted by
        // the previous embeddedRoot
        this.bsonParseContext.handleSemanticError(
            ErrorMessages.invalidScorePath(
                Operator.Type.HAS_ANCESTOR,
                childPath,
                EmbeddedRootSource.HAS_ANCESTOR,
                hasAncestorOperator.ancestorPath()));
      }
    }
  }

  private void validateEmbeddedDocsAllowedOperator(Operator operator) throws BsonParseException {
    if (operator instanceof AllDocumentsOperator || operator instanceof MoreLikeThisOperator) {
      this.bsonParseContext.handleSemanticError(
          ErrorMessages.notAllowedInEmbeddedDocs(operator.getType()));
    }
  }

  private void validateEmbeddedRootPresence(ValidatorContext context, Operator operator)
      throws BsonParseException {
    if (context.embeddedRoot.isEmpty()) {
      this.bsonParseContext.handleSemanticError(
          ErrorMessages.missingEmbeddedRoot(operator.getType()));
    }
  }

  private void validateLeafOperatorPaths(
      List<FieldPath> paths, List<FieldPath> scorePaths, ValidatorContext context)
      throws BsonParseException {
    if (context.embeddedRoot.isPresent()) {
      for (FieldPath childPath : paths) {
        if (!childPath.isChildOf(context.embeddedRoot().get())) {
          this.bsonParseContext.handleSemanticError(
              ErrorMessages.invalidQueryPath(context.operatorType, childPath, context));
        }
      }
      for (FieldPath scorePath : scorePaths) {
        if (!scorePath.isChildOf(context.embeddedRoot().get())) {
          this.bsonParseContext.handleSemanticError(
              ErrorMessages.invalidScorePath(context.operatorType, scorePath, context));
        }
      }
    }
  }

  private void validate(Operator operator, ValidatorContext context) throws BsonParseException {
    if (context.numJoins >= ValidatorContext.MAX_JOINS) {
      this.bsonParseContext.handleSemanticError(ErrorMessages.maxJoinsExceeded());
    }
    switch (operator) {
      case AllDocumentsOperator allDocumentsOperator -> {
        // AllDocumentsOperator has no path to validate
      }
      case AutocompleteOperator autocompleteOperator ->
          validateLeafOperatorPaths(
              List.of(autocompleteOperator.path()),
              autocompleteOperator.score().getChildPaths(),
              context.of(autocompleteOperator));
      case CompoundOperator compoundOperator -> {
        ValidatorContext currentOperatorContext = context.of(compoundOperator);
        CheckedStream.from(compoundOperator.getOperators())
            .forEachChecked(o -> validate(o, currentOperatorContext));
      }
      case EmbeddedDocumentOperator embeddedDocumentOperator -> {
        validateEmbeddedOperator(embeddedDocumentOperator, context);
        validate(embeddedDocumentOperator.operator(), context.of(embeddedDocumentOperator));
      }
      case EqualsOperator equalsOperator ->
          validateLeafOperatorPaths(
              List.of(equalsOperator.path()),
              equalsOperator.score().getChildPaths(),
              context.of(equalsOperator));
      case ExistsOperator existsOperator ->
          validateLeafOperatorPaths(
              List.of(FieldPath.parse(existsOperator.path())),
              existsOperator.score().getChildPaths(),
              context.of(existsOperator));
      case GeoShapeOperator geoShapeOperator ->
          validateLeafOperatorPaths(
              geoShapeOperator.paths(),
              geoShapeOperator.score().getChildPaths(),
              context.of(geoShapeOperator));
      case GeoWithinOperator geoWithinOperator ->
          validateLeafOperatorPaths(
              geoWithinOperator.paths(),
              geoWithinOperator.score().getChildPaths(),
              context.of(geoWithinOperator));
      case HasAncestorOperator hasAncestorOperator -> {
        validateHasAncestorOperator(hasAncestorOperator, context);
        validate(hasAncestorOperator.operator(), context.of(hasAncestorOperator));
      }
      case HasRootOperator hasRootOperator -> {
        validateEmbeddedDocsAllowedOperator(hasRootOperator.operator());
        validateEmbeddedRootPresence(context, hasRootOperator);
        validate(hasRootOperator.operator(), context.of(hasRootOperator));
      }
      case InOperator inOperator ->
          validateLeafOperatorPaths(
              inOperator.paths(), inOperator.score().getChildPaths(), context.of(inOperator));
      case VectorSearchOperator vectorSearchOperator ->
          validateLeafOperatorPaths(
              List.of(vectorSearchOperator.criteria().path()),
              vectorSearchOperator.score().getChildPaths(),
              context.of(vectorSearchOperator));
      case KnnBetaOperator knnBetaOperator ->
          validateLeafOperatorPaths(
              knnBetaOperator.paths(),
              knnBetaOperator.score().getChildPaths(),
              context.of(knnBetaOperator));
      case MoreLikeThisOperator moreLikeThisOperator -> {
        // MLT is not supported by any operators using embeddedDocuments
      }
      case NearOperator nearOperator ->
          validateLeafOperatorPaths(
              nearOperator.paths(), nearOperator.score().getChildPaths(), context.of(nearOperator));
      case PhraseOperator phraseOperator ->
          validateLeafOperatorPaths(
              UnresolvedStringPath.toFieldPaths(phraseOperator.paths()),
              phraseOperator.score().getChildPaths(),
              context.of(phraseOperator));
      case QueryStringOperator queryStringOperator ->
          validateLeafOperatorPaths(
              List.of(FieldPath.parse(queryStringOperator.defaultPath())),
              queryStringOperator.score().getChildPaths(),
              context.of(queryStringOperator));
      case RangeOperator rangeOperator ->
          validateLeafOperatorPaths(
              rangeOperator.paths(),
              rangeOperator.score().getChildPaths(),
              context.of(rangeOperator));
      case RegexOperator regexOperator ->
          validateLeafOperatorPaths(
              UnresolvedStringPath.toFieldPaths(regexOperator.paths()),
              regexOperator.score().getChildPaths(),
              context.of(regexOperator));
      case SearchOperator searchOperator ->
          validateLeafOperatorPaths(
              StringPath.toFieldPaths(searchOperator.paths()),
              searchOperator.score().getChildPaths(),
              context.of(searchOperator));
      case SpanOperator spanOperator ->
          validateLeafOperatorPaths(
              StringPath.toFieldPaths(spanOperator.getPaths()),
              spanOperator.score().getChildPaths(),
              context.of(spanOperator));
      case TermOperator termOperator ->
          validateLeafOperatorPaths(
              StringPath.toFieldPaths(termOperator.paths()),
              termOperator.score().getChildPaths(),
              context.of(termOperator));
      case TextOperator textOperator ->
          validateLeafOperatorPaths(
              UnresolvedStringPath.toFieldPaths(textOperator.paths()),
              textOperator.score().getChildPaths(),
              context.of(textOperator));
      case WildcardOperator wildcardOperator ->
          validateLeafOperatorPaths(
              UnresolvedStringPath.toFieldPaths(wildcardOperator.paths()),
              wildcardOperator.score().getChildPaths(),
              context.of(wildcardOperator));
    }
  }

  public void validate(Operator operator, Optional<FieldPath> embeddedRoot)
      throws BsonParseException {
    Optional<EmbeddedRootSource> embeddedRootSource =
        embeddedRoot.isPresent() ? Optional.of(EmbeddedRootSource.RETURN_SCOPE) : Optional.empty();
    validate(
        operator, new ValidatorContext(0, embeddedRoot, operator.getType(), embeddedRootSource));
  }

  /** Validates an operator relative to its inline embedded-document scope. */
  public void validate(EmbeddedDocumentScope scope) throws BsonParseException {
    validate(
        scope.operator(),
        new ValidatorContext(
            0,
            Optional.of(scope.path()),
            scope.operator().getType(),
            Optional.of(EmbeddedRootSource.EMBEDDED_DOCUMENT)));
  }
}
