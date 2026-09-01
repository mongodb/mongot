package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.mql.ComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.EqOperator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * The {@code indexNamespace} option of a {@code $vectorSearch} query: a single-field equality
 * {@code { <path>: { $eq: <value> } }} selecting the namespace of the index to search. Required
 * when the index definition declares an {@code indexNamespace}, in which case {@code path} must
 * match the definition's path.
 */
public record IndexNamespaceFilter(FieldPath path, EqOperator eq) implements Encodable {

  private static final String SHAPE_ERROR =
      "indexNamespace must be a document of the form { <path>: { $eq: <value> } }";

  /** The namespace value to search. */
  public BsonValue value() {
    return this.eq.operatorToBson();
  }

  public static IndexNamespaceFilter fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    if (!value.isDocument()) {
      context.handleSemanticError(SHAPE_ERROR);
    }
    BsonDocument document = value.asDocument();
    if (document.size() != 1) {
      context.handleSemanticError(SHAPE_ERROR);
    }
    String key = document.getFirstKey();
    BsonValue predicate = document.get(key);
    if (!predicate.isDocument()) {
      context.handleSemanticError(SHAPE_ERROR);
    }
    try (var parser = BsonDocumentParser.withContext(context, predicate.asDocument()).build()) {
      if (!(ComparisonOperator.fromBson(parser) instanceof EqOperator eqOperator)) {
        return context.handleSemanticError(SHAPE_ERROR);
      }
      return new IndexNamespaceFilter(FieldPath.parse(key), eqOperator);
    }
  }

  @Override
  public BsonValue toBson() {
    return new BsonDocument(this.path.toString(), this.eq.toBson());
  }
}
