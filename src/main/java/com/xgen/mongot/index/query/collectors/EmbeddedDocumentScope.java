package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import org.bson.BsonDocument;

/**
 * The inline embedded-document scope carried by a scoped facet (and, in a later milestone, a scoped
 * sort). {@code path} names the {@code embeddedDocuments} array to scope into and {@code operator}
 * is an operator over that array's children selecting which ones contribute.
 *
 * <p>A facet with no {@code embeddedDocument} block is document-indexed exactly as today. See the
 * "embeddedDoc-scoped Sort and Facet" syntax design.
 */
public record EmbeddedDocumentScope(FieldPath path, Operator operator)
    implements DocumentEncodable {

  static final class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
    static final Field.Required<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::parseForFacetCollector)
            .disallowUnknownFields()
            .required();
  }

  public static EmbeddedDocumentScope fromBson(DocumentParser parser) throws BsonParseException {
    FieldPath path = parser.getField(Fields.PATH).unwrap();
    Operator operator = parser.getField(Fields.OPERATOR).unwrap();
    // The scope's operator selects children of `path`, so its operand paths must be under `path`,
    // mirroring how EmbeddedDocumentOperator validates its inner operator.
    EmbeddedDocumentOperator.validateOperatorPaths(path, operator, parser.getContext());
    return new EmbeddedDocumentScope(path, operator);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.OPERATOR, this.operator)
        .build();
  }
}
