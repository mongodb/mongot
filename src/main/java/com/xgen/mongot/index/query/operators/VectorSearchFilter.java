package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Abstract representation of a filter used in vector search. Can be either:
 *
 * <ul>
 *   <li>{@link ClauseFilter} — used in {@code $vectorSearch}
 *   <li>{@link OperatorFilter} — used in {@code $search.vectorSearch}
 * </ul>
 */
public sealed interface VectorSearchFilter extends DocumentEncodable
    permits VectorSearchFilter.ClauseFilter, VectorSearchFilter.OperatorFilter {

  enum Type {
    CLAUSE,
    OPERATOR
  }

  Type type();

  record ClauseFilter(Clause clause) implements VectorSearchFilter {
    @Override
    public Type type() {
      return Type.CLAUSE;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.CLAUSE_FILTER, Optional.of(this.clause()))
          .build();
    }
  }

  record OperatorFilter(Operator operator) implements VectorSearchFilter {

    @Override
    public Type type() {
      return Type.OPERATOR;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.OPERATOR_FILTER, Optional.of(this.operator()))
          .build();
    }
  }

  class Fields {
    static final Field.Optional<Clause> CLAUSE_FILTER =
        Field.builder("filter").classField(Clause::fromBson).optional().noDefault();

    static final Field.Optional<Clause> CLAUSE_PARENT_FILTER =
        Field.builder("parentFilter").classField(Clause::fromBson).optional().noDefault();

    public static final Field.Optional<Operator> OPERATOR_FILTER =
        Field.builder("filter")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<Operator> OPERATOR_PARENT_FILTER =
        Field.builder("parentFilter")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  static Optional<VectorSearchFilter> fromBson(DocumentParser parser, Type type)
      throws BsonParseException {
    return switch (type) {
      case OPERATOR -> parser.getField(Fields.OPERATOR_FILTER).unwrap().map(OperatorFilter::new);
      case CLAUSE -> parser.getField(Fields.CLAUSE_FILTER).unwrap().map(ClauseFilter::new);
    };
  }

  static Optional<VectorSearchFilter> fromBsonParentFilter(DocumentParser parser, Type type)
      throws BsonParseException {
    return switch (type) {
      case OPERATOR ->
          parser.getField(Fields.OPERATOR_PARENT_FILTER).unwrap().map(OperatorFilter::new);
      case CLAUSE -> parser.getField(Fields.CLAUSE_PARENT_FILTER).unwrap().map(ClauseFilter::new);
    };
  }
}
