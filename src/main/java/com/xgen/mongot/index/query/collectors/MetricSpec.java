package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * One {@code aggregations.<outName>} metric of an aggregation-capable facet node: {@code { type:
 * <op>, path?: <field> }}.
 *
 * <p>Purely syntactic at parse time: the metric type is mapped to an {@link Operator} and the path
 * string converts to a {@link FieldPath} (via {@link FieldPathField}, as operator paths do); schema
 * eligibility is checked later at spec-build/plan time.
 */
public record MetricSpec(Operator operator, Optional<FieldPath> path) implements DocumentEncodable {

  private static final class Fields {
    // The user-facing metric type names are the Operator enum's camel-case names
    // (count/sum/avg, matching MQL accumulator names), so the wire syntax and the
    // execution registry share a single source of truth.
    // TODO(CLOUDP-425978): first/last are absent from Operator until global-sort plumbing lands;
    // until then they are rejected here as unknown metric types.
    private static final Field.Required<Operator> TYPE =
        Field.builder("type").enumField(Operator.class).asCamelCase().required();
    // Optional because count takes no path (it counts bucket docs); every other operator
    // requires one -- enforced in fromBson below.
    private static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();
  }

  static MetricSpec fromBson(DocumentParser parser) throws BsonParseException {
    Operator operator = parser.getField(Fields.TYPE).unwrap();
    Optional<FieldPath> path = parser.getField(Fields.PATH).unwrap();

    // count takes no path; every other operator requires one. Semantic field-type checks
    // (number/date eligibility) happen later at spec-build time.
    if (operator == Operator.COUNT && path.isPresent()) {
      return parser.getContext().handleSemanticError("metric of type 'count' does not take a path");
    }
    if (operator != Operator.COUNT && path.isEmpty()) {
      return parser
          .getContext()
          .handleSemanticError("metric of type '" + operator.registryKey() + "' requires a path");
    }

    return new MetricSpec(operator, path);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TYPE, this.operator)
        .field(Fields.PATH, this.path)
        .build();
  }
}
