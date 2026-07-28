package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

/**
 * Class for deserializing mdb-like comparison operators as defined here: <a
 * href="https://www.mongodb.com/docs/manual/reference/operator/query-comparison/">here:</a>. *
 */
public sealed interface ComparisonOperator extends MqlFilterOperator, DocumentEncodable
    permits EqOperator,
        InOperator,
        NeOperator,
        NinOperator,
        OpenRangeBoundComparisonOperator,
        ClosedRangeBoundComparisonOperator {
  class Fields {
    static final Field.Optional<GtOperator> GT =
        Fields.buildWithValueParser("$gt", GtOperator::fromBson);

    static final Field.Optional<GteOperator> GTE =
        Fields.buildWithValueParser("$gte", GteOperator::fromBson);

    static final Field.Optional<LtOperator> LT =
        Fields.buildWithValueParser("$lt", LtOperator::fromBson);

    static final Field.Optional<LteOperator> LTE =
        Fields.buildWithValueParser("$lte", LteOperator::fromBson);

    static final Field.Optional<EqOperator> EQ =
        Fields.buildWithValueParser("$eq", EqOperator::fromBson);

    static final Field.Optional<NeOperator> NE =
        Fields.buildWithValueParser("$ne", NeOperator::fromBson);

    static final Field.Optional<InOperator> IN =
        Fields.buildWithValueParser("$in", InOperator::fromBson);

    static final Field.Optional<NinOperator> NIN =
        Fields.buildWithValueParser("$nin", NinOperator::fromBson);

    static final List<Field.Optional<? extends MqlFilterOperator>> ALL =
        List.of(GT, GTE, LT, LTE, EQ, NE, IN, NIN);

    private static <T extends ComparisonOperator> Field.Optional<T> buildWithValueParser(
        String name, ClassField.FromValueParser<T> parser) {
      return Field.builder(name)
          .classField(parser, ComparisonOperator::operatorToBson)
          .optional()
          .preserveBsonNull()
          .noDefault();
    }
  }

  static ComparisonOperator fromBson(DocumentParser parser) throws BsonParseException {
    return parser.getGroup().exactlyOneOf(MqlFilterOperator.parseAllFields(parser, Fields.ALL));
  }

  BsonValue operatorToBson();

  ValueType getValueType();
}
