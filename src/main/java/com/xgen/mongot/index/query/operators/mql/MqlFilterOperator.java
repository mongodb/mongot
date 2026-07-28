package com.xgen.mongot.index.query.operators.mql;

import com.google.common.collect.Streams;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.List;
import java.util.stream.Stream;

public sealed interface MqlFilterOperator extends DocumentEncodable
    permits ComparisonOperator, NotOperator, ExistsOperator {

  /** Groups operators into categories for metrics reporting. */
  enum Category {
    LOWER_BOUND,
    UPPER_BOUND,
    CLOSED_RANGE,
    EXISTS,
    EQ,
    NE,
    IN,
    NIN,
    NOT,
  }

  /** Returns the category of the operator for metrics reporting. */
  Category getCategory();

  // RedundantSuppression required due to https://youtrack.jetbrains.com/issue/IDEA-284759
  @SuppressWarnings({"unchecked", "rawtypes", "RedundantSuppression"})
  static <T extends ComparisonOperator> ParsedField.Optional<T>[] parseAllFields(
      DocumentParser parser, List<Field.Optional<? extends MqlFilterOperator>> fields)
      throws BsonParseException {
    var result = new ParsedField.Optional[fields.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = parser.getField(fields.get(i));
    }
    return result;
  }

  static List<MqlFilterOperator> atLeastOneFromBson(DocumentParser parser)
      throws BsonParseException {
    List<Field.Optional<? extends MqlFilterOperator>> all =
        Streams.concat(
                ComparisonOperator.Fields.ALL.stream(),
                Stream.of(ExistsOperator.Fields.EXISTS, NotOperator.Fields.NOT))
            .toList();
    return parser.getGroup().atLeastOneOf(parseAllFields(parser, all));
  }
}
