package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Class that deserializes MQL clauses relating to a single field. It supports the following
 * formats:
 *
 * <ol>
 *   <li>{pathname: {operation1, operation2},...}
 *       <ul>
 *         <li>{ pages: { $gt: 300, $lte: 500} }
 *         <li>{ pages: { $not: { $gt: 300}, $eq: 400}}
 *       </ul>
 *   <li>{ pathname: value }
 *       <ul>
 *         <li>{ genre: "fiction" }
 *       </ul>
 * </ol>
 */
public record SimpleClause(FieldPath path, List<MqlFilterOperator> mqlFilterOperators)
    implements Clause {

  static class Fields {
    static Field.Required<MqlFilterOperatorList> comparisonOperatorList(String path) {
      return Field.builder(path)
          .classField(MqlFilterOperatorList::fromBson, MqlFilterOperatorList::toBson)
          .disallowUnknownFields()
          .required();
    }
  }

  public SimpleClause(FieldPath path, List<MqlFilterOperator> mqlFilterOperators) {
    this.path = path;
    // We should not be able to reach this. It's fine to be a little slow on query parsing.
    Check.argNotEmpty(mqlFilterOperators, "mqlFilterOperators");
    this.mqlFilterOperators = List.copyOf(mqlFilterOperators);
  }

  // The valueInThePath is the value in the (path, valueInThePath) kv pair.
  public static SimpleClause bsonToSimpleClause(
      BsonParseContext context, DocumentParser parser, String path, BsonValue valueInThePath)
      throws BsonParseException {
    if (isEqShortForm(valueInThePath)) {
      EqOperator eqOperator = EqOperator.fromBson(context, valueInThePath);
      List<MqlFilterOperator> comparisonOperators = List.of(eqOperator);
      return new SimpleClause(FieldPath.parse(path), comparisonOperators);
    }
    MqlFilterOperatorList operatorList =
        parser.getField(Fields.comparisonOperatorList(path)).unwrap();
    return new SimpleClause(FieldPath.parse(path), operatorList.mqlFilterOperators());
  }

  // Whether this bsonValue is in a equal short form: {path: bsonValue}
  private static boolean isEqShortForm(BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      return true;
    }
    BsonDocument clauseDocument = bsonValue.asDocument();
    // Check if there is any operator key present in the clauseDocument.
    // If the clauseDocument is of the form { "$op1": value1, "$op2": value2 ...}
    // it is a comparisonOperatorList.
    // If the document has any key which starts with "$" it's treated as comparisonOperatorList.
    // If there is a non operator key present along with at least one operator key we throw error
    // during parsing of comparisonOperatorList
    return clauseDocument.keySet().stream().noneMatch(clauseKey -> clauseKey.startsWith("$"));
  }

  @Override
  public BsonValue toBson() {
    return new BsonDocument(
        this.path.toString(), new MqlFilterOperatorList(this.mqlFilterOperators).toBson());
  }
}
