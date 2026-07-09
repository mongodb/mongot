package com.xgen.mongot.index.query.operators.mql;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class AndClause extends CompoundClause {
  // explicitAnd will always be true for an explicit AND: {"$and": [filters]}
  // and false for an implicit AndClause: {path1: value1, path2: value2}
  private final boolean explicitAnd;

  @VisibleForTesting
  public AndClause(List<Clause> clauses, boolean explicitAnd) {
    super(clauses);
    this.explicitAnd = explicitAnd;
    if (this.clauses.isEmpty()) {
      // An empty implicit AndClause represents an empty filter. This check is only for sanity
      // check and is not user facing.
      Check.checkState(!this.explicitAnd, "Empty array is not allowed in $and");
    }
  }

  // Creates a pass-thru Clause if there is only one clause in the list.
  // Otherwise, creates an AndClause.
  public static Clause createImplicitAndClause(List<Clause> clauses) {
    if (clauses.size() == 1) {
      return clauses.get(0);
    }
    return new AndClause(clauses, false);
  }

  public static AndClause bsonToExplicitAndClause(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    List<Clause> clauses = Values.CLAUSES.getParser().parse(context, value);
    // The clauses won't be empty because CLAUSES has mustNotBeEmpty().
    return new AndClause(clauses, true);
  }

  @Override
  public Operator getOperator() {
    return Operator.AND;
  }

  @Override
  public List<Clause> getClauses() {
    return this.clauses;
  }

  public boolean isExplicitAnd() {
    return this.explicitAnd;
  }

  @Override
  public BsonValue toBson() {
    if (this.explicitAnd) {
      return new BsonDocument(
          AND_FIELD_KEY, CompoundClause.Values.CLAUSES.getEncoder().encode(this.clauses));
    }
    BsonDocument mergedDoc = new BsonDocument();
    for (Clause clause : getClauses()) {
      BsonDocument clauseDoc = clause.toBson().asDocument();
      Check.checkState(
          clauseDoc.keySet().size() == 1,
          "This level of AndClause is an implicit AND. Therefore, all the subclauses "
              + "must be either simple form, or explicit $and or $or. This clause document is: %s",
          clauseDoc);
      mergedDoc.putAll(clauseDoc);
    }
    return mergedDoc;
  }
}
