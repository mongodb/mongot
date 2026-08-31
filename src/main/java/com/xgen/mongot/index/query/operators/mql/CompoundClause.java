package com.xgen.mongot.index.query.operators.mql;

import com.google.common.base.Objects;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;

/**
 * mql clauses like $and/$or {simpleClause1} {simpleClause2} E.g, $and: [ { genre: "fiction" }, {
 * pages: { $gt: 300 } } ]
 */
public abstract sealed class CompoundClause implements Clause
    permits AndClause, NorClause, OrClause {
  protected final List<Clause> clauses;

  public static class Values {
    static final Value.Required<List<Clause>> CLAUSES =
        Value.builder()
            .listOf(Value.builder().classValue(Clause::fromBson).required())
            .mustNotBeEmpty()
            .required();
  }

  static final String AND_FIELD_KEY = "$and";
  static final String OR_FIELD_KEY = "$or";
  static final String NOR_FIELD_KEY = "$nor";

  public enum Operator {
    AND,
    OR,
    NOR
  }

  public CompoundClause(List<Clause> clauses) {
    this.clauses = List.copyOf(clauses);
  }

  public abstract Operator getOperator();

  public abstract List<Clause> getClauses();

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompoundClause that = (CompoundClause) o;
    return Objects.equal(this.clauses, that.clauses);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.clauses);
  }
}
