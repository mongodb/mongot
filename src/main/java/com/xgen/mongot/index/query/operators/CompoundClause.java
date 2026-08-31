package com.xgen.mongot.index.query.operators;

import java.util.Collections;
import java.util.List;

public record CompoundClause(List<? extends Operator> operators) {

  public CompoundClause(List<? extends Operator> operators) {
    this.operators = List.copyOf(operators);
  }

  static final CompoundClause EMPTY = new CompoundClause(Collections.emptyList());

  public boolean isEmpty() {
    return this.operators.isEmpty();
  }

  public boolean isPresent() {
    return !this.operators.isEmpty();
  }
}
