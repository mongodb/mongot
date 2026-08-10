package com.xgen.mongot.index.query.aggregation;

/** Positional aliases for grouping keys on the sharded merge wire format (CLOUDP-432762). */
public final class GroupingKeyAliases {

  private GroupingKeyAliases() {}

  /** Alias for the grouping key at {@code levelIndex} (0 = outermost along a branch). */
  public static String alias(int levelIndex) {
    return "k" + levelIndex;
  }
}
