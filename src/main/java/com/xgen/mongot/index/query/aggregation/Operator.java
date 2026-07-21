package com.xgen.mongot.index.query.aggregation;

/** Supported Aggregation operators. */
public enum Operator {
  COUNT("count"),
  AVG("avg"),
  SUM("sum");

  private final String registryKey;

  Operator(String registryKey) {
    this.registryKey = registryKey;
  }

  /**
   * The key this operator is registered under in the {@link AggregationRegistry} operator map. Kept
   * on the enum so the operator name has a single source of truth, rather than being duplicated as
   * loose constants and bridged by a switch.
   */
  public String registryKey() {
    return this.registryKey;
  }
}
