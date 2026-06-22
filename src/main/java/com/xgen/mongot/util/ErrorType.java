package com.xgen.mongot.util;

/** Enum for error types to use consistently as metric tags or logs. */
public enum ErrorType {
  USER_FACING_ERROR("userFacingError"),
  INTERNAL_ERROR("internalError");

  public static final String METRIC_TAG_KEY = "errorType";

  private final String label;

  ErrorType(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return this.label;
  }
}
