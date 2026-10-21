package com.xgen.mongot.embedding.utils;

/**
 * Array element shapes that the pre-CLOUDP-459586 materialized-view writer appended twice.
 *
 * <p>{@link MaterializedViewFieldValueHandler} used to write a value from both {@code
 * handleUuid}/{@code handleKnnVector} and the array iteration that followed. Only elements inside a
 * passthrough array are represented here, because those became extra elements in the MV rather than
 * duplicate document keys.
 */
public enum MaterializedViewArrayElementType {
  UUID("uuid"),
  VECTOR("vector");

  private final String tagValue;

  MaterializedViewArrayElementType(String tagValue) {
    this.tagValue = tagValue;
  }

  /** Value for the {@code elementType} metric tag. */
  public String tagValue() {
    return this.tagValue;
  }
}
