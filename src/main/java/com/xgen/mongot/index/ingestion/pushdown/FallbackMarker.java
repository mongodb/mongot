package com.xgen.mongot.index.ingestion.pushdown;

import org.jetbrains.annotations.Range;

/**
 * This enum represents possible values when reading a $mqlBson/fallback_marker field. This field is
 * used when we are unable to efficiently store a BsonValue in a type-specific column.
 */
public enum FallbackMarker {

  // Note: A value's `id` may never change. When adding a value, it's `id` should be equal to its
  // ordinal unless you are simultaneously updating `FallbackMarker.of(id)`
  /** The value at this path is an object. We only write markers to prevent quadratic storage */
  OBJECT(0),

  /** The value at this path is a primitive which exceeds the 32KiB Term limit. */
  VALUE_TOO_LARGE(1),

  /** The value at this path is an empty array */
  EMPTY_ARRAY(2);

  public final int id;

  FallbackMarker(int id) {
    this.id = id;
  }

  private static final FallbackMarker[] READ_ONLY_VALUES = FallbackMarker.values();

  /** Returns the enum value given the id read from docValues. */
  public static FallbackMarker of(@Range(from = 0, to = 2) int id) {
    return READ_ONLY_VALUES[id];
  }
}
