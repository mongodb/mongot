package com.xgen.mongot.index.ingestion.pushdown;

import com.xgen.mongot.util.FieldPath;
import java.util.Locale;

/**
 * This enum represents field types needed to support MQL semantics for pushdown. Each BSON type has
 * a corresponding field name so that we can preserve type information for project, plus two meta
 * fields FALLBACK_MARKER and PROJECTION.
 */
public enum MqlField {
  MIN_KEY,
  UNDEFINED,
  NULL,
  INT32,
  INT64,
  DOUBLE,
  STRING,
  SYMBOL,
  BOOLEAN,
  BINARY,
  REGULAR_EXP,
  JAVASCRIPT,
  DB_REF,
  JAVASCRIPT_WITH_SCOPE,
  DATE_TIME,
  TIMESTAMP,
  OBJECT_ID,
  MAX_KEY,
  /** Field indicates value could not be indexed. */
  FALLBACK_MARKER,
  /** Field used to store pre-computed projections. */
  PROJECTION;

  private final String prefix;

  MqlField() {
    this.prefix = "$mqlBson:" + this.name().toLowerCase(Locale.ENGLISH) + '/';
  }

  public String getFieldName(FieldPath path) {
    return this.prefix + path;
  }

  public String getFieldName(String path) {
    return this.prefix + path;
  }
}
