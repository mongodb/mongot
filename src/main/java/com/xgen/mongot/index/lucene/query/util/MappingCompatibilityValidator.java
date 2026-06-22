package com.xgen.mongot.index.lucene.query.util;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryTimeMappingChecks;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class MappingCompatibilityValidator {

  /**
   * Validates that the configured mappings for the index could serve query with the specified
   * datatype.
   */
  public static void validate(
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      QueryTimeMappingChecks mappingChecks,
      ValueType type)
      throws InvalidQueryException {

    boolean hasFilter = mappingChecks.supportsFilter();
    switch (type) {
      case DATE ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsDate(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "date");
      case NUMBER ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsNumber(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "number");
      case STRING ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsToken(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "token");
      case BOOLEAN ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsBoolean(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "boolean");
      case OBJECT_ID ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsObjectId(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "objectId");
      case UUID ->
          InvalidQueryException.validate(
              mappingChecks.indexedAsUuid(path, embeddedRoot),
              "Path '%s' needs to be indexed as %s",
              path,
              hasFilter ? "filter" : "uuid");
      case NULL -> {
        // Do nothing since null values are indexed automatically for all fields
      }
    }
  }

  /**
   * Validates that a field used in an {@code $exists} filter is actually present in the index. For
   * vector indexes this means the field must be defined as a {@code filter} field; for search
   * indexes the default {@link QueryTimeMappingChecks#indexedForExists} returns {@code true} so no
   * additional check is needed.
   */
  public static void validateExists(
      FieldPath path, Optional<FieldPath> embeddedRoot, QueryTimeMappingChecks mappingChecks)
      throws InvalidQueryException {
    boolean hasFilter = mappingChecks.supportsFilter();
    InvalidQueryException.validate(
        mappingChecks.indexedForExists(path, embeddedRoot),
        "Path '%s' needs to be indexed as %s",
        path,
        hasFilter ? "filter" : "token");
  }
}
