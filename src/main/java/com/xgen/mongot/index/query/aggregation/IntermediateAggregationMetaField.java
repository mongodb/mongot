package com.xgen.mongot.index.query.aggregation;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Reserved field names on flat intermediate aggregation documents emitted by shards.
 *
 * <p>Uses the same {@code $meta/} prefix convention as {@link
 * com.xgen.mongot.index.lucene.field.FieldName.MetaField} so routing metadata cannot collide with
 * user grouping keys or metric partial field names.
 */
public final class IntermediateAggregationMetaField {

  private static final String PREFIX = "$meta/";

  /** Top-level aggregation branch name used to route rows into merge-pipeline facet arms. */
  public static final String GROUP_NAME = PREFIX + "group_name";

  /** Grouping key path values for one leaf bucket row. */
  public static final String KEYS = PREFIX + "keys";

  private IntermediateAggregationMetaField() {}

  /** Rejects metric partials or other child fields that claim reserved intermediate metadata. */
  public static void assertNoReservedFields(BsonDocument doc) {
    if (doc.containsKey(GROUP_NAME)) {
      throw new IllegalStateException("Document must not set routing field " + GROUP_NAME);
    }
    if (doc.containsKey(KEYS)) {
      throw new IllegalStateException("Document must not set grouping-keys field " + KEYS);
    }
  }

  /** Merges parent and child grouping-key documents, failing on conflicting values. */
  public static BsonDocument mergeGroupingKeys(BsonDocument parentKeys, BsonDocument childKeys) {
    for (String key : childKeys.keySet()) {
      if (parentKeys.containsKey(key) && !parentKeys.get(key).equals(childKeys.get(key))) {
        throw new IllegalStateException(
            "Conflicting grouping key '"
                + key
                + "': \""
                + describeValue(parentKeys.get(key))
                + "\" vs \""
                + describeValue(childKeys.get(key))
                + "\"");
      }
    }
    BsonDocument merged = new BsonDocument();
    merged.putAll(parentKeys);
    merged.putAll(childKeys);
    return merged;
  }

  /**
   * {@code $getField} expression reading {@code fieldName} from the current merge input document.
   *
   * <p>Required because intermediate field names use the {@code $meta/} prefix and cannot be
   * referenced with a plain {@code $field} path in a {@code $group} stage.
   */
  public static BsonDocument rootFieldExpression(String fieldName) {
    return fieldExpression(fieldName, "$$ROOT");
  }

  /**
   * {@code $match} stage comparing a {@code $meta/}-prefixed field inside {@code $_id}.
   *
   * <p>Plain {@code $match} query paths cannot reference these field names literally on mongod; use
   * {@code $expr} + {@code $getField} instead.
   */
  public static BsonDocument matchIdFieldStage(String fieldName, BsonValue expected) {
    return new BsonDocument("$match", matchIdFieldExpression(fieldName, expected));
  }

  static BsonDocument matchIdFieldExpression(String fieldName, BsonValue expected) {
    return new BsonDocument(
        "$expr",
        new BsonDocument(
            "$eq", new BsonArray(List.of(fieldExpression(fieldName, "$_id"), expected))));
  }

  private static BsonDocument fieldExpression(String fieldName, String input) {
    return new BsonDocument(
        "$getField",
        new BsonDocument()
            .append("field", new BsonString(fieldName))
            .append("input", new BsonString(input)));
  }

  /** Aggregation expression for one grouping key inside a merged {@code $group} result row. */
  public static String groupedKeyFieldPath(String keyName) {
    return "$$b._id['" + KEYS + "']." + keyName;
  }

  /** Aggregation expression for one grouping key on the current facet-arm document. */
  public static String idKeysFieldPath(String keyName) {
    return "$_id['" + KEYS + "']." + keyName;
  }

  private static String describeValue(BsonValue value) {
    // BsonString#toString() renders as BsonString{value='NYC'} - quote the raw value for errors.
    if (value.isString()) {
      return value.asString().getValue();
    }
    return value.toString();
  }
}
