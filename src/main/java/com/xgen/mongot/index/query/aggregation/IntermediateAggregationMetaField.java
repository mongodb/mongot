package com.xgen.mongot.index.query.aggregation;

import org.bson.BsonDocument;
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

  private static String describeValue(BsonValue value) {
    // BsonString#toString() renders as BsonString{value='NYC'}; quote the raw value for errors.
    if (value.isString()) {
      return value.asString().getValue();
    }
    return value.toString();
  }
}
