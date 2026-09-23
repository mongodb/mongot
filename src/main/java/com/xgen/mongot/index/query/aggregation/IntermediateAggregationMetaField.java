package com.xgen.mongot.index.query.aggregation;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
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

  /**
   * Per-bucket document count on the emitting shard. Summed in stage-1 {@code $group} as {@code
   * count_value} and propagated through nested facet-arm {@code $group}s for terms top-N.
   */
  public static final String BUCKET_DOC_COUNT = "count";

  /** Since Search aggregation scans through all documents the count is always the total count. */
  public static final String BUCKET_DOC_COUNT_TYPE = "total";

  /**
   * Field names within merged {@code $group} {@code _id} documents.
   *
   * <p>Must not use the {@code $meta/} prefix: expression-object keys starting with {@code $} are
   * reserved for aggregation operators, so a composite {@code _id} cannot use {@link #GROUP_NAME}
   * or {@link #KEYS} as literal field names.
   */
  public static final String ID_GROUP_NAME = "group_name";

  public static final String ID_KEYS = "keys";

  /**
   * Field name intermediate bucket documents fields reserved by the query engine. User-supplied
   * aggregation names must not collide with these.
   */
  public static final List<String> RESERVED_BUCKET_FIELDS = List.of(BUCKET_DOC_COUNT);

  private IntermediateAggregationMetaField() {}

  /** Rejects metric partials or other child fields that claim reserved intermediate metadata. */
  public static void assertNoReservedFields(BsonDocument doc) {
    if (doc.containsKey(GROUP_NAME)) {
      throw new IllegalStateException("Document must not set routing field " + GROUP_NAME);
    }
    if (doc.containsKey(KEYS)) {
      throw new IllegalStateException("Document must not set grouping-keys field " + KEYS);
    }
    if (doc.containsKey(BUCKET_DOC_COUNT)) {
      throw new IllegalStateException(
          "Document must not set bucket doc-count field " + BUCKET_DOC_COUNT);
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
    BsonValue actual = idFieldValue(idFieldNameForGroupId(fieldName), expected.isNull());
    return new BsonDocument(
        "$expr", new BsonDocument("$eq", new BsonArray(List.of(actual, expected))));
  }

  private static BsonValue idFieldValue(String idFieldName, boolean coalesceMissingToNull) {
    BsonValue fieldValue = fieldExpression(idFieldName, "$_id");
    if (!coalesceMissingToNull) {
      return fieldValue;
    }
    return new BsonDocument("$ifNull", new BsonArray(List.of(fieldValue, new BsonNull())));
  }

  private static String idFieldNameForGroupId(String intermediateFieldName) {
    if (GROUP_NAME.equals(intermediateFieldName)) {
      return ID_GROUP_NAME;
    }
    if (KEYS.equals(intermediateFieldName)) {
      return ID_KEYS;
    }
    return intermediateFieldName;
  }

  private static BsonDocument fieldExpression(String fieldName, String input) {
    return new BsonDocument(
        "$getField",
        new BsonDocument()
            .append("field", fieldNameArgument(fieldName))
            .append("input", new BsonString(input)));
  }

  /**
   * {@code $getField}'s {@code field} argument must be a string expression. Names with a {@code $}
   * prefix (e.g. {@link #GROUP_NAME}) are field-path syntax in aggregation and must be wrapped in
   * {@code $literal}.
   */
  private static BsonValue fieldNameArgument(String fieldName) {
    if (fieldName.startsWith("$")) {
      return new BsonDocument("$literal", new BsonString(fieldName));
    }
    return new BsonString(fieldName);
  }

  /** Aggregation expression for one grouping key alias on the current facet-arm document. */
  public static BsonDocument idKeyExpression(String keyAlias, String idInput) {
    return new BsonDocument(
        "$getField",
        new BsonDocument()
            .append("field", fieldNameArgument(keyAlias))
            .append("input", idKeysExpression(idInput)));
  }

  private static BsonDocument idKeysExpression(String idInput) {
    return fieldExpression(idKeysFieldName(idInput), idInput);
  }

  private static String idKeysFieldName(String idInput) {
    if ("$_id".equals(idInput) || idInput.endsWith("._id")) {
      return ID_KEYS;
    }
    return KEYS;
  }

  /** Field path for one grouping key alias inside a flat {@code $map} bucket row. */
  public static String groupedKeyFieldPath(String keyAlias) {
    return "$$b._id." + ID_KEYS + "." + keyAlias;
  }

  /** Aggregation expression for one grouping key on the current facet-arm document. */
  public static String idKeysFieldPath(String keyName) {
    return "$_id." + ID_KEYS + "." + keyName;
  }

  private static String describeValue(BsonValue value) {
    // BsonString#toString() renders as BsonString{value='NYC'} - quote the raw value for errors.
    if (value.isString()) {
      return value.asString().getValue();
    }
    return value.toString();
  }
}
