package com.xgen.mongot.index.query.aggregation;

import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/** Rewrites aggregation expressions that reference wire partial field names. */
final class ShardMergeAliasExpressions {

  private ShardMergeAliasExpressions() {}

  static BsonValue rewriteWireRefs(BsonValue expr, Map<String, String> wirePartialToAlias) {
    if (expr.isString()) {
      return rewriteStringRef(expr.asString().getValue(), wirePartialToAlias);
    }
    if (expr.isArray()) {
      BsonArray rewritten = new BsonArray();
      for (BsonValue value : expr.asArray()) {
        rewritten.add(rewriteWireRefs(value, wirePartialToAlias));
      }
      return rewritten;
    }
    if (!expr.isDocument()) {
      return expr;
    }
    BsonDocument rewritten = new BsonDocument();
    for (Map.Entry<String, BsonValue> entry : expr.asDocument().entrySet()) {
      String key = entry.getKey();
      rewritten.append(
          resolveWireKey(key, wirePartialToAlias),
          rewriteWireRefs(entry.getValue(), wirePartialToAlias));
    }
    return rewritten;
  }

  private static String resolveWireKey(String key, Map<String, String> wirePartialToAlias) {
    String alias = wirePartialToAlias.get(key);
    if (alias != null) {
      return alias;
    }
    for (Map.Entry<String, String> entry : wirePartialToAlias.entrySet()) {
      String wire = entry.getKey();
      if (key.startsWith(wire + ".")) {
        return entry.getValue() + key.substring(wire.length());
      }
    }
    return key;
  }

  private static BsonValue rewriteStringRef(String value, Map<String, String> wirePartialToAlias) {
    if (!value.startsWith("$")) {
      return new BsonString(value);
    }
    if (value.startsWith("$$")) {
      return rewriteBucketRef(value, wirePartialToAlias);
    }
    String wireName = value.substring(1);
    String alias = resolveWireKey(wireName, wirePartialToAlias);
    return new BsonString("$" + alias);
  }

  private static BsonValue rewriteBucketRef(String value, Map<String, String> wirePartialToAlias) {
    int dot = value.indexOf('.', 2);
    if (dot < 0) {
      return new BsonString(value);
    }
    String prefix = value.substring(0, dot + 1);
    String wireName = value.substring(dot + 1);
    String alias = resolveWireKey(wireName, wirePartialToAlias);
    return new BsonString(prefix + alias);
  }
}
