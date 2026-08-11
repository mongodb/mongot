package com.xgen.mongot.index.query.aggregation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Positional facet ({@code f0}) and metric ({@code m0}) aliases for the sharded merge wire format
 * and merge pipeline. Built once from the {@link TopAggregationSpec} tree so shard emit and mongod
 * merge agree on names.
 */
public final class ShardMergeAliasPlan {

  private final Map<String, String> facetUserToAlias;
  private final Map<String, String> facetAliasToUser;
  private final Map<String, String> wirePartialToAlias;

  private ShardMergeAliasPlan(
      Map<String, String> facetUserToAlias,
      Map<String, String> facetAliasToUser,
      Map<String, String> wirePartialToAlias) {
    this.facetUserToAlias = Map.copyOf(facetUserToAlias);
    this.facetAliasToUser = Map.copyOf(facetAliasToUser);
    this.wirePartialToAlias = Map.copyOf(wirePartialToAlias);
  }

  /**
   * Builds the alias plan by walking {@code spec} in the same order as {@link
   * #buildShardMergePipeline}.
   */
  public static ShardMergeAliasPlan from(TopAggregationSpec spec, List<Integer> sortDirections) {
    Builder builder = new Builder();
    for (AggregationSpec child : spec.children()) {
      builder.registerFacet(child.name());
      walk(child, builder, sortDirections);
    }
    return builder.build();
  }

  public String facetAlias(String userFacetName) {
    return this.facetUserToAlias.get(userFacetName);
  }

  public String facetUserName(String facetAlias) {
    return this.facetAliasToUser.get(facetAlias);
  }

  public String wirePartialAlias(String wirePartialName) {
    return this.wirePartialToAlias.get(wirePartialName);
  }

  Map<String, String> wirePartialToAliasMap() {
    return this.wirePartialToAlias;
  }

  /** Rewrites flat intermediate rows to use facet and metric aliases on the wire. */
  public BsonArray rewriteIntermediateBson(BsonArray docs) {
    BsonArray rewritten = new BsonArray();
    for (BsonValue value : docs) {
      BsonDocument source = value.asDocument();
      BsonDocument doc = new BsonDocument();
      for (Map.Entry<String, BsonValue> entry : source.entrySet()) {
        String field = entry.getKey();
        if (IntermediateAggregationMetaField.GROUP_NAME.equals(field)) {
          String userFacet = entry.getValue().asString().getValue();
          doc.append(field, new BsonString(facetAlias(userFacet)));
        } else {
          String alias = this.wirePartialToAlias.get(field);
          doc.append(alias != null ? alias : field, entry.getValue());
        }
      }
      rewritten.add(doc);
    }
    return rewritten;
  }

  private static void walk(AggregationSpec spec, Builder builder, List<Integer> sortDirections) {
    if (spec instanceof GroupAggregationSpec group) {
      List<GroupAggregationSpec> groupChildren =
          group.children().stream()
              .filter(GroupAggregationSpec.class::isInstance)
              .map(GroupAggregationSpec.class::cast)
              .toList();
      if (groupChildren.size() == 1) {
        builder.registerFacet(groupChildren.getFirst().name());
      }
      for (AggregationSpec child : group.children()) {
        walk(child, builder, sortDirections);
      }
      return;
    }
    if (spec instanceof OperatorAggregationSpec operator) {
      for (String wirePartial : ShardMergeWireFields.wirePartials(operator, sortDirections)) {
        builder.registerWirePartial(wirePartial);
      }
    }
  }

  private static final class Builder {
    private final LinkedHashMap<String, String> facetUserToAlias = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> facetAliasToUser = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> wirePartialToAlias = new LinkedHashMap<>();

    private int nextFacetIndex;
    private int nextMetricIndex;

    void registerFacet(String userName) {
      this.facetUserToAlias.computeIfAbsent(
          userName,
          ignored -> {
            String alias = FacetMetricAliases.facet(this.nextFacetIndex++);
            this.facetAliasToUser.put(alias, userName);
            return alias;
          });
    }

    void registerWirePartial(String wireName) {
      this.wirePartialToAlias.computeIfAbsent(
          wireName, ignored -> FacetMetricAliases.metric(this.nextMetricIndex++));
    }

    ShardMergeAliasPlan build() {
      return new ShardMergeAliasPlan(
          this.facetUserToAlias, this.facetAliasToUser, this.wirePartialToAlias);
    }
  }
}
