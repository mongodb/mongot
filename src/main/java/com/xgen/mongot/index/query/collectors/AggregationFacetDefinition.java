package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * One recursive node of the Search Facet Extension -- an entry under {@code facet.facets.<name>}
 * and, recursively, its nested {@code facets}. The node name is the map key, supplied by the
 * enclosing map field.
 *
 * <p>The grouping payload ({@code type}/{@code path}/{@code numBuckets}/{@code boundaries}/{@code
 * default}) is parsed by the existing {@link FacetDefinition}, so the group semantics -- and their
 * validation -- stay identical to legacy facets. On top of that, a node may additionally carry:
 *
 * <ul>
 *   <li>an {@code aggregations} map of named {@link MetricSpec} metric leaves -- leaf-only, and
 *   <li>a nested {@code facets} map of child {@code AggregationFacetDefinition} subtrees.
 * </ul>
 *
 * <p>{@code aggregations} and a non-empty {@code facets} are mutually exclusive on the same node
 * (aggregations are leaf-only); the combination is rejected at parse time. A node with neither is a
 * legacy single-field facet: the canonical constructor materializes its implicit {@code count}
 * metric, so the parsed model always carries the metric explicitly.
 *
 * <p>An intermediate node's {@code facets} may currently hold only a single nested branch: the
 * sharded merge planner assumes at most one nested grouping child per level, so multiple branches
 * at an intermediate level are rejected at parse time (deferred, not a blocker for private
 * preview/GA). Multiple <em>top-level</em> facets remain allowed -- those are independent facet
 * trees, parsed by {@link AggregationCollector}.
 */
public record AggregationFacetDefinition(
    FacetDefinition group,
    FieldPath path,
    Map<String, MetricSpec> aggregations,
    Map<String, AggregationFacetDefinition> facets)
    implements DocumentEncodable {

  /**
   * A node with neither {@code aggregations} nor nested {@code facets} is a legacy single-field
   * facet, which per the syntax doc is a {@code count} leaf; materialize the implicit metric here
   * so the model always carries it explicitly and lowering has no special case. Named {@code count}
   * -- the legacy bucket field name -- so bucket docs keep the legacy facet response shape.
   */
  public AggregationFacetDefinition(
      FacetDefinition group,
      FieldPath path,
      Map<String, MetricSpec> aggregations,
      Map<String, AggregationFacetDefinition> facets) {
    this.group = group;
    this.path = path;
    this.aggregations =
        aggregations.isEmpty() && facets.isEmpty()
            ? Map.of(Operator.COUNT.registryKey(), new MetricSpec(Operator.COUNT, Optional.empty()))
            : aggregations;
    this.facets = facets;
  }

  /**
   * The field names that make a node aggregation-capable. Single source of truth shared with the
   * shallow discriminator pre-scan ({@link AggregationCollector#isAggregationCapable}) and this
   * class's own field builders and encoder, so the routing decision cannot drift from what actually
   * parses.
   */
  static final String AGGREGATIONS_FIELD = "aggregations";

  static final String FACETS_FIELD = "facets";

  private static final class Fields {
    private static final Field.Optional<Map<String, MetricSpec>> AGGREGATIONS =
        Field.builder(AGGREGATIONS_FIELD)
            .classField(MetricSpec::fromBson)
            .disallowUnknownFields()
            .asOrderedMap()
            .mustNotBeEmpty()
            .optional()
            .noDefault();

    // Nested child facets recurse into this same parser; one descriptor per bucket-limit mode so
    // nothing is rebuilt per parse call.
    private static final Field.Optional<Map<String, AggregationFacetDefinition>> FACETS_1K =
        facetsField(false);
    private static final Field.Optional<Map<String, AggregationFacetDefinition>> FACETS_10K =
        facetsField(true);

    private static Field.Optional<Map<String, AggregationFacetDefinition>> facetsField(
        boolean allow10k) {
      return Field.builder(FACETS_FIELD)
          .classField((DocumentParser p) -> AggregationFacetDefinition.fromBson(p, allow10k))
          .disallowUnknownFields()
          .asOrderedMap()
          .mustNotBeEmpty()
          .optional()
          .noDefault();
    }
  }

  static AggregationFacetDefinition fromBson(DocumentParser parser, boolean allow10k)
      throws BsonParseException {
    // Reuse legacy facet parsing for the grouping payload (type/path/numBuckets/boundaries).
    // The path string converts to a FieldPath at parse time, as operator paths do; FacetDefinition
    // itself stays untouched, so the conversion happens here rather than inside it.
    FacetDefinition group = FacetDefinition.fromBson(parser, allow10k);
    FieldPath path = FieldPath.parse(group.path());

    Map<String, MetricSpec> aggregations =
        parser.getField(Fields.AGGREGATIONS).unwrap().orElseGet(Map::of);
    // "facets" is absent (empty here) on a leaf node: only intermediate nodes carry nested facets.
    Map<String, AggregationFacetDefinition> facets =
        parser
            .getField(allow10k ? Fields.FACETS_10K : Fields.FACETS_1K)
            .unwrap()
            .orElseGet(Map::of);

    if (!aggregations.isEmpty() && !facets.isEmpty()) {
      return parser
          .getContext()
          .handleSemanticError(
              "facet cannot have both aggregations and nested facets; aggregations are leaf-only");
    }

    // An intermediate node may fan out into only a single nested facet branch for now: the sharded
    // merge planner assumes at most one nested grouping child per level. Multiple branches at an
    // intermediate level (which would let sibling accumulators share heap) are deferred, so reject
    // them at parse time rather than silently mis-plan on sharded collections.
    // TODO(CLOUDP-425979): support multiple nested facet branches at an intermediate level once
    // the sharded merge planner can plan more than one nested grouping child per level.
    if (facets.size() > 1) {
      return parser
          .getContext()
          .handleSemanticError(
              "facet cannot have more than one nested facet branch; multiple branches at an"
                  + " intermediate level are not yet supported");
    }

    return new AggregationFacetDefinition(group, path, aggregations, facets);
  }

  /** A node with a nested {@code facets} map is an intermediate grouping node, not a leaf. */
  public boolean isLeaf() {
    return this.facets.isEmpty();
  }

  /**
   * Whether this node or any descendant carries a {@code first}/{@code last} metric — the only
   * metrics whose semantics depend on the global {@code sort}. Used to reject a {@code sort} that
   * would otherwise be silently ignored.
   */
  public boolean usesFirstOrLast() {
    return this.aggregations.values().stream().anyMatch(MetricSpec::usesSortOrder)
        || this.facets.values().stream().anyMatch(AggregationFacetDefinition::usesFirstOrLast);
  }

  @VisibleForTesting
  public static AggregationFacetDefinition create(
      FacetDefinition group,
      Map<String, MetricSpec> aggregations,
      Map<String, AggregationFacetDefinition> facets) {
    return new AggregationFacetDefinition(
        group, FieldPath.parse(group.path()), aggregations, facets);
  }

  /**
   * Emits the normalized wire form, which may differ from the input BSON: a legacy-shaped leaf that
   * omitted {@code aggregations} is parsed with an implicit {@code aggregations.count}, so {@code
   * toBson()} emits that {@code aggregations} block even though the source document did not.
   */
  @Override
  public BsonDocument toBson() {
    // The group payload (type/path/numBuckets/boundaries/default) round-trips through
    // FacetDefinition; aggregations and nested facets (mutually exclusive) are appended on top.
    BsonDocument document = this.group.toBson();
    if (!this.aggregations.isEmpty()) {
      BsonDocument aggregations = new BsonDocument();
      this.aggregations.forEach((name, metric) -> aggregations.append(name, metric.toBson()));
      document.append(AGGREGATIONS_FIELD, aggregations);
    }
    if (!this.facets.isEmpty()) {
      BsonDocument facets = new BsonDocument();
      this.facets.forEach((name, facet) -> facets.append(name, facet.toBson()));
      document.append(FACETS_FIELD, facets);
    }
    return document;
  }
}
