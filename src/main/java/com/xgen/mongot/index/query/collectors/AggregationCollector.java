package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Collector for search aggregation. Holds the query predicate, the top-level recursive
 * aggregation-facet definitions keyed by facet name, and the optional global {@link SortSpec}
 * borrowed from the enclosing {@code $search}/{@code $searchMeta} query.
 *
 * <p>Constructed by {@link #fromBson} only for aggregation-capable requests (see {@link
 * #isAggregationCapable}); all-legacy facet requests are still parsed by {@link FacetCollector}.
 *
 * <p>The {@code sortSpec} defines the total order used to pick the {@code first}/{@code last}
 * document per bucket. It is parsed as a sibling of the collector (not inside {@code facet}) and
 * attached afterwards via {@link #withSortSpec}, since a single global sort keeps execution to one
 * index scan per query.
 */
public record AggregationCollector(
    Operator operator, Map<String, AggregationFacetDefinition> facets, Optional<SortSpec> sortSpec)
    implements Collector {

  private static final class Fields {
    private static final Field.WithDefault<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::parseForFacetCollector)
            .disallowUnknownFields()
            .optional()
            .withDefault(AllDocumentsOperator.INSTANCE);

    // One descriptor per bucket-limit mode, mirroring FacetCollector's FACETS / FACETS_10K, so
    // nothing is rebuilt per parse or encode call.
    private static final Field.Required<Map<String, AggregationFacetDefinition>> FACETS =
        facetsField(false);
    private static final Field.Required<Map<String, AggregationFacetDefinition>> FACETS_10K =
        facetsField(true);

    private static Field.Required<Map<String, AggregationFacetDefinition>> facetsField(
        boolean allow10k) {
      return Field.builder(AggregationFacetDefinition.FACETS_FIELD)
          .classField(
              (DocumentParser parser) -> AggregationFacetDefinition.fromBson(parser, allow10k))
          .disallowUnknownFields()
          // Declared facet order is preserved through lowering and serialization.
          .asOrderedMap()
          .mustNotBeEmpty()
          .required();
    }
  }

  /**
   * Deserializes an aggregation collector from the {@code facet} collector document. The global
   * {@code sortSpec} lives outside the {@code facet} document and is attached later via {@link
   * #withSortSpec} once the enclosing query has parsed it.
   */
  public static AggregationCollector fromBson(DocumentParser parser, boolean allow10k)
      throws BsonParseException {
    return new AggregationCollector(
        parser.getField(Fields.OPERATOR).unwrap(),
        parser.getField(allow10k ? Fields.FACETS_10K : Fields.FACETS).unwrap(),
        Optional.empty());
  }

  /** Returns a copy of this collector with the given global sort spec attached. */
  public AggregationCollector withSortSpec(Optional<SortSpec> sortSpec) {
    return new AggregationCollector(this.operator, this.facets, sortSpec);
  }

  /**
   * Whether any facet tree in this collector carries a {@code first}/{@code last} metric. A global
   * {@code sort} is rejected when none does, since it would otherwise be silently ignored.
   */
  public boolean usesFirstOrLast() {
    return this.facets.values().stream().anyMatch(AggregationFacetDefinition::usesFirstOrLast);
  }

  /**
   * Shallow, non-parsing pre-scan of a search query document to apply the legacy-vs-aggregation
   * discriminator: returns {@code true} iff any top-level {@code facet.facets} entry carries an
   * {@code aggregations} or nested {@code facets} key. Used at the collector-selection gate to
   * decide whether to construct a {@link FacetCollector} (all-legacy) or an {@code
   * AggregationCollector}. Only checks for key presence -- full field validation happens in {@link
   * #fromBson}.
   */
  public static boolean isAggregationCapable(BsonDocument searchQueryDocument) {
    BsonValue facet = searchQueryDocument.get(Collector.FACET_FIELD);
    if (facet == null || !facet.isDocument()) {
      return false;
    }
    BsonValue facets = facet.asDocument().get(AggregationFacetDefinition.FACETS_FIELD);
    if (facets == null || !facets.isDocument()) {
      return false;
    }
    for (BsonValue entry : facets.asDocument().values()) {
      if (entry.isDocument()) {
        BsonDocument node = entry.asDocument();
        if (node.containsKey(AggregationFacetDefinition.AGGREGATIONS_FIELD)
            || node.containsKey(AggregationFacetDefinition.FACETS_FIELD)) {
          return true;
        }
      }
    }
    return false;
  }

  @VisibleForTesting
  public static AggregationCollector create(
      Operator operator, Map<String, AggregationFacetDefinition> facets) {
    return new AggregationCollector(operator, facets, Optional.empty());
  }

  @Override
  public BsonValue collectorToBson() {
    // Encodes the facet-collector document (operator + facets). The global sortSpec is a sibling of
    // the collector, serialized by the enclosing query, so it is intentionally not emitted here.
    // allow10k does not affect encoding (it only widens parse-time bucket-count bounds).
    // Omit the operator when it is the AllDocumentsOperator default: it is not a user-writable
    // operator, so emitting it would produce a document that fails to re-parse.
    return BsonDocumentBuilder.builder()
        .fieldOmitDefaultValue(Fields.OPERATOR, this.operator)
        .field(Fields.FACETS_10K, this.facets)
        .build();
  }

  @Override
  public Type getType() {
    return Type.AGGREGATION;
  }

  @Override
  public Optional<Operator> getOperator() {
    return Optional.of(this.operator);
  }
}
