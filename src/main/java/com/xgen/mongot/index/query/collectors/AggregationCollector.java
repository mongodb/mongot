package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Collector for search aggregation. Holds the query predicate and the top-level recursive
 * aggregation-facet definitions keyed by facet name.
 *
 * <p>Constructed by {@link #fromBson} only for aggregation-capable requests (see {@link
 * #isAggregationCapable}); all-legacy facet requests are still parsed by {@link FacetCollector}.
 */
public record AggregationCollector(
    Operator operator, Map<String, AggregationFacetDefinition> facets) implements Collector {

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

  /** Deserializes an aggregation collector from the {@code facet} collector document. */
  public static AggregationCollector fromBson(DocumentParser parser, boolean allow10k)
      throws BsonParseException {
    return new AggregationCollector(
        parser.getField(Fields.OPERATOR).unwrap(),
        parser.getField(allow10k ? Fields.FACETS_10K : Fields.FACETS).unwrap());
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
    return new AggregationCollector(operator, facets);
  }

  @Override
  public BsonValue collectorToBson() {
    // Encodes the facet-collector document (operator + facets).
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
