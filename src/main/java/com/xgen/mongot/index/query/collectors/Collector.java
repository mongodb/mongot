package com.xgen.mongot.index.query.collectors;

import static java.util.stream.Collectors.joining;

import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Arrays;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public sealed interface Collector extends DocumentEncodable
    permits FacetCollector, AggregationCollector {

  /** The {@code facet} collector wire field name, shared by every {@code facet} field builder. */
  String FACET_FIELD = "facet";

  class Fields {
    public static final Field.Optional<FacetCollector> FACET =
        Field.builder(FACET_FIELD)
            .classField(FacetCollector::fromBson, FacetCollector::collectorToBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<FacetCollector> FACET_10K_ALLOWED =
        Field.builder(FACET_FIELD)
            .classField(
                FacetCollector::fromBson10kAllowed, FacetCollector::collectorToBson10kAllowed)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    // The same "facet" wire field parsed into an AggregationCollector (aggregation-capable
    // requests under the SEARCH_AGGREGATION flag); one descriptor per bucket-limit mode, mirroring
    // FACET / FACET_10K_ALLOWED above.
    static final Field.Optional<AggregationCollector> FACET_AGGREGATION =
        facetAggregationField(false);
    static final Field.Optional<AggregationCollector> FACET_AGGREGATION_10K =
        facetAggregationField(true);

    private static Field.Optional<AggregationCollector> facetAggregationField(boolean allow10k) {
      return Field.builder(FACET_FIELD)
          .classField(
              (DocumentParser p) -> AggregationCollector.fromBson(p, allow10k),
              AggregationCollector::collectorToBson)
          .disallowUnknownFields()
          .optional()
          .noDefault();
    }
  }

  // Aggregation isn't ready: so it is not user-selectable yet and is excluded from this
  // user-facing list of valid collectors.
  public static final String ALL_COLLECTORS =
      Arrays.stream(Type.values())
          .filter(type -> type != Type.AGGREGATION)
          .map(Type::getName)
          .collect(joining(", "));

  /**
   * Collector Type Enumeration.
   *
   * <p>Please keep Type enumeration in alphabetical order.
   *
   * <p>Although currently facet is the only existing collector, we'll follow a structure parallel
   * to that in Operator.java (see it for reference)
   */
  enum Type {
    /** All collector types (with their associated name in MQL) */
    FACET("facet"),

    /** Aggregation collector type */
    AGGREGATION("aggregation");

    private final String name;

    /**
     * Constructs a {@code Type} enum with provided user-facing name.
     *
     * @param name The user-facing name associated with enum.
     */
    Type(String name) {
      this.name = name;
    }

    /**
     * This method is distinct from the built-in {@code name()} method of Java enums. While {@code
     * name()} returns the declared name of the enum constant, {@code getName()} returns the
     * associated user-facing name defined on creation.
     *
     * <p>Example usage: Collector.Type.getName() will return the text name of some collector object
     * (ex: FACET.getName() will return "facet")
     *
     * @return The user-facing name for this {@code Type}, as should be entered in MQL
     */
    String getName() {
      return this.name;
    }
  }

  @Override
  default BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder();
    return switch (this) {
      case FacetCollector facetCollector ->
          builder.field(Fields.FACET_10K_ALLOWED, Optional.of(facetCollector)).build();
      case AggregationCollector aggregationCollector ->
          builder.field(Fields.FACET_AGGREGATION_10K, Optional.of(aggregationCollector)).build();
    };
  }

  BsonValue collectorToBson();

  /**
   * Parses the {@code facet} collector, if present. When {@code useAggregation} is true (the
   * caller's shape pre-scan found an aggregation-capable request under an enabled feature flag),
   * the {@code facet} field is parsed into an {@link AggregationCollector}; otherwise it is parsed
   * into a {@link FacetCollector} exactly as before, leaving the legacy path untouched.
   */
  static Optional<Collector> atMostOneFromBson(
      DocumentParser parser, boolean allow10k, boolean useAggregation) throws BsonParseException {
    if (useAggregation) {
      return parser
          .getGroup()
          .atMostOneOf(
              parser.getField(allow10k ? Fields.FACET_AGGREGATION_10K : Fields.FACET_AGGREGATION))
          .map(c -> (Collector) c);
    }
    return parser
        .getGroup()
        .atMostOneOf(parser.getField(allow10k ? Fields.FACET_10K_ALLOWED : Fields.FACET))
        .map(c -> (Collector) c);
  }

  Type getType();

  /** Returns the inner operator associated with this collector, if any. */
  Optional<Operator> getOperator();
}
