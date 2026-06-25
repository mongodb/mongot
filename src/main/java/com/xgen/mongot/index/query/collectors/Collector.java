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

  class Fields {
    public static final Field.Optional<FacetCollector> FACET =
        Field.builder("facet")
            .classField(FacetCollector::fromBson, FacetCollector::collectorToBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<FacetCollector> FACET_10K_ALLOWED =
        Field.builder("facet")
            .classField(
                FacetCollector::fromBson10kAllowed, FacetCollector::collectorToBson10kAllowed)
            .disallowUnknownFields()
            .optional()
            .noDefault();
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
          throw new UnsupportedOperationException(
              "AggregationCollector BSON serialization is not implemented");
    };
  }

  BsonValue collectorToBson();

  static Optional<Collector> atMostOneFromBson(DocumentParser parser, boolean allow10k)
      throws BsonParseException {
    return parser
        .getGroup()
        .atMostOneOf(parser.getField(allow10k ? Fields.FACET_10K_ALLOWED : Fields.FACET))
        .map(c -> (Collector) c);
  }

  Type getType();

  /** Returns the inner operator associated with this collector, if any. */
  Optional<Operator> getOperator();
}
