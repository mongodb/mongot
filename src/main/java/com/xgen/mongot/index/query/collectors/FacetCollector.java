package com.xgen.mongot.index.query.collectors;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonValue;

/**
 * FacetCollector
 *
 * @param facetDefinitions Keyed by facet name.
 */
public record FacetCollector(
    Operator operator,
    Map<String, FacetDefinition> facetDefinitions,
    Optional<DrillSidewaysInfo> drillSidewaysInfo)
    implements Collector {

  private static class Fields {
    private static final Field.WithDefault<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::parseForFacetCollector)
            .disallowUnknownFields()
            .optional()
            .withDefault(AllDocumentsOperator.INSTANCE);
    private static final Field.Required<Map<String, FacetDefinition>> FACETS =
        Field.builder("facets")
            .classField(parser -> FacetDefinition.fromBson(parser))
            .disallowUnknownFields()
            .asMap()
            .mustNotBeEmpty()
            .required();
    private static final Field.Required<Map<String, FacetDefinition>> FACETS_10K =
        Field.builder("facets")
            .classField(parser -> FacetDefinition.fromBson(parser, true))
            .disallowUnknownFields()
            .asMap()
            .mustNotBeEmpty()
            .required();
  }

  static boolean containsDoesNotAffect(Operator op) {
    return switch (op) {
      // To avoid recursing through the operator twice, once here to check for doesNotAffect and
      // once in DrillSidewaysInfoBuilder to build operator maps, we set doesNotAffect to true for
      // any query with a nest-able operators (e.g. Compound, EmbeddedDoc) and send the operator to
      // DrillSidewaysInfoBuilder.buildFacetOperators. If the query does not contain doesNotAffect
      // at any interior operator level, buildFacetOperators() creates a non-drill sideways query.
      case CompoundOperator compoundOp -> true;
      case EmbeddedDocumentOperator embeddedDocOp -> true;
      case EqualsOperator equalsOp -> equalsOp.doesNotAffectDefined();
      case InOperator inOp -> inOp.doesNotAffectDefined();
      case RangeOperator rangeOp -> rangeOp.doesNotAffectDefined();

      // Add any new operators here with explicit handling for doesNotAffect
      case AllDocumentsOperator ignored -> false;
      case AutocompleteOperator ignored -> false;
      case ExistsOperator ignored -> false;
      case GeoShapeOperator ignored -> false;
      case GeoWithinOperator ignored -> false;
      case KnnBetaOperator ignored -> false;
      case VectorSearchOperator ignored -> false;
      case MoreLikeThisOperator ignored -> false;
      case NearOperator ignored -> false;
      case PhraseOperator ignored -> false;
      case QueryStringOperator ignored -> false;
      case SearchOperator ignored -> false;
      case SpanOperator ignored -> false;
      case TermOperator ignored -> false;
      case TermLevelOperator ignored -> false;
      case TextOperator ignored -> false;
      case HasAncestorOperator ignored -> false;
      case HasRootOperator ignored -> false;
    };
  }

  static Optional<DrillSidewaysInfo> buildDrillSidewaysInfo(
      Operator operator, Map<String, FacetDefinition> facetDefinitions) {
    // if operator doesn't contain doesNotAffect, create and run a non-drill sideways query
    if (!containsDoesNotAffect(operator)) {
      return Optional.empty();
    }

    DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    return info.optimizationStatus() == DrillSidewaysInfo.QueryOptimizationStatus.NON_DRILL_SIDEWAYS
        ? Optional.empty()
        : Optional.of(info);
  }

  private static FacetCollector create(
      Operator operator, Map<String, FacetDefinition> facetDefinitions) {

    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        buildDrillSidewaysInfo(operator, facetDefinitions);

    return new FacetCollector(operator, facetDefinitions, drillSidewaysInfo);
  }

  /** Deserializes collector from a BSON. */
  public static FacetCollector fromBson(DocumentParser parser) throws BsonParseException {
    return FacetCollector.create(
        parser.getField(Fields.OPERATOR).unwrap(), parser.getField(Fields.FACETS).unwrap());
  }

  @Override
  public BsonValue collectorToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OPERATOR, this.operator)
        .field(Fields.FACETS_10K, this.facetDefinitions)
        .build();
  }

  /**
   * Deserializes collector from BSON with 10k limits when {@link
   * com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags#ENABLE_10K_BUCKET_LIMIT} is on (string
   * {@code numBuckets} and number/date {@code boundaries}).
   */
  public static FacetCollector fromBson10kAllowed(DocumentParser parser) throws BsonParseException {
    return FacetCollector.create(
        parser.getField(Fields.OPERATOR).unwrap(), parser.getField(Fields.FACETS_10K).unwrap());
  }

  /** Encodes this collector to BSON (same as collectorToBson; used for FACET_10K_ALLOWED field). */
  public BsonValue collectorToBson10kAllowed() {
    return collectorToBson();
  }

  /**
   * Returns total requested facet buckets across all facets in this collector (for {@code
   * totalFacetBucketsPerQuery} metrics): string {@code numBuckets} plus {@code boundaries.size() -
   * 1} for each number/date facet.
   *
   * <p>Emitting the metric is gated by {@link
   * com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags#ENABLE_TOTAL_FACET_BUCKETS} (wired
   * through {@link com.xgen.mongot.index.MeteredSearchIndexReader} into {@link
   * com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater
   * #recordTotalFacetBucketsIfApplicable}, which records {@code totalFacetBucketsPerQuery} when
   * this sum is positive).
   */
  public int getTotalRequestedFacetBuckets() {
    @Var int total = 0;
    for (FacetDefinition def : this.facetDefinitions.values()) {
      total += requestedBucketCount(def);
    }
    return total;
  }

  private static int requestedBucketCount(FacetDefinition def) {
    return switch (def) {
      case FacetDefinition.StringFacetDefinition stringFacet -> stringFacet.numBuckets();
      case FacetDefinition.NumericFacetDefinition numericFacet ->
          numericFacet.boundaries().size() - 1;
      case FacetDefinition.DateFacetDefinition dateFacet -> dateFacet.boundaries().size() - 1;
    };
  }

  /** Returns facet definitions of the given type. */
  public Map<FacetDefinition.Type, Map<String, FacetDefinition>> getFacetDefinitionsByType() {
    Map<FacetDefinition.Type, Map<String, FacetDefinition>> result = new HashMap<>();
    Arrays.stream(FacetDefinition.Type.values()).forEach(type -> result.put(type, new HashMap<>()));
    this.facetDefinitions.forEach(
        (key, value) -> {
          FacetDefinition.Type type = value.getType();
          result.get(type).put(key, value);
        });
    return result;
  }

  @Override
  public Type getType() {
    return Type.FACET;
  }

  @Override
  public Optional<Operator> getOperator() {
    return Optional.of(this.operator);
  }
}
