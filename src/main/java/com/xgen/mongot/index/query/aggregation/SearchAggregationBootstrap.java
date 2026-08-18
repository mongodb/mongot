package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupSpecSupplier;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonValue;
import org.jetbrains.annotations.TestOnly;

/** All AggregationSpec registration should be done in this class at startup. */
public class SearchAggregationBootstrap {
  // Internal operator and grouping keys
  public static final String TERMS = "terms";
  public static final String NUMBER_BOUNDARY = "numberBoundary";
  public static final String DATE_BOUNDARY = "dateBoundary";
  public static final String COUNT = Operator.COUNT.registryKey();
  public static final String SUM = Operator.SUM.registryKey();
  public static final String AVG = Operator.AVG.registryKey();
  public static final String MIN = Operator.MIN.registryKey();
  public static final String MAX = Operator.MAX.registryKey();
  public static final String FIRST = Operator.FIRST.registryKey();
  public static final String LAST = Operator.LAST.registryKey();

  private SearchAggregationBootstrap() {}

  /**
   * Installs production group and operator suppliers at mongot startup when {@link
   * Feature#SEARCH_AGGREGATION} is enabled.
   */
  public static void installRegistryIfEnabled(FeatureFlags featureFlags) {
    if (featureFlags.isEnabled(Feature.SEARCH_AGGREGATION)) {
      installRegistry();
    }
  }

  /** Installs production group and operator suppliers unconditionally. */
  public static void installRegistry() {
    AggregationRegistry.install(groupSuppliers(), operatorSuppliers());
  }

  @TestOnly
  static void installRegistry(Map<String, AggregationRegistry.OperatorSupplier> operators) {
    AggregationRegistry.install(groupSuppliers(), operators);
  }

  private static Map<String, AggregationRegistry.OperatorSupplier> operatorSuppliers() {
    return Map.of(
        COUNT,
        (name, path) -> {
          requireNoPath(path, COUNT);
          return new CountAggregationSpec(name);
        },
        SUM,
        (name, path) -> new SumAggregationSpec(name, requirePath(path, SUM)),
        AVG,
        (name, path) -> new AvgAggregationSpec(name, requirePath(path, AVG)),
        MIN,
        (name, path) -> new MinAggregationSpec(name, requirePath(path, MIN)),
        MAX,
        (name, path) -> new MaxAggregationSpec(name, requirePath(path, MAX)),
        FIRST,
        (name, path) -> new FirstAggregationSpec(name, requirePath(path, FIRST)),
        LAST,
        (name, path) -> new LastAggregationSpec(name, requirePath(path, LAST)));
  }

  /**
   * Counterpart to {@link #requirePath}: count is the only operator that takes no path, and this is
   * the second entry point where that can be enforced. {@code MetricSpec#fromBson} rejects a path
   * on count while parsing user BSON, but {@code MetricSpec} is a record with no validation in its
   * canonical constructor, so a programmatically built spec reaches the registry unchecked.
   */
  private static void requireNoPath(Optional<FieldPath> path, String operator) {
    if (path.isPresent()) {
      throw new IllegalArgumentException(operator + " operator does not take a field path");
    }
  }

  private static FieldPath requirePath(Optional<FieldPath> path, String operator) {
    return path.orElseThrow(
        () -> new IllegalArgumentException(operator + " operator requires a field path"));
  }

  private static Map<String, GroupSpecSupplier> groupSuppliers() {
    return Map.of(
        TERMS,
        (name, path, children, args, registry) -> {
          GroupingArgs.TermsArgs termsArgs = Check.instanceOf(args, GroupingArgs.TermsArgs.class);
          return new TermsGroupAggregationSpec(name, path, children, termsArgs);
        },
        NUMBER_BOUNDARY,
        (name, path, children, args, registry) ->
            SearchAggregationBootstrap.boundary(name, path, children, args, NUMBER_BOUNDARY),
        DATE_BOUNDARY,
        (name, path, children, args, registry) ->
            SearchAggregationBootstrap.boundary(name, path, children, args, DATE_BOUNDARY));
  }

  private static GroupAggregationSpec boundary(
      String name,
      FieldPath path,
      List<AggregationSpec> children,
      GroupingArgs args,
      String boundaryType) {
    GroupingArgs.BoundaryArgs boundaryArgs =
        Check.instanceOf(args, GroupingArgs.BoundaryArgs.class);
    if (boundaryArgs.fieldDefinition().isEmpty()) {
      return BoundaryGroupAggregationSpec.createBoundarySpecWithoutResolving(
          name, path, children, boundaryArgs);
    }
    TypeField typeField =
        boundaryArgs
            .typeField()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Boundary aggregations require a type field when a field definition is"
                            + " present"));
    FieldDefinition fieldDef =
        boundaryArgs.fieldDefinition().orElseThrow(IllegalStateException::new);
    // We only expect V2 number and date field indexing definition.
    FieldTypeDefinition fieldTypeDefinition =
        switch (boundaryType) {
          case NUMBER_BOUNDARY -> fieldDef.numberFieldDefinition().orElseThrow();
          case DATE_BOUNDARY -> fieldDef.dateFieldDefinition().orElseThrow();
          default ->
              throw new IllegalArgumentException("Unsupported boundary type: " + boundaryType);
        };

    List<? extends BsonValue> boundaries = boundaryArgs.boundaries();
    int n = boundaries.size();
    long[] edges = new long[n];
    // Since boundaries is right exclusive, so minus one for number of ranges.
    List<BsonValue> rangeKeys = new ArrayList<>(n - 1);
    for (int i = 0; i < n; i++) {
      edges[i] = BoundaryEncoder.encode(boundaries.get(i), fieldTypeDefinition, typeField);
      if (i < n - 1) {
        rangeKeys.add(boundaries.get(i));
      }
    }
    boolean lastEdgeInclusive =
        BoundaryEncoder.lastEdgeInclusive(boundaries.get(n - 1), fieldTypeDefinition);
    return new BoundaryGroupAggregationSpec(
        name, path, children, edges, rangeKeys, lastEdgeInclusive, boundaryArgs);
  }
}
