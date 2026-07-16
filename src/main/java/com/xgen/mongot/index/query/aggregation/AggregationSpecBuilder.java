package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.collectors.AggDefinition;
import com.xgen.mongot.index.query.collectors.AggDefinition.LeafOperator;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonString;

/**
 * Builds {@link AggregationSpec} trees from {@link AggregationCollector} definitions.
 *
 * <p>Shared by shard planning ({@code planShardedSearch}) and Lucene execution ({@link
 * com.xgen.mongot.index.lucene.LuceneAggregationSearchManager}).
 */
public final class AggregationSpecBuilder {

  /**
   * Resolves index field metadata required to build boundary group specs. Present at execution time
   * via {@link com.xgen.mongot.index.lucene.LuceneFacetContext}; absent at plan time for boundary
   * aggregations.
   */
  public interface BoundaryFieldResolver {
    FieldDefinition getFieldDefinition(FieldPath path);

    TypeField getBoundaryFacetFieldType(BoundaryFacetDefinition<?> facetDefinition);
  }

  private AggregationSpecBuilder() {}

  /**
   * Builds a top-level spec tree from the collector definition.
   *
   * @param boundaryResolver index field metadata for number/date boundary group specs. {@link
   *     Optional#empty()} at shard plan time ({@code planShardedSearch}) because the planner does
   *     not yet have index field definitions; terms (string) grouping works without it. Required at
   *     Lucene execution time when the query includes boundary grouping. Boundary shard planning is
   *     tracked in TODO(CLOUDP-424290).
   */
  public static TopAggregationSpec toTopLevelSpec(
      AggregationCollector collector,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    List<AggregationSpec> children = new ArrayList<>(collector.aggDefinitions().size());
    for (Map.Entry<String, AggDefinition> entry : collector.aggDefinitions().entrySet()) {
      children.add(lower(entry.getKey(), entry.getValue(), registry, boundaryResolver));
    }
    return new TopAggregationSpec(children);
  }

  private static AggregationSpec lower(
      String name,
      AggDefinition def,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    @Var AggregationSpec spec = operatorSpec(name, def.leafOperator(), registry);
    List<FacetDefinition> aggs = def.aggs();
    for (int i = aggs.size() - 1; i >= 0; i--) {
      boolean isRoot = i == 0;
      spec = groupSpec(name, aggs.get(i), isRoot, List.of(spec), registry, boundaryResolver);
    }
    return spec;
  }

  private static AggregationSpec groupSpec(
      String name,
      FacetDefinition facetDefinition,
      boolean isRoot,
      List<AggregationSpec> childrenSpecs,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    FieldPath path = FieldPath.parse(facetDefinition.path());
    return switch (facetDefinition) {
      case FacetDefinition.StringFacetDefinition stringFacetDefinition ->
          registry
              .getGroupSpecSupplier(SearchAggregationBootstrap.TERMS)
              .build(
                  name,
                  path,
                  childrenSpecs,
                  new AggregationRegistry.GroupingArgs.TermsArgs(
                      isRoot, TypeField.TOKEN, stringFacetDefinition.numBuckets()),
                  registry);
      case FacetDefinition.DateFacetDefinition dateFacetDefinition -> {
        BoundaryFieldResolver resolver = requireBoundaryResolver(boundaryResolver, path);
        FieldDefinition fieldDef = resolver.getFieldDefinition(path);
        yield registry
            .getGroupSpecSupplier(SearchAggregationBootstrap.DATE_BOUNDARY)
            .build(
                name,
                path,
                childrenSpecs,
                new AggregationRegistry.GroupingArgs.BoundaryArgs(
                    dateFacetDefinition.boundaries(),
                    dateFacetDefinition.defaultBucketName().map(BsonString::new),
                    resolver.getBoundaryFacetFieldType(dateFacetDefinition),
                    fieldDef),
                registry);
      }
      case FacetDefinition.NumericFacetDefinition numericFacetDefinition -> {
        BoundaryFieldResolver resolver = requireBoundaryResolver(boundaryResolver, path);
        FieldDefinition fieldDef = resolver.getFieldDefinition(path);
        yield registry
            .getGroupSpecSupplier(SearchAggregationBootstrap.NUMBER_BOUNDARY)
            .build(
                name,
                path,
                childrenSpecs,
                new AggregationRegistry.GroupingArgs.BoundaryArgs(
                    numericFacetDefinition.boundaries(),
                    numericFacetDefinition.defaultBucketName().map(BsonString::new),
                    resolver.getBoundaryFacetFieldType(numericFacetDefinition),
                    fieldDef),
                registry);
      }
    };
  }

  private static BoundaryFieldResolver requireBoundaryResolver(
      Optional<BoundaryFieldResolver> boundaryResolver, FieldPath path) {
    return boundaryResolver.orElseThrow(
        () ->
            new IllegalStateException(
                "Boundary aggregations require index field definitions for path: " + path));
  }

  private static AggregationSpec operatorSpec(
      String name, LeafOperator leafOperator, AggregationRegistry registry) {
    return registry.getOperatorSupplier(leafOperator.name()).build(name, leafOperator.path());
  }
}
