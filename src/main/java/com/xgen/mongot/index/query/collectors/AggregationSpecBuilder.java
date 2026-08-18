package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
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
   * Builds a top-level spec tree from the collector definition, lowering each top-level facet
   * branch recursively.
   *
   * @param boundaryResolver index field metadata for number/date boundary group specs. {@link
   *     Optional#empty()} at shard plan time; the Search registry then builds boundary specs from
   *     the query alone. Required at execution time so edges can be encoded into doc-values space.
   */
  public static TopAggregationSpec toTopLevelSpec(
      AggregationCollector collector,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    List<AggregationSpec> children = new ArrayList<>(collector.facets().size());
    for (Map.Entry<String, AggregationFacetDefinition> entry : collector.facets().entrySet()) {
      children.add(
          lower(entry.getKey(), entry.getValue(), /* isRoot= */ true, registry, boundaryResolver));
    }
    return new TopAggregationSpec(children);
  }

  /**
   * Lower one recursive facet node into its group spec. A group node's children are its nested
   * facet subtrees XOR its metric leaves, never both (enforced at parse time): an intermediate node
   * recurses into its nested facets, while a leaf builds one operator spec per {@code aggregations}
   * entry (a legacy-shaped leaf's implicit {@code count} is materialized at construction time by
   * {@link AggregationFacetDefinition}).
   */
  private static AggregationSpec lower(
      String name,
      AggregationFacetDefinition def,
      boolean isRoot,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    List<AggregationSpec> children = new ArrayList<>();
    if (!def.isLeaf()) {
      def.facets()
          .forEach(
              (child, childDef) ->
                  children.add(
                      lower(child, childDef, /* isRoot= */ false, registry, boundaryResolver)));
    } else {
      def.aggregations()
          .forEach(
              (out, metric) ->
                  children.add(operatorSpec(out, metric.operator(), metric.path(), registry)));
    }
    return groupSpec(name, def.group(), def.path(), isRoot, children, registry, boundaryResolver);
  }

  private static AggregationSpec groupSpec(
      String name,
      FacetDefinition facetDefinition,
      FieldPath path,
      boolean isRoot,
      List<AggregationSpec> childrenSpecs,
      AggregationRegistry registry,
      Optional<BoundaryFieldResolver> boundaryResolver) {
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
      case FacetDefinition.DateFacetDefinition dateFacetDefinition ->
          registry
              .getGroupSpecSupplier(SearchAggregationBootstrap.DATE_BOUNDARY)
              .build(
                  name,
                  path,
                  childrenSpecs,
                  toBoundaryArgs(dateFacetDefinition, path, boundaryResolver),
                  registry);
      case FacetDefinition.NumericFacetDefinition numericFacetDefinition ->
          registry
              .getGroupSpecSupplier(SearchAggregationBootstrap.NUMBER_BOUNDARY)
              .build(
                  name,
                  path,
                  childrenSpecs,
                  toBoundaryArgs(numericFacetDefinition, path, boundaryResolver),
                  registry);
    };
  }

  /**
   * When {@code boundaryResolver} is empty, {@code typeField} and {@code fieldDefinition} are empty
   * so {@link SearchAggregationBootstrap} can build a merge-planning spec from the query boundaries
   * alone.
   */
  private static AggregationRegistry.GroupingArgs.BoundaryArgs toBoundaryArgs(
      BoundaryFacetDefinition<?> facetDefinition,
      FieldPath path,
      Optional<BoundaryFieldResolver> boundaryResolver) {
    return new AggregationRegistry.GroupingArgs.BoundaryArgs(
        facetDefinition.boundaries(),
        facetDefinition.defaultBucketName().map(BsonString::new),
        boundaryResolver.map(resolver -> resolver.getBoundaryFacetFieldType(facetDefinition)),
        boundaryResolver.map(resolver -> resolver.getFieldDefinition(path)));
  }

  private static AggregationSpec operatorSpec(
      String name, Operator operator, Optional<FieldPath> path, AggregationRegistry registry) {
    return registry.getOperatorSupplier(operator.registryKey()).build(name, path);
  }
}
