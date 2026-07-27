package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

/** Resolves boundary facet fields through {@link LuceneAggregationContextFieldResolver}. */
final class FacetContextBoundaryResolver implements AggregationSpecBuilder.BoundaryFieldResolver {
  private final LuceneAggregationContextFieldResolver fieldResolver;

  FacetContextBoundaryResolver(LuceneAggregationContextFieldResolver fieldResolver) {
    this.fieldResolver = fieldResolver;
  }

  @Override
  public FieldDefinition getFieldDefinition(FieldPath path) {
    try {
      return this.fieldResolver.getFieldDefinition(path, Optional.empty());
    } catch (InvalidQueryException e) {
      throw new IllegalStateException("Unresolvable field definition for path: " + path, e);
    }
  }

  @Override
  public FieldName.TypeField getBoundaryFacetFieldType(BoundaryFacetDefinition<?> facetDefinition) {
    try {
      return this.fieldResolver.getBoundaryFacetFieldType(facetDefinition, Optional.empty());
    } catch (InvalidQueryException e) {
      throw new IllegalStateException("Failed to get field type for " + facetDefinition.path(), e);
    }
  }
}
