package com.xgen.mongot.index.lucene.sort;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.SortField;

/**
 * Factory for creating Lucene {@link org.apache.lucene.search.Sort} objects for index sorting.
 * Assumes sort fields have been pre-validated in {@link
 * com.xgen.mongot.index.definition.IndexSortValidator}
 */
public class LuceneIndexSortFactory {
  private final SearchFieldDefinitionResolver fieldDefinitionResolver;

  public LuceneIndexSortFactory(SearchFieldDefinitionResolver fieldDefinitionResolver) {
    this.fieldDefinitionResolver = fieldDefinitionResolver;
  }

  /**
   * Create a Lucene Sort from a validated Sort spec.
   *
   * @param sort the sprt specification
   * @return Lucene Sort object
   */
  public org.apache.lucene.search.Sort createIndexSort(Sort sort) {
    List<SortField> sortFields = new ArrayList<>();

    for (MongotSortField mongotSortField : sort.getSortFields()) {
      sortFields.addAll(createSortFieldsFromMongotField(mongotSortField));
    }

    return new org.apache.lucene.search.Sort(sortFields.toArray(new SortField[0]));
  }

  private List<SortField> createSortFieldsFromMongotField(MongotSortField mongotSortField) {
    FieldDefinition fieldDefinition =
        Check.isPresent(
            this.fieldDefinitionResolver.getFieldDefinition(
                mongotSortField.field(), Optional.empty()),
            "fieldDefinition");

    ImmutableSet<FieldName.TypeField> typeFields =
        determineTypeFields(fieldDefinition, mongotSortField);

    // Single-type fields use Lucene's native optimized SortField (e.g., SortedNumericSortField),
    // which supports efficient comparators and codec-level optimizations. Multi-type fields fall
    // back to MqlMixedSort, which implements cross-type BSON comparison via a custom IndexSorter.
    if (typeFields.size() == 1) {
      Optional<SortField> optimized =
          LuceneSortFactory.createOptimizedSortField(
              mongotSortField,
              typeFields,
              Optional.empty(),
              Optional.empty(),
              this.fieldDefinitionResolver.getIndexCapabilities(),
              true);

      if (optimized.isEmpty()) {
        throw new IllegalStateException("Failed to create sort field " + mongotSortField.field());
      }

      SortField sortField = optimized.get();
      FieldName.TypeField typeField = typeFields.iterator().next();
      boolean needsNullness =
          typeField == FieldName.TypeField.NUMBER_INT64_V2
              || typeField == FieldName.TypeField.DATE_V2;

      List<SortField> result = new ArrayList<>();
      if (needsNullness) {
        FieldPath nullnessPath =
            FieldPath.newRoot(FieldName.getNullnessFieldName(mongotSortField.field()));
        MongotSortField nullnessSortField =
            new MongotSortField(nullnessPath, mongotSortField.options());
        result.add(LuceneSortFactory.createNullnessSortField(nullnessSortField));
      }
      result.add(sortField);
      return result;
    }

    // MqlMixedSort handles null/missing internally via its IndexSorter, so no nullness prefix.
    return List.of(new MqlMixedSort(mongotSortField, Optional.empty()));
  }

  /** Collects all sortable Lucene TypeFields for a given field definition. */
  private ImmutableSet<FieldName.TypeField> determineTypeFields(
      FieldDefinition fieldDefinition, MongotSortField mongotSortField) {
    ImmutableSet.Builder<FieldName.TypeField> typeFields = ImmutableSet.builder();

    if (fieldDefinition.booleanFieldDefinition().isPresent()
        && this.fieldDefinitionResolver
            .getIndexCapabilities()
            .supportsObjectIdAndBooleanDocValues()) {
      typeFields.add(FieldName.TypeField.BOOLEAN);
    }

    if (fieldDefinition.dateFieldDefinition().isPresent()) {
      typeFields.add(FieldName.TypeField.DATE_V2);
    }

    if (fieldDefinition.tokenFieldDefinition().isPresent()) {
      typeFields.add(FieldName.TypeField.TOKEN);
    }

    if (fieldDefinition.uuidFieldDefinition().isPresent()) {
      typeFields.add(FieldName.TypeField.UUID);
    }

    if (fieldDefinition.objectIdFieldDefinition().isPresent()) {
      typeFields.add(FieldName.TypeField.OBJECT_ID);
    }

    if (fieldDefinition.numberFieldDefinition().isPresent()) {
      typeFields.add(determineNumberTypeField(fieldDefinition.numberFieldDefinition().get()));
    }

    ImmutableSet<FieldName.TypeField> result = typeFields.build();
    if (result.isEmpty()) {
      throw new IllegalArgumentException(
          "No supported sortable type found for field: " + mongotSortField.field());
    }
    return result;
  }

  private FieldName.TypeField determineNumberTypeField(
      NumericFieldDefinition numberFieldDefinition) {
    FieldName.TypeField typeField =
        switch (numberFieldDefinition.options().representation()) {
          case INT64 -> FieldName.TypeField.NUMBER_INT64_V2;
          case DOUBLE -> FieldName.TypeField.NUMBER_DOUBLE_V2;
        };
    return typeField;
  }
}
