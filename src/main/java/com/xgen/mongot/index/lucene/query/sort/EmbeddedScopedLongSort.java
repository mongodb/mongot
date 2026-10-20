package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.EmbeddedScopedLongComparator;
import com.xgen.mongot.index.lucene.query.sort.comparator.MqlLongComparator;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedNumericSelector;

/**
 * An int64 sort field scoped to the matching children of an {@code embeddedDocuments} block. It
 * builds on {@link MqlLongSort} but sorts each root by a value reduced (MIN ascending / MAX
 * descending) over only the children selected by {@code scope}, via {@link
 * EmbeddedScopedLongComparator}. Index-sort early termination and pruning are disabled (the sort
 * value is derived from children, not the root's own stored value).
 *
 * <p>The child-selecting operator that produces the scope's child filter is resolved at the query
 * layer (see CLOUDP-441316); this engine takes the {@link ScopeFilters} directly.
 */
public class EmbeddedScopedLongSort extends MqlLongSort {

  private final ScopeFilters scope;

  public EmbeddedScopedLongSort(
      MongotSortField sortField, FieldPath embeddedPath, ScopeFilters scope) {
    super(
        FieldName.TypeField.NUMBER_INT64_V2,
        sortField,
        /* enablePruning= */ false,
        Optional.of(embeddedPath),
        /* indexSorted= */ false,
        /* isSortableNumberBetaV1= */ false);
    this.scope = scope;
  }

  @Override
  protected MqlLongComparator createComparator(int numHits, Pruning pruning) {
    return new EmbeddedScopedLongComparator(
        numHits,
        getField(),
        getReverse(),
        pruning,
        getNumericType(),
        getSelector(),
        this.nullEmptySortPosition,
        this.scope,
        getSelector() == SortedNumericSelector.Type.MAX);
  }
}
