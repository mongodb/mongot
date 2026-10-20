package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.DoubleFieldConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.EmbeddedScopedLongComparator;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedNumericSelector;

/**
 * A double sort field scoped to the matching children of an {@code embeddedDocuments} block. mongot
 * indexes doubles as order-preserving sortable longs ({@link MqlDoubleSort}), so the same
 * block-join reduction the int64 engine uses ({@link EmbeddedScopedLongComparator} over {@link
 * com.xgen.mongot.index.lucene.query.sort.comparator.BlockJoinReducedNumericDocValues}) reduces the
 * encoded values correctly -- MIN ascending / MAX descending is monotonic in the encoding. The
 * reduced encoded-long value is then decoded back to a BSON double by the {@code MqlDoubleSort}
 * wrapper. The child-selecting operator is resolved at the query layer (CLOUDP-441316); this engine
 * takes the {@link ScopeFilters} directly.
 */
public class EmbeddedScopedDoubleSort extends MqlDoubleSort {

  private final ScopeFilters scope;

  public EmbeddedScopedDoubleSort(
      MongotSortField sortField, FieldPath embeddedPath, ScopeFilters scope) {
    super(
        FieldName.TypeField.NUMBER_DOUBLE_V2,
        sortField,
        /* enablePruning= */ false,
        Optional.of(embeddedPath),
        /* indexSorted= */ false,
        /* isSortableNumberBetaV1= */ false);
    this.scope = scope;
  }

  @Override
  protected DoubleFieldConverter createConverter() {
    // EmbeddedScopedLongComparator represents missing values as null, not a long sentinel.
    return new DoubleFieldConverter(null);
  }

  @Override
  protected FieldComparator<Long> createInnerComparator(int numHits, Pruning pruning) {
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
