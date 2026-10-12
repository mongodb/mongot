package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.DoubleFieldConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.EmbeddedScopedLongComparator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.join.BitSetProducer;

/**
 * A double sort field scoped to the matching children of an {@code embeddedDocuments} block. mongot
 * indexes doubles as order-preserving sortable longs ({@link MqlDoubleSort}), so the same
 * block-join reduction the int64 engine uses ({@link EmbeddedScopedLongComparator} over {@link
 * com.xgen.mongot.index.lucene.query.sort.comparator.BlockJoinReducedNumericDocValues}) reduces the
 * encoded values correctly -- MIN ascending / MAX descending is monotonic in the encoding. The
 * reduced encoded-long value is then decoded back to a BSON double by the {@code MqlDoubleSort}
 * wrapper. The child-selecting operator is resolved at the query layer (CLOUDP-441316); this engine
 * takes the {@link BitSetProducer}s directly.
 */
public class EmbeddedScopedDoubleSort extends MqlDoubleSort {

  private final BitSetProducer parentFilter;
  private final BitSetProducer childFilter;

  public EmbeddedScopedDoubleSort(
      MongotSortField sortField,
      FieldPath embeddedPath,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    super(
        FieldName.TypeField.NUMBER_DOUBLE_V2,
        sortField,
        /* enablePruning= */ false,
        Optional.of(embeddedPath),
        /* indexSorted= */ false,
        /* isSortableNumberBetaV1= */ false);
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
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
        this.parentFilter,
        this.childFilter,
        getSelector() == SortedNumericSelector.Type.MAX);
  }
}
