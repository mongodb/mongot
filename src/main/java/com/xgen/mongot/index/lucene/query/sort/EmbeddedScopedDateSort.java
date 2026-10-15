package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.EmbeddedScopedLongComparator;
import com.xgen.mongot.index.lucene.query.sort.comparator.MqlLongComparator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.join.BitSetProducer;

/**
 * A date sort field scoped to the matching children of an {@code embeddedDocuments} block. Dates
 * are indexed as int64 millis ({@link MqlDateSort}), so this reuses the int64 block-join reduction
 * ({@link EmbeddedScopedLongComparator} over {@link
 * com.xgen.mongot.index.lucene.query.sort.comparator.BlockJoinReducedNumericDocValues}) -- MIN
 * ascending / MAX descending over a root's matching children -- with the {@code MqlDateSort}
 * wrapper decoding the reduced value back to a BSON date. The child-selecting operator is resolved
 * at the query layer (CLOUDP-441316); this engine takes the {@link BitSetProducer}s directly.
 */
public class EmbeddedScopedDateSort extends MqlDateSort {

  private final BitSetProducer parentFilter;
  private final BitSetProducer childFilter;

  public EmbeddedScopedDateSort(
      MongotSortField sortField,
      FieldPath embeddedPath,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    super(
        FieldName.TypeField.DATE_V2,
        sortField,
        /* enablePruning= */ false,
        Optional.of(embeddedPath),
        /* indexSorted= */ false,
        /* isIndexWriterConfigSortField= */ false);
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
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
        this.parentFilter,
        this.childFilter,
        getSelector() == SortedNumericSelector.Type.MAX);
  }
}
