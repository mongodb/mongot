package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

/**
 * An {@link MqlLongComparator} whose per-doc value is reduced over only the matching children of an
 * {@code embeddedDocuments} block (embeddedDoc-scoped sort). It reuses all of {@code
 * MqlLongComparator}'s MQL-compliant ordering and missing-value handling; the only change is that
 * its leaf comparator reads block-join-reduced values (via {@link
 * BlockJoinReducedNumericDocValues}) instead of the document's own value.
 */
public class EmbeddedScopedLongComparator extends MqlLongComparator {

  private final BitSetProducer parentFilter;
  private final BitSetProducer childFilter;
  private final boolean reduceMax;

  public EmbeddedScopedLongComparator(
      int numHits,
      String field,
      boolean reverse,
      Pruning pruning,
      SortField.Type sortType,
      SortedNumericSelector.Type selector,
      NullEmptySortPosition nullEmptySortPosition,
      BitSetProducer parentFilter,
      BitSetProducer childFilter,
      boolean reduceMax) {
    super(numHits, field, reverse, pruning, sortType, selector, nullEmptySortPosition);
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
    this.reduceMax = reduceMax;
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new ScopedLongLeafComparator(context);
  }

  /** Reads the parent's reduced child value rather than the parent's own document value. */
  public class ScopedLongLeafComparator extends LongLeafComparator {

    public ScopedLongLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    @Override
    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
        throws IOException {
      // The superclass reduces a child's own multi-values via the sort selector; wrap that to
      // reduce again across the parent's matching children.
      NumericDocValues perChildValues = super.getNumericDocValues(context, field);
      BitSet parents = EmbeddedScopedLongComparator.this.parentFilter.getBitSet(context);
      BitSet children = EmbeddedScopedLongComparator.this.childFilter.getBitSet(context);
      return new BlockJoinReducedNumericDocValues(
          perChildValues, parents, children, EmbeddedScopedLongComparator.this.reduceMax);
    }
  }
}
