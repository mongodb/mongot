package com.xgen.mongot.index.lucene.query.sort.mixed;

import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;

/**
 * This LeafComparator supports {@link SortedDocValues} for sorting heterogeneous fields. This
 * implementation uses ordinal comparisons for comparing against top/bottom. For homogenous fields,
 * MqlStringSort provides an optimized implementation that uses pruning and ordinal comparisons
 * between heap elements.
 */
class MixedOrdinalLeafComparator implements MixedLeafFieldComparator {

  private final SortedDocValues dv;
  private final int priority;
  private final Function<BytesRef, BsonValue> converter;
  private final int topOrd;
  private int bottomOrd;

  MixedOrdinalLeafComparator(
      CompositeComparator parent,
      SortedDocValues dv,
      int priority,
      Function<BytesRef, BsonValue> converter)
      throws IOException {
    this.dv = dv;
    this.priority = priority;
    this.converter = converter;
    // topOrd is never used if top is not in same bracket, so value doesn't matter
    this.topOrd =
        parent.getTopBracket() == priority ? dv.lookupTerm(extractTerm(parent.getTop())) : -1;
  }

  @Override
  public int getBracketPriority() {
    return this.priority;
  }

  @Override
  public boolean hasValue(int doc) throws IOException {
    return this.dv.advanceExact(doc);
  }

  @Override
  public int compareBottomToCurrent() throws IOException {
    // Called after hasValue returns true and bracket matches bottom
    return compareWithinBracket(this.bottomOrd);
  }

  @Override
  public BsonValue getCurrentValue() throws IOException {
    // called after hasValue returns true
    BytesRef reusableBuffer = this.dv.lookupOrd(this.dv.ordValue());
    return this.converter.apply(reusableBuffer);
  }

  private static BytesRef extractTerm(BsonValue value) {
    BsonType type = value.getBsonType();
    return switch (type) {
      case STRING -> new BytesRef(value.asString().getValue());
      case SYMBOL -> new BytesRef(value.asSymbol().getSymbol());
      case BINARY -> new BytesRef(value.asBinary().asUuid(UuidRepresentation.STANDARD).toString());
      case OBJECT_ID -> new BytesRef(value.asObjectId().getValue().toByteArray());
      default ->
          throw new IllegalStateException("Cannot perform ordinal comparison on type: " + type);
    };
  }

  @Override
  public void notifyNewBottom(BsonValue bottom) throws IOException {
    BytesRef term = extractTerm(bottom);
    this.bottomOrd = this.dv.lookupTerm(term);
  }

  /**
   * Compare `ord` to the current ord.
   *
   * @param ord - The left value to compare to. If the term ord applies to does not exactly match
   *     anything in this segment, then its value is {@code -insertionPoint - 1}
   * @return negative if `ord` is less than the current ord.
   */
  private int compareWithinBracket(int ord) throws IOException {
    if (ord >= 0) {
      return Integer.compare(ord, this.dv.ordValue());
    } else {
      int floorInsertPosition = -(ord + 2); // term(-ord - 2) < original_term < term(-ord - 1)
      int curr = this.dv.ordValue();

      if (floorInsertPosition >= curr) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  @Override
  public int compareTopToCurrent() throws IOException {
    return compareWithinBracket(this.topOrd);
  }

  @Override
  public int nextDoc() throws IOException {
    return this.dv.nextDoc();
  }
}
