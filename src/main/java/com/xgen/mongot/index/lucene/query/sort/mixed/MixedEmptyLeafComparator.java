package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.google.errorprone.annotations.Immutable;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import org.apache.lucene.search.DocIdSetIterator;
import org.bson.BsonValue;

/**
 * This comparator compares all documents as equal, which is not the same as DOC_ID sort in the case
 * of a compound sort spec. This comparator is used to sort on a field which does not exist within a
 * segment.
 */
@Immutable
class MixedEmptyLeafComparator implements MixedLeafFieldComparator {

  private final NullEmptySortPosition nullPosition;

  MixedEmptyLeafComparator(NullEmptySortPosition nullPosition) {
    this.nullPosition = nullPosition;
  }

  @Override
  public int getBracketPriority() {
    return this.nullPosition.nullMissingPriority;
  }

  @Override
  public boolean hasValue(int docId) {
    return false;
  }

  @Override
  public int compareBottomToCurrent() {
    return 0;
  }

  @Override
  public int compareTopToCurrent() {
    return 0;
  }

  @Override
  public BsonValue getCurrentValue() {
    // This method is never called because hasValue() above returns false
    return this.nullPosition.getNullMissingSortValue();
  }

  @Override
  public void notifyNewBottom(BsonValue bottom) {}

  @Override
  public int nextDoc() {
    return DocIdSetIterator.NO_MORE_DOCS;
  }
}
