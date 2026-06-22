package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.bson.BsonDouble;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * A LeafComparator that reads doubles from DocValues but is able to compare them to other types
 * within the same sort bracket.
 */
class MixedDoubleLeafComparator extends MixedNumericLeafComparator {
  private static final int DOUBLE_BRACKET_PRIORITY =
      SortUtil.getBracketPriority(BsonType.DOUBLE, NullEmptySortPosition.LOWEST);

  /**
   * Create a leaf comparator for a column of toMqlSortableLong-encoded docValues.
   *
   * @param dv - Expected to be instantiated as Type.LONG rather than rely on Lucene's default
   *     double decoding scheme.
   */
  MixedDoubleLeafComparator(CompositeComparator parent, NumericDocValues dv) {
    super(parent, dv, DOUBLE_BRACKET_PRIORITY);
  }

  @Override
  public BsonValue getCurrentValue() throws IOException {
    return new BsonDouble(LuceneDoubleConversionUtils.fromMqlSortableLong(this.dv.longValue()));
  }
}
