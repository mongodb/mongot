package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

public class DoubleFieldConverter extends BsonConverter<Long> {

  public DoubleFieldConverter(@Nullable Long missingValue) {
    super(missingValue);
  }

  @Override
  protected Long decodeNonMissingValue(BsonValue value) {
    double d = value.asNumber().doubleValue();
    return LuceneDoubleConversionUtils.toMqlSortableLong(d);
  }

  @Override
  protected BsonValue encodeNonMissingValue(Long value) {
    double d = LuceneDoubleConversionUtils.fromMqlSortableLong(value);
    return new BsonDouble(d);
  }
}
