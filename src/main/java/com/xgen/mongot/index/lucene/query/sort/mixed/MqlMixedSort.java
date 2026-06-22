package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * A custom sort that mimics MQL sort semantics on heterogeneous fields. See <a
 * href="https://www.mongodb.com/docs/v5.3/reference/bson-type-comparison-order/">BSON Sort
 * Order</a>
 */
public class MqlMixedSort extends SortField {
  private final SortOptions options;
  private final FieldPath field;
  Optional<FieldPath> embeddedRoot;

  public MqlMixedSort(MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    super(sortField.field().toString(), Type.CUSTOM, sortField.options().order().isReverse());
    this.options = sortField.options();
    this.field = sortField.field();
    this.embeddedRoot = embeddedRoot;
  }

  @Override
  public FieldComparator<BsonValue> getComparator(int numHits, Pruning unused) {
    return CompositeComparator.create(
        createMixedFieldComparators(), (UserFieldSortOptions) this.options, numHits);
  }

  @Override
  public IndexSorter getIndexSorter() {
    return new MqlMixedIndexSorter(this, (UserFieldSortOptions) this.options);
  }

  MixedFieldComparator[] createMixedFieldComparators() {
    return new MixedFieldComparator[] {
      new MixedFieldComparator(
          TypeField.NUMBER_DOUBLE_V2, BsonType.DOUBLE, this.field, this.embeddedRoot),
      new MixedFieldComparator(
          TypeField.NUMBER_INT64_V2, BsonType.INT64, this.field, this.embeddedRoot),
      new MixedFieldComparator(
          TypeField.DATE_V2, BsonType.DATE_TIME, this.field, this.embeddedRoot),
      new MixedFieldComparator(TypeField.TOKEN, BsonType.STRING, this.field, this.embeddedRoot),
      new MixedFieldComparator(TypeField.BOOLEAN, BsonType.BOOLEAN, this.field, this.embeddedRoot),
      new MixedFieldComparator(TypeField.UUID, BsonType.BINARY, this.field, this.embeddedRoot),
      new MixedFieldComparator(TypeField.NULL, BsonType.NULL, this.field, this.embeddedRoot),
      new MixedFieldComparator(
          TypeField.OBJECT_ID, BsonType.OBJECT_ID, this.field, this.embeddedRoot),
    };
  }
}
