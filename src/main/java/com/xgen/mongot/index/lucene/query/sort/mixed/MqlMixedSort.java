package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * A custom sort that mimics MQL sort semantics on heterogeneous fields. See <a
 * href="https://www.mongodb.com/docs/v5.3/reference/bson-type-comparison-order/">BSON Sort
 * Order</a>
 */
public class MqlMixedSort extends SortField {
  private final SortOptions options;
  private final FieldPath field;
  private final Optional<FieldPath> embeddedRoot;
  private final Optional<ScopeFilters> scope;

  public MqlMixedSort(MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    this(sortField, embeddedRoot, Optional.empty());
  }

  private MqlMixedSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot, Optional<ScopeFilters> scope) {
    super(
        sortField.field().toString(),
        scope.isPresent()
            ? new ScopedComparatorSource(
                new Scope(sortField, embeddedRoot.orElseThrow(), scope.orElseThrow()))
            : null,
        sortField.options().order().isReverse());
    this.options = sortField.options();
    this.field = sortField.field();
    this.embeddedRoot = embeddedRoot;
    this.scope = scope;
  }

  /**
   * A multi-type sort scoped to the matching children of an {@code embeddedDocuments} block. Each
   * per-type comparator reads a block-join-reduced (MIN ascending / MAX descending) view of its
   * doc-values over the parent's matching children, and the composite comparator resolves
   * cross-type ordering by bracket over those reduced values. See CLOUDP-441316.
   */
  public static MqlMixedSort scoped(
      MongotSortField sortField, FieldPath embeddedPath, ScopeFilters scope) {
    return new MqlMixedSort(sortField, Optional.of(embeddedPath), Optional.of(scope));
  }

  @Override
  public FieldComparator<BsonValue> getComparator(int numHits, Pruning unused) {
    return CompositeComparator.create(
        createMixedFieldComparators(), (UserFieldSortOptions) this.options, numHits);
  }

  @Override
  public @Nullable IndexSorter getIndexSorter() {
    return this.scope.isPresent()
        ? null
        : new MqlMixedIndexSorter(this, (UserFieldSortOptions) this.options);
  }

  private record Scope(MongotSortField field, FieldPath path, ScopeFilters filters) {}

  /** Gives scoped sorts a distinct identity even against a plain, deserialized SortField. */
  private static final class ScopedComparatorSource extends FieldComparatorSource {
    private final Scope scope;

    private ScopedComparatorSource(Scope scope) {
      this.scope = scope;
    }

    @Override
    public FieldComparator<BsonValue> newComparator(
        String field, int numHits, Pruning pruning, boolean reversed) {
      return MqlMixedSort.scoped(this.scope.field(), this.scope.path(), this.scope.filters())
          .getComparator(numHits, pruning);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ScopedComparatorSource source && this.scope.equals(source.scope);
    }

    @Override
    public int hashCode() {
      return this.scope.hashCode();
    }
  }

  MixedFieldComparator[] createMixedFieldComparators() {
    return new MixedFieldComparator[] {
      mixedComparator(TypeField.NUMBER_DOUBLE_V2, BsonType.DOUBLE),
      mixedComparator(TypeField.NUMBER_INT64_V2, BsonType.INT64),
      mixedComparator(TypeField.DATE_V2, BsonType.DATE_TIME),
      mixedComparator(TypeField.TOKEN, BsonType.STRING),
      mixedComparator(TypeField.BOOLEAN, BsonType.BOOLEAN),
      mixedComparator(TypeField.UUID, BsonType.BINARY),
      mixedComparator(TypeField.NULL, BsonType.NULL),
      mixedComparator(TypeField.OBJECT_ID, BsonType.OBJECT_ID),
    };
  }

  private MixedFieldComparator mixedComparator(TypeField typeField, BsonType bsonType) {
    return new MixedFieldComparator(typeField, bsonType, this.field, this.embeddedRoot, this.scope);
  }
}
