package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * This class encapsulates options for sorting non-metadata fields, which can contain null or empty
 * values. It extends SortOptions to specify how null or missing values should be positioned within
 * the sort order.
 *
 * <p>An optional {@link EmbeddedSortScope} selects matching children of an {@code
 * embeddedDocuments} array. The scope survives inversion and BSON serialization. Without a scope,
 * the field is sorted on its own document-indexed value.
 */
public record UserFieldSortOptions(
    SortOrder order,
    SortSelector selector,
    NullEmptySortPosition nullEmptySortPosition,
    Optional<EmbeddedSortScope> embeddedScope)
    implements SortOptions {

  /**
   * Ascending sort with default values for all other options. Same as using {@code sort: {foo: 1}}.
   * This is an ascending sort, with a MIN selector, and a NULLS FIRST sorting behavior.
   */
  public static final UserFieldSortOptions DEFAULT_ASC =
      new UserFieldSortOptions(SortOrder.ASC, NullEmptySortPosition.LOWEST);

  /**
   * Descending sort with default values for all other options. Same as using {@code sort: {foo:
   * -1}}. This is a descending sort, with a MAX selector, and a NULLS LAST sorting behavior.
   */
  public static final UserFieldSortOptions DEFAULT_DESC =
      new UserFieldSortOptions(SortOrder.DESC, NullEmptySortPosition.LOWEST);

  public UserFieldSortOptions(SortOrder order, NullEmptySortPosition nullEmptySortPosition) {
    this(
        order,
        order == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX,
        nullEmptySortPosition,
        Optional.empty());
  }

  /** Convenience constructor for an unscoped sort (no embedded-document scope). */
  public UserFieldSortOptions(
      SortOrder order, SortSelector selector, NullEmptySortPosition nullEmptySortPosition) {
    this(order, selector, nullEmptySortPosition, Optional.empty());
  }

  /**
   * Returns a new {@link SortOptions} object that has all the same settings, except it has an
   * inverted {@link SortOrder}.
   *
   * <p>Note: The {@link SortSelector} is preserved in the new object even though its default value
   * is usually determined by the {@link SortOrder}. That is, {@code DEFAULT_ASC.invert() !=
   * DEFAULT_DESC} because {@link #DEFAULT_ASC} and {@code DEFAULT_ASC.invert()} both use {@link
   * SortSelector#MIN}.
   */
  @Override
  public UserFieldSortOptions invert() {
    return new UserFieldSortOptions(
        this.order.invert(), this.selector, this.nullEmptySortPosition, this.embeddedScope);
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder().field(Fields.ORDER, Optional.of(this.order));
    builder.field(Fields.NO_DATA, Optional.of(this.nullEmptySortPosition));
    builder.field(Fields.EMBEDDED_DOCUMENT, this.embeddedScope);
    return builder.build();
  }

  @Override
  public String toString() {
    return "UserFieldSortOptions("
        + "nullEmptySortPosition="
        + this.nullEmptySortPosition
        + ", embeddedScope="
        + this.embeddedScope
        + ')';
  }
}
