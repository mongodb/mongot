package com.xgen.testing.mongot.index.query.sort;

import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public final class UserFieldSortOptionsBuilder
    extends SortOptionsBuilder<UserFieldSortOptions, UserFieldSortOptionsBuilder> {
  private NullEmptySortPosition nullEmptySortPosition = NullEmptySortPosition.LOWEST;
  private Optional<EmbeddedSortScope> embeddedScope = Optional.empty();

  @Override
  UserFieldSortOptionsBuilder getBuilder() {
    return this;
  }

  public static UserFieldSortOptionsBuilder builder() {
    return new UserFieldSortOptionsBuilder();
  }

  public UserFieldSortOptionsBuilder nullEmptySortPosition(
      NullEmptySortPosition nullEmptySortPosition) {
    this.nullEmptySortPosition = nullEmptySortPosition;
    return this;
  }

  /** Sets the embedded-document scope (parsed path and raw operator). */
  public UserFieldSortOptionsBuilder embeddedScope(EmbeddedSortScope embeddedScope) {
    this.embeddedScope = Optional.of(embeddedScope);
    return this;
  }

  @Override
  public UserFieldSortOptions build() {
    var sortOrder = Check.isPresent(getSortOrder(), "sortOrder");
    return new UserFieldSortOptions(
        sortOrder,
        sortOrder == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX,
        this.nullEmptySortPosition,
        this.embeddedScope);
  }
}
