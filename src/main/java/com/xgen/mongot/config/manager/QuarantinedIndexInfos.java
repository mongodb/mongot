package com.xgen.mongot.config.manager;

import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * A snapshot of the indexes whose config journal entry could not be parsed, kept split by the
 * journal state they were read from because an index can be quarantined in both at once.
 */
public record QuarantinedIndexInfos(
    List<QuarantinedIndex> liveIndexes, List<QuarantinedIndex> stagedIndexes) {

  public static final QuarantinedIndexInfos EMPTY =
      new QuarantinedIndexInfos(ImmutableList.of(), ImmutableList.of());

  public static QuarantinedIndexInfos from(IncompatibleIndexes incompatibleIndexes) {
    return new QuarantinedIndexInfos(
        ImmutableList.copyOf(incompatibleIndexes.getLiveIndexes()),
        ImmutableList.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  public boolean isEmpty() {
    return this.liveIndexes.isEmpty() && this.stagedIndexes.isEmpty();
  }
}
