package com.xgen.mongot.monitor.index;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.InitializedIndex;
import java.io.IOException;
import java.util.OptionalLong;

/**
 * Sums the on-disk size of every index generation mongot holds, whichever state it is in: live and
 * queryable, staged for a rebuild, or phasing out awaiting cursor cleanup.
 *
 * <p>This is deliberately not the same total as {@code
 * AutoscalingMetrics.liveIndexSizeTotalBytes()}, which counts live generations only: autoscaling
 * sizes a node to its serving footprint, while this gate has to account for the transient footprint
 * of a rebuild.
 */
public final class OnDiskIndexSizeSampler {

  private final InitializedIndexCatalog initializedIndexCatalog;

  public OnDiskIndexSizeSampler(InitializedIndexCatalog initializedIndexCatalog) {
    this.initializedIndexCatalog = initializedIndexCatalog;
  }

  /**
   * Zero is a successful empty-catalog result; empty means the total could not be determined, and
   * callers must keep their previous reading rather than assume a smaller total.
   *
   * <p>Walks one index directory per generation, so this belongs off hot paths.
   */
  public OptionalLong sampleTotalIndexBytes() throws IOException {
    @Var long total = 0;
    for (InitializedIndex initializedIndex : this.initializedIndexCatalog.getIndexes()) {
      try {
        total = Math.addExact(total, initializedIndex.refreshAndGetIndexSize());
      } catch (RuntimeException e) {
        return OptionalLong.empty();
      }
    }
    return OptionalLong.of(total);
  }
}
