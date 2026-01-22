package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.monitor.DiskMonitor;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.HysteresisGate;
import com.xgen.mongot.monitor.ToggleGate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

/**
 * Wrapper for <code>MergePolicy</code> that blocks merges according to the disk usage.
 *
 * <p>This class blocks merges according to the return value of the given gate. When the disk usage
 * is high, no new merges will be scheduled. But ongoing merges will still proceed.
 */
public class DiskUtilizationAwareMergePolicy extends FilterMergePolicy {
  private final Gate mergeGate;
  // Track every segment merge that we discard.
  private final Counter discardedMergeCounter;

  public DiskUtilizationAwareMergePolicy(
      MergePolicy parent, Gate mergeGate, MeterRegistry meterRegistry) {
    super(parent);
    this.mergeGate = mergeGate;
    var metricsFactory =
        new MetricsFactory(
            "diskUtilizationAwarenessMergePolicy",
            meterRegistry,
            ServerStatusDataExtractor.Scope.LUCENE.getTag());
    this.discardedMergeCounter = metricsFactory.counter("discardedMerge");
  }

  public static Gate createMergeGate(LuceneConfig config, DiskMonitor diskMonitor) {
    var mergeConfig = config.mergePolicyDiskUtilizationConfig();
    if (mergeConfig.isEmpty()) {
      return ToggleGate.opened();
    }
    var gate =
        new HysteresisGate(mergeConfig.get().openThreshold(), mergeConfig.get().closeThreshold());
    diskMonitor.register(gate);
    return gate;
  }

  @Override
  @Nullable
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return maybePruneMergeSpecification(super.findMerges(mergeTrigger, segmentInfos, mergeContext));
  }

  @Override
  @Nullable
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return maybePruneMergeSpecification(
        super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext));
  }

  @Override
  @Nullable
  public MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    return maybePruneMergeSpecification(
        super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext));
  }

  @Override
  @Nullable
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    return maybePruneMergeSpecification(super.findForcedDeletesMerges(segmentInfos, mergeContext));
  }

  @Nullable
  MergeSpecification maybePruneMergeSpecification(@Nullable MergeSpecification mergeSpecification) {
    if (mergeSpecification == null) {
      return null;
    }

    if (this.mergeGate.isClosed()) {
      // If disk usage is high, reject all merges by returning null.
      this.discardedMergeCounter.increment(mergeSpecification.merges.size());
      return null;
    }
    return mergeSpecification;
  }
}
