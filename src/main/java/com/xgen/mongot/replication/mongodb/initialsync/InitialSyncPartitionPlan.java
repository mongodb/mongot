package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.util.Check.argNotNull;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.UUID;

/**
 * Result of the initial sync partition planning stage, which decides whether a collection scan can
 * be split across several concurrent {@code $natural} cursors.
 *
 * <p>Planning is the best effort and must never fail an initial sync. It always produces one of two
 * variants: a {@link PartitionedResult} to scan in parallel, or a {@link SequentialFallback} that
 * tells the caller to run the existing single cursor scan. Every condition that makes partitioning
 * unsafe or not worthwhile yields a {@link SequentialFallback} carrying the reason. {@link
 * ShortCircuitReason} lists them all. Callers never have to handle a failed plan.
 */
public sealed interface InitialSyncPartitionPlan
    permits InitialSyncPartitionPlan.PartitionedResult,
        InitialSyncPartitionPlan.SequentialFallback {

  /**
   * A plan to scan the collection with one concurrent cursor per partition.
   *
   * <p>Partitions are contiguous, cover the whole collection, and are listed in scan order. Each is
   * a self-contained unit of work.
   *
   * @param partitions the partition ranges, at least two, contiguous and in scan order
   * @param initialSyncId the sync source storage incarnation every partition is anchored to
   */
  record PartitionedResult(ImmutableList<PartitionRange> partitions, UUID initialSyncId)
      implements InitialSyncPartitionPlan {

    public PartitionedResult {
      argNotNull(partitions, "partitions");
      argNotNull(initialSyncId, "initialSyncId");
      checkArg(partitions.size() > 1, "a partitioned plan needs at least two partitions");
      checkArg(
          partitions.get(0).lowerRecordId().isEmpty(), "first partition must have no lower bound");
      checkArg(
          partitions.get(partitions.size() - 1).upperRecordId().isEmpty(),
          "last partition must have no upper bound");
      for (int i = 1; i < partitions.size(); i++) {
        checkArg(
            partitions.get(i).lowerRecordId().equals(partitions.get(i - 1).upperRecordId()),
            "partitions must be contiguous, found %s then %s",
            partitions.get(i - 1).upperRecordId(),
            partitions.get(i).lowerRecordId());
      }
    }
  }

  /**
   * A decision not to partition. The caller runs the existing single cursor scan.
   *
   * @param reason why partitioning was skipped, for metrics and logs only. Callers must not branch
   *     on it
   */
  record SequentialFallback(ShortCircuitReason reason) implements InitialSyncPartitionPlan {

    public SequentialFallback {
      argNotNull(reason, "reason");
    }
  }

  /**
   * One partition's {@code $recordId} range, exclusive lower and inclusive upper: {@code
   * (lowerRecordId, upperRecordId]}. A self-contained unit of work for one {@code $natural} cursor.
   *
   * @param lowerRecordId exclusive lower bound, empty for the first partition
   * @param upperRecordId inclusive upper bound, empty for the last partition
   */
  record PartitionRange(Optional<Long> lowerRecordId, Optional<Long> upperRecordId) {

    public PartitionRange {
      argNotNull(lowerRecordId, "lowerRecordId");
      argNotNull(upperRecordId, "upperRecordId");
      if (lowerRecordId.isPresent() && upperRecordId.isPresent()) {
        checkArg(
            lowerRecordId.get() < upperRecordId.get(),
            "partition lower bound must be below upper bound, found %s and %s",
            lowerRecordId.get(),
            upperRecordId.get());
      }
    }
  }

  /** Why partition planning declined to partition. Used as a metric and log tag only. */
  enum ShortCircuitReason {
    /** Empty, below the minimum document count, or a short {@code $sample}. */
    TOO_FEW_DOCUMENTS,
    /** {@code $recordId} is {@code BinData} on clustered collections. */
    CLUSTERED_COLLECTION,
    /** Sync source predates the natural order resume token contract. */
    UNSUPPORTED_SERVER,
    /** Sync source did not report an {@code $initialSyncId}. */
    NO_INITIAL_SYNC_ID,
    /** Sampling failed, or the post batch resume token was missing. */
    SAMPLE_UNAVAILABLE,
    /** Planning exceeded its wall clock budget. */
    PLANNING_BUDGET_EXCEEDED,
    /** A planning read failed, or planning hit an unexpected error. */
    PLANNING_FAILED
  }
}
