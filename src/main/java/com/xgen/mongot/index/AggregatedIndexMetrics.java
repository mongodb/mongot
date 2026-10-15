package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import org.bson.BsonTimestamp;

/**
 * Per-index metrics folded across an index's generations for the conf call. {@code dataSize} and
 * {@code coldStorageSizeBytes} are both summed over generations, so a rebuild reports old plus new.
 * Cold storage bytes stay on their own key, never folded into {@code dataSize}, because every
 * reader of {@code dataSize} treats it as local disk.
 */
public record AggregatedIndexMetrics(
    long dataSize,
    long numDocs,
    BsonTimestamp opTime,
    long requiredMemory,
    long coldStorageSizeBytes) {

  /** Constructs a new AggregatedIndexMetrics. */
  @VisibleForTesting
  public AggregatedIndexMetrics {}

  /** Creates an AggregatedIndexMetrics from a list of metrics ordered by descending precedence. */
  public static AggregatedIndexMetrics createFromOrderedMetrics(
      List<IndexMetrics> metricsByDescendingPrecedence) {
    // we sum the data size over all the IndexMetrics
    long dataSize =
        metricsByDescendingPrecedence.stream()
            .mapToLong(indexMetrics -> indexMetrics.indexingMetrics().indexSize().toBytes())
            .sum();

    Optional<IndexMetrics> firstIndexMetrics = metricsByDescendingPrecedence.stream().findFirst();

    // we take the number of docs and opTime from the first IndexMetrics
    long numDocs =
        firstIndexMetrics
            .map(indexMetrics -> indexMetrics.indexingMetrics().numMongoDbDocs())
            .orElse(0L);

    BsonTimestamp opTime =
        firstIndexMetrics
            .flatMap(indexMetrics -> indexMetrics.indexingMetrics().replicationOpTime())
            .orElse(new BsonTimestamp(0L));

    long requiredMemory =
        firstIndexMetrics
            .map(indexMetrics -> indexMetrics.indexingMetrics().requiredMemory().toBytes())
            .orElse(0L);

    long coldStorageSizeBytes =
        metricsByDescendingPrecedence.stream()
            .mapToLong(indexMetrics -> indexMetrics.indexingMetrics().coldStorageSizeBytes())
            .sum();

    return new AggregatedIndexMetrics(
        dataSize, numDocs, opTime, requiredMemory, coldStorageSizeBytes);
  }
}
