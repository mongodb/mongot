package com.xgen.mongot.index.lucene;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.lucene.backing.DiskStats;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.lucene.search.ReferenceManager;
import org.jetbrains.annotations.Nullable;

public class DiskIndexBackingStrategy implements IndexBackingStrategy {

  private static final Duration DISK_STATS_CACHE_EXPIRATION = Duration.ofMinutes(3);

  private record PartitionStats(long[] partitionSizesBytes, DiskStats totalStats) {
    public static final PartitionStats EMPTY = new PartitionStats(new long[0], DiskStats.EMPTY);
  }

  private record CachedPartitionStats(PartitionStats stats, Instant sampledAt) {
    static CachedPartitionStats of(PartitionStats stats) {
      return new CachedPartitionStats(stats, Instant.now());
    }

    boolean isFresh(Instant now) {
      return Duration.between(this.sampledAt, now).compareTo(DISK_STATS_CACHE_EXPIRATION) < 0;
    }
  }

  private final int numPartitions;
  private final Path directoryPath;
  private final PerIndexMetricsFactory metricsFactory;
  private final ScheduledExecutorService refreshExecutor;
  private final Duration refreshInterval;
  private final AtomicDirectoryRemover directoryRemover;
  private final Path metadataDirectoryPath;

  @GuardedBy("this")
  private Optional<CachedPartitionStats> cachedPartitionStats;

  @GuardedBy("this")
  private boolean released;

  public DiskIndexBackingStrategy(
      Path directoryPath,
      PerIndexMetricsFactory metricsFactory,
      ScheduledExecutorService refreshExecutor,
      Duration refreshInterval,
      AtomicDirectoryRemover directoryRemover,
      Path metadataDirectoryPath,
      int numPartitions) {
    this.directoryPath = directoryPath;
    this.metricsFactory = metricsFactory;
    this.refreshExecutor = refreshExecutor;
    this.refreshInterval = refreshInterval;
    this.directoryRemover = directoryRemover;
    this.metadataDirectoryPath = metadataDirectoryPath;
    this.numPartitions = numPartitions;
    this.cachedPartitionStats = Optional.empty();
  }

  @Override
  public Closeable createIndexRefresher(
      Supplier<IndexStatus> statusRef, ImmutableList<ReferenceManager<?>> searcherManagers) {
    var refresherMetricsFactory = this.metricsFactory.childMetricsFactory("luceneIndexRefresher");
    return new PeriodicLuceneIndexRefresher(
        this.refreshExecutor,
        this.refreshInterval,
        searcherManagers,
        statusRef,
        refresherMetricsFactory);
  }

  @Override
  public synchronized void releaseResources() throws IOException {
    this.released = true;
    this.directoryRemover.deleteDirectory(this.directoryPath);
    this.directoryRemover.deleteDirectory(this.metadataDirectoryPath);
    this.cachedPartitionStats = Optional.of(CachedPartitionStats.of(PartitionStats.EMPTY));
  }

  @Override
  public DiskStats getDiskStats() {
    return currentPartitionStats().totalStats();
  }

  @Override
  public DiskStats refreshDiskStats() throws IOException {
    PartitionStats stats = scanDiskStatsOrThrow();
    synchronized (this) {
      if (this.released) {
        throw new IOException("index directory has been released");
      }
      this.cachedPartitionStats = Optional.of(CachedPartitionStats.of(stats));
    }
    return stats.totalStats();
  }

  @Override
  public long getIndexSizeForIndexPartition(int indexPartitionId) {
    long[] partitionSizes = currentPartitionStats().partitionSizesBytes();
    return 0 <= indexPartitionId && indexPartitionId < partitionSizes.length
        ? partitionSizes[indexPartitionId]
        : 0L;
  }

  @Override
  public synchronized void clearMetadata() throws IOException {
    this.directoryRemover.deleteFilesInDirectory(this.metadataDirectoryPath);
  }

  private synchronized PartitionStats currentPartitionStats() {
    Instant now = Instant.now();
    Optional<CachedPartitionStats> freshCachedStats =
        this.cachedPartitionStats.filter(sample -> sample.isFresh(now));
    if (freshCachedStats.isPresent()) {
      return freshCachedStats.get().stats();
    }
    CachedPartitionStats sample = CachedPartitionStats.of(scanDiskStatsBestEffort());
    this.cachedPartitionStats = Optional.of(sample);
    return sample.stats();
  }

  private PartitionStats scanDiskStatsOrThrow() throws IOException {
    synchronized (this) {
      if (this.released) {
        throw new IOException("index directory has been released");
      }
    }
    var visitor = new DiskStatsVisitor(this.directoryPath, this.numPartitions);
    Files.walkFileTree(this.directoryPath, visitor);
    return visitor.getPartitionStats();
  }

  private PartitionStats scanDiskStatsBestEffort() {
    try {
      return scanDiskStatsOrThrow();
    } catch (IOException e) {
      return PartitionStats.EMPTY;
    }
  }

  /**
   * A visitor that can accumulate all file-related metrics in a single pass over the file tree.
   * This is important because these metrics are IO-bound and can block for long periods of time on
   * large indexes.
   */
  static class DiskStatsVisitor implements FileVisitor<Path> {

    /** Sum of file sizes per partition ID. Empty for an unpartitioned index. */
    private final long[] partitionSizeBytes;

    private long totalSizeBytes = 0L;
    private long largestFileByteSize = 0L;
    private long numFiles = 0;
    private long graphAndQuantizedBytes = 0L;
    private long cfsBytes = 0L;
    // A quantized field writes both a .veq and a sibling .vec under the same prefix, but only the
    // .veq is heap-resident, so a .vec counts only when no same-prefix .veq exists.
    private final Map<String, Long> rawVecBytesByPrefix = new HashMap<>();
    private final Set<String> quantizedFieldPrefixes = new HashSet<>();

    private final Path indexRoot;

    @Var private OptionalInt currentPartition = OptionalInt.empty();

    DiskStatsVisitor(Path indexRoot, int numPartitions) {
      this.indexRoot = indexRoot;
      this.partitionSizeBytes = new long[numPartitions];
    }

    public PartitionStats getPartitionStats() {
      @Var long vectorDataBytes = this.graphAndQuantizedBytes;
      for (Map.Entry<String, Long> entry : this.rawVecBytesByPrefix.entrySet()) {
        if (!this.quantizedFieldPrefixes.contains(entry.getKey())) {
          vectorDataBytes += entry.getValue();
        }
      }

      // Estimate vector bytes inside .cfs compound files by assuming they contain the same
      // proportion of vector data as the non-.cfs files we could inspect directly. When there are
      // no non-.cfs files (e.g. an all-compound index) there is no basis to extrapolate, so .cfs
      // contributes nothing. vectorDataBytes only counts non-.cfs files, so it is 0 whenever
      // nonCfsTotal is 0.
      long nonCfsTotal = this.totalSizeBytes - this.cfsBytes;
      long estimatedCfsRequired =
          nonCfsTotal > 0 ? (long) (this.cfsBytes * ((double) vectorDataBytes / nonCfsTotal)) : 0L;
      long totalRequiredMemory = vectorDataBytes + estimatedCfsRequired;
      DiskStats totalStats =
          new DiskStats(
              this.totalSizeBytes, this.largestFileByteSize, this.numFiles, totalRequiredMemory);
      return new PartitionStats(this.partitionSizeBytes, totalStats);
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      ++this.numFiles;
      long size = attrs.size();
      this.largestFileByteSize = Math.max(this.largestFileByteSize, size);
      this.totalSizeBytes += size;
      this.currentPartition.ifPresent(slot -> this.partitionSizeBytes[slot] += size);
      String fileName = file.getFileName().toString();
      if (fileName.endsWith(".vex")) {
        // .vex = HNSW graph
        this.graphAndQuantizedBytes += size;
      } else if (fileName.endsWith(".veq")) {
        // .veq = quantized vectors
        this.graphAndQuantizedBytes += size;
        this.quantizedFieldPrefixes.add(stripVectorExtension(file));
      } else if (fileName.endsWith(".vec")) {
        // .vec = raw float vectors
        this.rawVecBytesByPrefix.merge(stripVectorExtension(file), size, Long::sum);
      } else if (fileName.endsWith(".cfs")) {
        this.cfsBytes += size;
      }
      return FileVisitResult.CONTINUE;
    }

    /**
     * Returns the full file path with its trailing vector extension removed, used to pair a field's
     * .veq and .vec siblings. The full path is used so identically-named files in different
     * partition subdirectories do not collide. Only called for .veq/.vec
     */
    private static String stripVectorExtension(Path file) {
      String path = file.toString();
      return path.substring(0, path.length() - ".vec".length());
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      // Ignore errors when visiting files.
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      // Immediate child directories potentially correspond to partitions. Extract the partition
      // ID so we can track partition sizes
      int pathSegments = dir.getNameCount();
      if (pathSegments == this.indexRoot.getNameCount() + 1) {
        String partitionName = dir.getName(pathSegments - 1).toString();
        @Nullable Integer id = Ints.tryParse(partitionName, 16);
        if (id != null && 0 <= id && id < this.partitionSizeBytes.length) {
          this.currentPartition = OptionalInt.of(id);
        }
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      // We just finished exploring an immediate child, so reset the current partition
      // This is harmless if `dir` didn't correspond to a partition.
      int pathSegments = dir.getNameCount();
      if (pathSegments == this.indexRoot.getNameCount() + 1) {
        this.currentPartition = OptionalInt.empty();
      }

      return FileVisitResult.CONTINUE;
    }
  }
}
