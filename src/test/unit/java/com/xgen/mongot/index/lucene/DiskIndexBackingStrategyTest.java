package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.lucene.backing.DiskStats;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.DirectorySize;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Runs under every {@link LuceneCodec.WriteFormat}: results must be write-format-invariant. */
@RunWith(Parameterized.class)
public class DiskIndexBackingStrategyTest {

  @Parameterized.Parameters(name = "{0}")
  public static LuceneCodec.WriteFormat[] writeFormats() {
    return LuceneCodec.WriteFormat.values();
  }

  @Parameterized.Parameter public LuceneCodec.WriteFormat writeFormat;

  private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(1);
  private static final ScheduledExecutorService REFRESH_EXECUTOR =
      mock(ScheduledExecutorService.class);
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_DOCS = 3;

  private Path indexDir;

  private AtomicDirectoryRemover atomicDirectoryRemover;

  @Before
  public void setup() throws IOException {
    this.indexDir = TestUtils.getTempFolderPath();
    List<Path> partitions =
        List.of(
            this.indexDir.resolve("3"),
            this.indexDir.resolve("5"), // Looks like partition ID, but is out of range
            this.indexDir.resolve("abc"), // Looks like hex partition ID, but is out of range
            this.indexDir.resolve("0"),
            this.indexDir.resolve("1"),
            this.indexDir.resolve("meta"));

    for (Path partitionDir : partitions) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(new LuceneCodec(this.writeFormat))
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setCommitOnClose(true);

      try (FSDirectory directory = FSDirectory.open(partitionDir);
          IndexWriter writer = new IndexWriter(directory, config)) {
        for (int i = 0; i < NUM_DOCS; ++i) {
          Document doc = new Document();
          doc.add(
              new StringField(FieldName.MetaField.ID.getLuceneFieldName(), "doc_" + i, Store.YES));
          writer.addDocument(doc);
          writer.flush();
        }
      }
    }
    Path trash = this.indexDir.resolveSibling(UUID.randomUUID().toString());
    this.atomicDirectoryRemover = new AtomicDirectoryRemover(trash);
  }

  @Test
  public void getDiskStats_partitionedIndex_returnsNonZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);
    long expectedTotalSize = DirectorySize.of(this.indexDir.toFile());

    DiskStats diskStats = strat.getDiskStats();

    assertThat(diskStats.largestFileByteSize()).isGreaterThan(0);
    assertThat(diskStats.largestFileByteSize()).isAtMost(expectedTotalSize / NUM_PARTITIONS);
    assertThat(diskStats.totalFileByteSize()).isEqualTo(expectedTotalSize);
    assertThat(diskStats.totalFileByteSize()).isAtLeast(diskStats.largestFileByteSize());
    assertThat(diskStats.numFiles()).isAtLeast(NUM_PARTITIONS * NUM_DOCS);
  }

  @Test
  public void getDiskStats_unpartitionedIndex_returnsNonZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir.resolve("0"),
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    DiskStats diskStats = strat.getDiskStats();

    assertThat(diskStats.largestFileByteSize()).isGreaterThan(0);
    assertThat(diskStats.totalFileByteSize()).isGreaterThan(0);
    assertThat(diskStats.totalFileByteSize()).isAtLeast(diskStats.largestFileByteSize());
    assertThat(diskStats.numFiles()).isAtLeast(NUM_DOCS);
  }

  @Test
  public void getDiskStats_indexClosed_returnsZero() throws Exception {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    strat.releaseResources();
    DiskStats diskStats = strat.getDiskStats();

    assertThat(diskStats.largestFileByteSize()).isEqualTo(0);
    assertThat(diskStats.numFiles()).isEqualTo(0);
    assertThat(diskStats.totalFileByteSize()).isEqualTo(0);
    assertThat(diskStats.requiredMemoryBytes()).isEqualTo(0);
  }

  @Test
  public void getIndexSizeForIndexPartition_indexClosed_returnsZero() throws Exception {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    strat.releaseResources();

    for (int i = 0; i < NUM_PARTITIONS; ++i) {
      long size = strat.getIndexSizeForIndexPartition(i);
      assertWithMessage("Partition %s should be empty", i).that(size).isEqualTo(0);
    }
  }

  @Test
  public void getIndexSizeForIndexPartition_validPartition_returnsTotalSize() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    long partition0 = strat.getIndexSizeForIndexPartition(0);
    long partition1 = strat.getIndexSizeForIndexPartition(1);
    long partition3 = strat.getIndexSizeForIndexPartition(3);

    assertThat(partition0).isEqualTo(DirectorySize.of(this.indexDir.resolve("0").toFile()));
    assertThat(partition1).isEqualTo(DirectorySize.of(this.indexDir.resolve("1").toFile()));
    assertThat(partition3).isEqualTo(DirectorySize.of(this.indexDir.resolve("3").toFile()));
  }

  @Test
  public void getIndexSizeForIndexPartition_emptyPartition_returnsZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    long partition2 = strat.getIndexSizeForIndexPartition(2);

    assertThat(partition2).isEqualTo(0);
  }

  @Test
  public void getIndexSizeForIndexPartition_allPartitions_areLessThanTotalSize() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    DiskStats diskStats = strat.getDiskStats();
    long partition0 = strat.getIndexSizeForIndexPartition(0);
    long partition1 = strat.getIndexSizeForIndexPartition(1);
    long partition2 = strat.getIndexSizeForIndexPartition(2);
    long partition3 = strat.getIndexSizeForIndexPartition(3);

    long totalSize = partition0 + partition1 + partition2 + partition3;
    assertThat(ImmutableSet.of(partition0, partition1, partition3)).hasSize(1);
    assertThat(totalSize).isAtMost(diskStats.totalFileByteSize());
  }

  @Test
  public void getIndexSizeForIndexPartition_invalidPartition_returnsZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    long offByOnePartition = strat.getIndexSizeForIndexPartition(4);
    long invalidPartition = strat.getIndexSizeForIndexPartition(9001);
    long negativePartition = strat.getIndexSizeForIndexPartition(-1);

    assertThat(offByOnePartition).isEqualTo(0);
    assertThat(invalidPartition).isEqualTo(0);
    assertThat(negativePartition).isEqualTo(0);
  }

  @Test
  public void getIndexSizeForIndexPartition_invalidHexPartition_returnsZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    long invalidPartition = strat.getIndexSizeForIndexPartition(Integer.parseInt("abc", 16));

    assertThat(invalidPartition).isEqualTo(0);
  }

  @Test
  public void getIndexSizeForIndexPartition_unpartitionedIndex_returnsZero() {
    Path unpartitionedIndexDir = this.indexDir.resolve("0");
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            unpartitionedIndexDir,
            unpartitionedIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    long invalidPartition = strat.getIndexSizeForIndexPartition(0);

    assertThat(invalidPartition).isEqualTo(0);
  }

  @Test
  public void getDiskStats_vectorFilesAcrossPartitions_sumsRequiredMemory() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.createDirectories(vectorIndexDir.resolve("0"));
    Files.createDirectories(vectorIndexDir.resolve("1"));
    Files.write(vectorIndexDir.resolve("0/_0.vex"), new byte[100]);
    Files.write(vectorIndexDir.resolve("1/_0.veq"), new byte[200]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            2);

    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(300);
  }

  @Test
  public void releaseResources_openIndex_removesEntireDirectory() throws Exception {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    strat.releaseResources();

    assertFalse(this.indexDir.toFile().exists());
    assertFalse(this.indexDir.resolve("meta").toFile().exists());
  }

  @Test
  public void getDiskStats_vexAndVeqFiles_countedInRequiredMemory() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0.vex"), new byte[100]);
    Files.write(vectorIndexDir.resolve("_0.veq"), new byte[200]);
    Files.write(vectorIndexDir.resolve("_0.fnm"), new byte[50]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    DiskStats diskStats = strat.getDiskStats();

    assertThat(diskStats.requiredMemoryBytes()).isEqualTo(300);
  }

  @Test
  public void getDiskStats_vecFileCountedInRequiredMemory() throws Exception {
    // .vec = raw float vectors (unquantized field). Counted in full.
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0.vec"), new byte[150]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(150);
  }

  @Test
  public void getDiskStats_cfsWithVectorFiles_extrapolatesRequiredMemory() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    // Non-CFS: 100 bytes total, 40 bytes vector data → ratio = 0.4
    Files.write(vectorIndexDir.resolve("_0.vex"), new byte[40]);
    Files.write(vectorIndexDir.resolve("_0.fnm"), new byte[60]);
    // CFS: 200 bytes → extrapolated vector contribution = 200 * 0.4 = 80
    Files.write(vectorIndexDir.resolve("_1.cfs"), new byte[200]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // expected = 40 + (long)(200 * 40.0 / 100) = 40 + 80 = 120
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(120);
  }

  @Test
  public void getDiskStats_allCfsNoVectorFiles_requiredMemoryIsZero() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0.cfs"), new byte[500]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(0);
  }

  @Test
  public void getDiskStats_onlyVectorFilesNoCfs_exactRequiredMemory() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0.vex"), new byte[300]);
    Files.write(vectorIndexDir.resolve("_0.veq"), new byte[200]);
    // A non-quantized field's raw vectors (no sibling .veq with this prefix), so it counts.
    Files.write(vectorIndexDir.resolve("_1.vec"), new byte[100]);
    Files.write(vectorIndexDir.resolve("_0.fnm"), new byte[50]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // No CFS files — requiredMemory is exactly the sum of all vector file sizes.
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(600);
  }

  @Test
  public void getDiskStats_quantizedField_excludesSiblingRawVec() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    // A quantized field writes a .veq plus a raw .vec sibling under the same prefix. The raw .vec
    // is kept only for rescoring/merge and is not heap-resident, so it must be excluded.
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.vex"), new byte[300]);
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.veq"), new byte[200]);
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.vec"), new byte[100]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // .vex (300) + .veq (200); the same-prefix .vec (100) is excluded.
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(500);
  }

  @Test
  public void getDiskStats_mixedQuantizedAndRawFields_excludesOnlyQuantizedRawVec()
      throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.vex"), new byte[300]);
    // Quantized field: .veq + sibling raw .vec (excluded).
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.veq"), new byte[200]);
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_0.vec"), new byte[100]);
    // Non-quantized field: raw .vec with a different prefix and no sibling .veq (counted).
    Files.write(vectorIndexDir.resolve("_0_Lucene99HnswVectorsFormat_1.vec"), new byte[50]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // .vex (300) + .veq (200) + non-quantized .vec (50); quantized field's raw .vec (100) excluded.
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(550);
  }

  @Test
  public void getDiskStats_largeCfsSmallVectorDataBytes_extrapolationDominates() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    // Non-CFS: 1000 bytes total, 10 bytes vector data → ratio = 10/1000 = 0.01
    Files.write(vectorIndexDir.resolve("_0.vex"), new byte[10]);
    Files.write(vectorIndexDir.resolve("_0.fnm"), new byte[990]);
    // CFS: 50_000 bytes → extrapolated = (long)(50_000 * 10/1000) = 500
    Files.write(vectorIndexDir.resolve("_1.cfs"), new byte[50_000]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // expected = 10 + (long)(50_000 * 10.0 / 1000) = 10 + 500 = 510
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(510);
  }

  @Test
  public void getDiskStats_noCfsButLargeNonVectorFiles_requiredMemoryIsZero() throws Exception {
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    Files.write(vectorIndexDir.resolve("_0.cfs"), new byte[100_000]);
    Files.write(vectorIndexDir.resolve("_1.cfs"), new byte[200_000]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    // No non-CFS vector files observed → ratio is 0 → CFS extrapolation is 0.
    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(0);
  }

  @Test
  public void getDiskStats_noVectorFiles_requiredMemoryIsZero() {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    assertThat(strat.getDiskStats().requiredMemoryBytes()).isEqualTo(0);
  }

  @Test
  public void getDiskStats_largeFileSizes_doesNotOverflow() throws Exception {
    // Verify that large (but realistic) file sizes do not cause overflow in the CFS ratio
    // arithmetic or the final requiredMemoryBytes sum.
    Path vectorIndexDir = TestUtils.getTempFolderPath();
    // Use sizes large enough to exercise the arithmetic without writing GB to disk.
    int fileSizeBytes = 10 * 1024 * 1024; // 10 MB
    Files.write(vectorIndexDir.resolve("_0.vex"), new byte[fileSizeBytes]);
    Files.write(vectorIndexDir.resolve("_0.fnm"), new byte[1024]);
    Files.write(vectorIndexDir.resolve("_1.cfs"), new byte[fileSizeBytes]);

    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            new AtomicDirectoryRemover(vectorIndexDir.resolveSibling(UUID.randomUUID().toString())),
            vectorIndexDir,
            vectorIndexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            1);

    DiskStats diskStats = strat.getDiskStats();
    // No overflow/wraparound: requiredMemory is at least the directly-counted .vex bytes and never
    // exceeds the total bytes on disk. An overflow would push it negative or above the total.
    assertThat(diskStats.requiredMemoryBytes()).isAtLeast(fileSizeBytes);
    assertThat(diskStats.requiredMemoryBytes()).isAtMost(diskStats.totalFileByteSize());
  }

  @Test
  public void clearMetadata_openIndex_removesOnlyMetadata() throws Exception {
    IndexBackingStrategy strat =
        IndexBackingStrategyFactory.diskBacked(
            REFRESH_EXECUTOR,
            REFRESH_INTERVAL,
            this.atomicDirectoryRemover,
            this.indexDir,
            this.indexDir.resolve("meta"),
            SearchIndex.mockMetricsFactory(),
            NUM_PARTITIONS);

    strat.clearMetadata();

    assertTrue(this.indexDir.toFile().exists());
    assertThat(DirectorySize.of(this.indexDir.toFile())).isGreaterThan(0);
    assertThat(DirectorySize.of(this.indexDir.resolve("0").toFile())).isGreaterThan(0);
    // Metadata should be cleared, but the directory should still exist.
    assertTrue(this.indexDir.resolve("meta").toFile().exists());
    assertThat(DirectorySize.of(this.indexDir.resolve("meta").toFile())).isEqualTo(0);
  }
}
