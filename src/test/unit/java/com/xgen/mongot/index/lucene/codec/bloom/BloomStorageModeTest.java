package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.codec.bloom.MongotFuzzySet.StorageMode;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Test;

/**
 * Covers the {@link Feature#BLOOM_FILTER_ON_HEAP} static feature flag, which selects the bloom read
 * path. The flag names the fallback, not the default: disabled — the shipped state — reads bitsets
 * from the memory-mapped {@code .blm}, and enabling it copies them onto the JVM heap at segment
 * open.
 *
 * <p>The two modes must be indistinguishable to a reader — same {@code .blm} format, same
 * membership answers, same query results — and differ only in where the bytes live. That is what
 * makes the flag a restart-only kill switch rather than a migration.
 */
public class BloomStorageModeTest {

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();

  @After
  public void restoreBloomStorageMode() {
    BloomStoragePolicy.setStorageModeForTest(null);
  }

  /**
   * The flag ships disabled, so the input read path is the default and the heap path is the
   * fallback reached only by enabling the flag and restarting.
   */
  @Test
  public void flag_defaultsToDisabled() {
    assertThat(Feature.BLOOM_FILTER_ON_HEAP.getDefaultState()).isEqualTo(Feature.State.DISABLED);
    assertThat(FeatureFlags.getDefault().isEnabled(Feature.BLOOM_FILTER_ON_HEAP)).isFalse();
    assertThat(
            FeatureFlags.withDefaults()
                .enable(Feature.BLOOM_FILTER_ON_HEAP)
                .build()
                .isEnabled(Feature.BLOOM_FILTER_ON_HEAP))
        .isTrue();
  }

  /**
   * With no process-wide {@link FeatureFlags} installed — the codec-level unit-test case — the
   * policy must resolve to the flag's shipped default rather than an arbitrary fallback, so those
   * tests exercise the same path production does.
   */
  @Test
  public void storageMode_withoutProcessFlags_matchesShippedDefault() {
    BloomStoragePolicy.setStorageModeForTest(null);

    assertThat(BloomStoragePolicy.storageMode()).isEqualTo(StorageMode.INPUT);
    assertThat(BloomStoragePolicy.inputReadPathEnabled()).isTrue();
  }

  @Test
  public void storageMode_followsOverride() {
    BloomStoragePolicy.setStorageModeForTest(StorageMode.INPUT);
    assertThat(BloomStoragePolicy.inputReadPathEnabled()).isTrue();

    BloomStoragePolicy.setStorageModeForTest(StorageMode.HEAP);
    assertThat(BloomStoragePolicy.inputReadPathEnabled()).isFalse();
  }

  /** Under the fallback — the flag enabled — bitsets are on the heap and nothing is mapped. */
  @Test
  public void heapMode_readsBloomOntoHeap_andStillResolvesIdLookups() throws IOException {
    BloomStoragePolicy.setStorageModeForTest(StorageMode.HEAP);

    withBloomIndex(
        producer -> {
          assertThat(producer.hasLoadedBloom()).isTrue();
          assertThat(producer.heapBloomRamBytesUsed()).isGreaterThan(0L);
          assertThat(producer.mappedBloomBytes()).isEqualTo(0L);
          assertThat(producer.getLoadedBloomForField(ID_FIELD).readPathStorageMode())
              .isEqualTo(StorageMode.HEAP);
        });
  }

  /** Under the default — the flag disabled — the footprint is the mapping and heap reads zero. */
  @Test
  public void inputMode_readsBloomFromInput_andStillResolvesIdLookups() throws IOException {
    BloomStoragePolicy.setStorageModeForTest(StorageMode.INPUT);

    withBloomIndex(
        producer -> {
          assertThat(producer.hasLoadedBloom()).isTrue();
          assertThat(producer.heapBloomRamBytesUsed()).isEqualTo(0L);
          assertThat(producer.mappedBloomBytes()).isGreaterThan(0L);
          assertThat(producer.getLoadedBloomForField(ID_FIELD).readPathStorageMode())
              .isEqualTo(StorageMode.INPUT);
        });
  }

  /**
   * A {@code .blm} written under one mode must be readable under the other. The flag is a read-path
   * choice only, so flipping it — in either direction, as a rollout or a rollback — must not
   * require a reindex. This is what the restart-only kill switch rests on.
   */
  @Test
  public void blmWrittenInEitherMode_isReadableInTheOther() throws IOException {
    assertBlmRoundTrips(StorageMode.INPUT, StorageMode.HEAP);
    assertBlmRoundTrips(StorageMode.HEAP, StorageMode.INPUT);
  }

  private static void assertBlmRoundTrips(StorageMode writeMode, StorageMode readMode)
      throws IOException {
    Path tempDir = Files.createTempDirectory("mongot-bloom-storage-mode-test");
    try (Directory dir = new MMapDirectory(tempDir)) {
      BloomStoragePolicy.setStorageModeForTest(writeMode);
      writeIndex(dir);

      BloomStoragePolicy.setStorageModeForTest(readMode);
      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();
        assertThat(producer.getLoadedBloomForField(ID_FIELD).readPathStorageMode())
            .isEqualTo(readMode);

        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "doc0")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "doc999")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "missing")))).isEqualTo(0);
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(dir, false);
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private interface ProducerAssertions {
    void check(MongotBloomFilteredFieldsProducer producer) throws IOException;
  }

  private static void withBloomIndex(ProducerAssertions assertions) throws IOException {
    Path tempDir = Files.createTempDirectory("mongot-bloom-storage-mode-test");
    try (Directory dir = new MMapDirectory(tempDir)) {
      writeIndex(dir);

      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();
        assertions.check(producer);

        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "doc0")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "missing")))).isEqualTo(0);
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(dir, false);
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private static void writeIndex(Directory dir) throws IOException {
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(
                LuceneCodec.Factory.forIndexWithBloomFilter(
                    Map.of(), () -> true, Optional.empty(), FeatureFlags.getDefault()))
            .setUseCompoundFile(false);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 1_000; i++) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc" + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    }
  }
}
