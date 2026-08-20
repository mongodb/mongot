package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.codec.bloom.MongotFuzzySet.ContainsResult;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
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
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the risks introduced by reading {@code _id} bloom bitsets straight from the
 * memory-mapped {@code .blm} instead of copying them onto the JVM heap (see {@link
 * MongotFuzzySet#fromInput} and {@link MongotBloomFilteredFieldsProducer}):
 *
 * <ul>
 *   <li><b>Per-field slice offsets.</b> A {@code .blm} with more than one bloom field must hand
 *       each field a {@link org.apache.lucene.store.RandomAccessInput} slice over <em>its own</em>
 *       word region; an off-by-one in the two-pass offset walk would make one field read another's
 *       bits.
 *   <li><b>Input lifecycle.</b> Eviction drops the fuzzy-set references but must leave the mapped
 *       {@code .blm} open for any probe still holding a reference, and the input must only be
 *       closed (invalidating every slice) when the producer closes.
 *   <li><b>Concurrency.</b> Searching a segment while it is evicted must never observe a torn read
 *       or wrong result — bloom only ever changes speed, never correctness.
 * </ul>
 */
public class MongotBloomInputRiskTest {

  // These tests assert input-read-path behavior, so they pin the storage mode rather than
  // inheriting whatever Feature.BLOOM_FILTER_ON_HEAP resolves to.
  @Before
  public void forceOffHeapBloom() {
    BloomStoragePolicy.setStorageModeForTest(MongotFuzzySet.StorageMode.INPUT);
  }

  @After
  public void restoreBloomStorageMode() {
    BloomStoragePolicy.setStorageModeForTest(null);
  }

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();
  private static final String SECOND_FIELD = "secondary";

  /**
   * Two bloom fields written into a single {@code .blm}: each field's slice must read only its own
   * bits. Values are drawn from disjoint spaces so a slice-offset bug would surface as a bloom from
   * one field admitting/denying the other field's terms.
   */
  @Test
  public void multiFieldBlm_slicesEachFieldIndependently() throws IOException {
    Path tempDir = Files.createTempDirectory("mongot-bloom-input-multifield");
    try (Directory dir = new MMapDirectory(tempDir)) {
      writeTwoBloomFields(dir, 2_000);

      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();

        MongotFuzzySet idBloom = producer.getLoadedBloomForField(ID_FIELD);
        MongotFuzzySet secondBloom = producer.getLoadedBloomForField(SECOND_FIELD);
        assertThat(idBloom).isNotNull();
        assertThat(secondBloom).isNotNull();

        // Each field's bloom must recognize its own terms (no false negatives) and reject the other
        // field's disjoint term space (proving the slices don't overlap).
        for (int i = 0; i < 2_000; i++) {
          assertThat(idBloom.contains(new BytesRef("id_" + i))).isEqualTo(ContainsResult.MAYBE);
          assertThat(secondBloom.contains(new BytesRef("f2_" + i))).isEqualTo(ContainsResult.MAYBE);
        }
        // The id-space terms are absent from the second field and vice versa. With disjoint spaces
        // a correctly-sliced bloom answers NO for the foreign space on the vast majority of probes;
        // a slice pointed at the wrong field's words would answer MAYBE for (nearly) all of them.
        assertThat(definiteNoCount(idBloom, "f2_", 2_000)).isGreaterThan(1_000);
        assertThat(definiteNoCount(secondBloom, "id_", 2_000)).isGreaterThan(1_000);

        // End-to-end: a foreign-space lookup on each field returns no hits, its own space one hit.
        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "id_0")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "f2_0")))).isEqualTo(0);
        assertThat(searcher.count(new TermQuery(new Term(SECOND_FIELD, "f2_0")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(SECOND_FIELD, "id_0")))).isEqualTo(0);

        // The mapped footprint is the sum of both fields' bitsets, all in page cache (0 on heap).
        assertThat(producer.mappedBloomBytes())
            .isEqualTo(idBloom.bitsetBytes() + secondBloom.bitsetBytes());
        assertThat(producer.mappedBloomBytes()).isGreaterThan(0L);
        assertThat(producer.heapBloomRamBytesUsed()).isEqualTo(0L);
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(dir, false);
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  /**
   * Eviction drops the fuzzy-set references (bloom fast-fail stops), but a probe that already holds
   * a reference must keep reading correctly from the still-open mapped {@code .blm}. The input is
   * only unmapped when the producer closes, which happens when the reader closes.
   */
  @Test
  public void retainedBloomReference_survivesEviction() throws IOException {
    Path tempDir = Files.createTempDirectory("mongot-bloom-input-evict");
    try (Directory dir = new MMapDirectory(tempDir)) {
      writeIdOnly(dir, 1_000);

      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();

        MongotFuzzySet retained = producer.getLoadedBloomForField(ID_FIELD);
        assertThat(retained).isNotNull();
        assertThat(retained.contains(new BytesRef("doc0"))).isEqualTo(ContainsResult.MAYBE);

        producer.evictBloom();

        // Fast-fail is gone from the producer, but the retained reference still reads live bits:
        // the mapped .blm is not closed until close().
        assertThat(producer.hasLoadedBloom()).isFalse();
        assertThat(producer.getLoadedBloomForField(ID_FIELD)).isNull();
        assertThat(retained.contains(new BytesRef("doc0"))).isEqualTo(ContainsResult.MAYBE);

        // Searches remain correct after eviction (now via the delegate postings, no fast-fail).
        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "doc0")))).isEqualTo(1);
        assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "missing")))).isEqualTo(0);
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(dir, false);
      }
      // Reader closed -> producer closed -> mapped .blm unmapped. The retained reference must not
      // be touched past this point; that invariant is enforced by close() ordering (bloom refs
      // dropped before the input is closed), verified structurally by close() completing cleanly.
    } finally {
      deleteRecursively(tempDir);
    }
  }

  /**
   * A segment's bloom bitset is a single {@link org.apache.lucene.store.RandomAccessInput} slice
   * over the mapped {@code .blm}, shared by every query thread that probes it. This verifies that
   * concurrent {@link MongotFuzzySet#contains} calls on that shared slice are safe (no torn reads,
   * no false negatives) and stay correct while an eviction races the readers. It probes the fuzzy
   * set directly rather than through {@link IndexSearcher} to isolate the input read path from
   * Lucene's own search-engine concurrency.
   */
  @Test
  public void concurrentBloomProbes_withEviction_stayCorrect() throws Exception {
    int numTerms = 5_000;
    Path tempDir = Files.createTempDirectory("mongot-bloom-input-concurrent");
    try (Directory dir = new MMapDirectory(tempDir)) {
      writeIdOnly(dir, numTerms);

      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();
        MongotFuzzySet bloom = producer.getLoadedBloomForField(ID_FIELD);
        assertThat(bloom).isNotNull();

        int threads = 8;
        int iterationsPerThread = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
          for (int t = 0; t < threads; t++) {
            pool.submit(
                () -> {
                  try {
                    start.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                      // No false negatives, ever: an inserted term must never come back NO, even
                      // while other threads hammer the same mapped slice.
                      assertThat(bloom.contains(new BytesRef("doc" + (i % numTerms))))
                          .isEqualTo(ContainsResult.MAYBE);
                      // Absent probes must not throw; the answer (NO/MAYBE) is read from the slice.
                      BloomContainsUnchecked.probe(bloom, "absent_" + i);
                    }
                  } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                  }
                });
          }
          start.countDown();
          // Race clear() against the concurrent probes. The retained reference keeps the mapped
          // input open, so in-flight probes stay valid.
          Thread.sleep(2);
          producer.evictBloom();
          pool.shutdown();
          assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        } finally {
          pool.shutdownNow();
        }
        if (failure.get() != null) {
          throw new AssertionError("Concurrent bloom probing failed", failure.get());
        }
        assertThat(producer.hasLoadedBloom()).isFalse();
        // The retained reference still reads correctly after the eviction and the read storm.
        assertThat(bloom.contains(new BytesRef("doc0"))).isEqualTo(ContainsResult.MAYBE);
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(dir, false);
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  /** Probes the bloom purely for its side effect of reading the mapped slice (result discarded). */
  private static final class BloomContainsUnchecked {
    private static void probe(MongotFuzzySet bloom, String value) {
      ContainsResult unused = bloom.contains(new BytesRef(value));
    }
  }

  /** Counts how many {@code prefix + i} probes the bloom reports as definitely absent (NO). */
  private static long definiteNoCount(MongotFuzzySet bloom, String prefix, int count) {
    return java.util.stream.IntStream.range(0, count)
        .filter(i -> bloom.contains(new BytesRef(prefix + i)) == ContainsResult.NO)
        .count();
  }

  private static void writeIdOnly(Directory dir, int numDocs) throws IOException {
    try (IndexWriter writer = new IndexWriter(dir, bloomWriterConfig(List.of(ID_FIELD)))) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc" + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
    }
  }

  private static void writeTwoBloomFields(Directory dir, int numDocs) throws IOException {
    try (IndexWriter writer =
        new IndexWriter(dir, bloomWriterConfig(List.of(ID_FIELD, SECOND_FIELD)))) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "id_" + i, Field.Store.YES));
        doc.add(new StringField(SECOND_FIELD, "f2_" + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
    }
  }

  /**
   * A write-time codec that routes the given fields through {@link
   * MongotBloomFilteringPostingsFormat} (all sharing one instance, so they land in a single {@code
   * .blm}) and everything else through the default Lucene99 postings format. It reports the
   * SPI-registered {@code "Lucene99"} name so segments are readable via the real {@link
   * Lucene99Codec} on {@link DirectoryReader#open} — per-field formats are self-describing in
   * segment metadata, so the read path recovers {@code MongotBloomFilter} for each bloom field
   * without this custom codec.
   */
  private static IndexWriterConfig bloomWriterConfig(List<String> bloomFields) {
    Lucene99Codec base = new Lucene99Codec();
    PostingsFormat delegate = base.getPostingsFormatForField(ID_FIELD);
    PostingsFormat bloomFormat = new MongotBloomFilteringPostingsFormat(delegate);
    PerFieldPostingsFormat perField =
        new PerFieldPostingsFormat() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return bloomFields.contains(field) ? bloomFormat : delegate;
          }
        };
    Codec codec =
        new FilterCodec(base.getName(), base) {
          @Override
          public PostingsFormat postingsFormat() {
            return perField;
          }
        };
    return new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  throw new java.io.UncheckedIOException(e);
                }
              });
    }
  }
}
