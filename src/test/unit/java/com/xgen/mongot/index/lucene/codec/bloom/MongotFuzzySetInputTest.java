package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.codec.bloom.MongotFuzzySet.ContainsResult;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the input-backed read path of {@link MongotFuzzySet}: bloom sets built on the write path
 * are read back straight from a {@link RandomAccessInput} over the serialized {@code .blm} words,
 * without copying the bitset onto the JVM heap.
 */
public class MongotFuzzySetInputTest {

  // These tests assert input-read-path behavior (zero heap bytes, slice lifetime, shared-slice
  // concurrency), so they pin the storage mode rather than inheriting whatever
  // Feature.BLOOM_FILTER_ON_HEAP resolves to.
  @Before
  public void forceOffHeapBloom() {
    BloomStoragePolicy.setStorageModeForTest(MongotFuzzySet.StorageMode.INPUT);
  }

  @After
  public void restoreBloomReadPathStorageMode() {
    BloomStoragePolicy.setStorageModeForTest(null);
  }

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();

  @Test
  public void inputBackedSet_matchesHeapSet_andReportsZeroHeap() throws IOException {
    int numTerms = 5_000;

    // Build the set exactly as the write path does, then serialize the (possibly downsized) form
    // that ends up in a .blm.
    MongotFuzzySet built = MongotFuzzySet.createOptimalSet(numTerms, 0.1023f);
    for (int i = 0; i < numTerms; i++) {
      built.addValue(term(i));
    }
    MongotFuzzySet downsized = built.downsize(built.getTargetMaxSaturation());
    MongotFuzzySet heapSet = downsized != null ? downsized : built;

    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexOutput out = dir.createOutput("bloom", IOContext.DEFAULT)) {
        heapSet.serialize(out);
      }

      try (IndexInput in = dir.openInput("bloom", IOContext.DEFAULT)) {
        int hashCount = in.readVInt();
        int bloomSize = in.readInt();
        int numWords = in.readInt();
        long wordsOffset = in.getFilePointer();
        RandomAccessInput words = in.randomAccessSlice(wordsOffset, (long) numWords * Long.BYTES);
        MongotFuzzySet inputSet = MongotFuzzySet.fromInput(words, hashCount, bloomSize, numWords);

        // No false negatives: every recorded term must be reported as MAYBE.
        for (int i = 0; i < numTerms; i++) {
          assertThat(inputSet.contains(term(i))).isEqualTo(ContainsResult.MAYBE);
        }

        // The input-backed view must be byte-for-byte equivalent to the heap set it was
        // serialized from, including for terms never added (exercising definitive-NO answers).
        for (int i = 0; i < 20_000; i++) {
          BytesRef probe = term(1_000_000 + i);
          assertThat(inputSet.contains(probe)).isEqualTo(heapSet.contains(probe));
        }

        // The bitset words are read through the input, not copied onto the heap.
        assertThat(inputSet.ramBytesUsed()).isEqualTo(0L);
      }
    }
  }

  @Test
  public void directoryReader_readsBloomFromInput_withZeroHeapBytes() throws IOException {
    Path tempDir = Files.createTempDirectory("mongot-bloom-input-test");
    try (Directory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  LuceneCodec.Factory.forIndexWithBloomFilter(
                      Map.of(), () -> true, Optional.empty()))
              .setUseCompoundFile(false);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < 1_000; i++) {
          Document doc = new Document();
          doc.add(new StringField(ID_FIELD, "doc" + i, Field.Store.YES));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      MongotBloomReadPolicy.setLoadBloomOnRead(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        MongotBloomFilteredFieldsProducer producer =
            MongotBloomFieldsProducerRegistry.lookup(segment, ID_FIELD).orElseThrow();

        // Bloom fast-fail is wired up, but nothing is on the JVM heap: bits stay in the file.
        assertThat(producer.hasLoadedBloom()).isTrue();
        assertThat(producer.heapBloomRamBytesUsed()).isEqualTo(0L);

        // Present and absent id lookups still resolve correctly through the input-backed filter.
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

  private static BytesRef term(int i) {
    return new BytesRef("term_" + i);
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
