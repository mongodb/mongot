package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/**
 * Verifies {@link LuceneSearcherManager} bloom heap eviction on initial-sync → steady-state
 * transition: only the {@link DirectoryReader} is reopened; the {@link IndexWriter} and {@link
 * Directory} stay open.
 */
public class LuceneSearcherManagerBloomEvictionTest {

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();

  @Test
  public void initialSyncToSteadyState_reopensReaderWithoutClosingWriter() throws IOException {
    try (Harness harness = newHarness()) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync =
          (DirectoryReader) searcherDuringSync.getIndexReader();
      assertThat(heapLoadedBloomForIdField(readerDuringSync)).isPresent();

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
      assertThat(readerSteady).isNotSameInstanceAs(readerDuringSync);
      assertThat(heapLoadedBloomForIdField(readerSteady)).isEmpty();

      harness.manager.release(searcherDuringSync);
      harness.manager.release(searcherSteady);

      assertThat(harness.writer.isOpen()).isTrue();
      Document doc2 = new Document();
      doc2.add(new StringField(ID_FIELD, "doc2", Field.Store.YES));
      harness.writer.addDocument(doc2);
      harness.writer.commit();
    }
  }

  /**
   * Ensures {@link LuceneSearcherManager} releases the pre-refresh reader on bloom eviction so
   * segment/postings resources are not leaked when the index partition stays open.
   */
  @Test
  public void bloomEviction_refresh_releasesOldDirectoryReader() throws IOException {
    try (Harness harness = newHarness()) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync =
          (DirectoryReader) searcherDuringSync.getIndexReader();
      harness.manager.release(searcherDuringSync);

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      assertThat(readerDuringSync.tryIncRef()).isFalse();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      try {
        assertThat(searcherSteady.getIndexReader()).isNotSameInstanceAs(readerDuringSync);
      } finally {
        harness.manager.release(searcherSteady);
      }
    }
  }

  @Test
  public void bloomEviction_refresh_releasesOldReaderAfterLastSearcherRelease() throws IOException {
    try (Harness harness = newHarness()) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync =
          (DirectoryReader) searcherDuringSync.getIndexReader();

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      harness.manager.release(searcherSteady);
      harness.manager.release(searcherDuringSync);

      assertThat(readerDuringSync.tryIncRef()).isFalse();
    }
  }

  /**
   * Step-by-step {@link DirectoryReader#getRefCount()} checks for bloom eviction via {@code
   * DirectoryReader.open(IndexCommit)}: the new reader's ref from {@code open()} is owned by
   * {@link LuceneSearcherManager} after swap; the old reader drops to zero only after the last
   * {@link LuceneSearcherManager#release}.
   */
  @Test
  public void bloomEviction_refresh_tracksDirectoryReaderRefCounts() throws IOException {
    try (Harness harness = newHarness()) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync =
          (DirectoryReader) searcherDuringSync.getIndexReader();
      assertThat(readerDuringSync.getRefCount()).isEqualTo(2);

      harness.manager.release(searcherDuringSync);
      assertThat(readerDuringSync.getRefCount()).isEqualTo(1);

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      assertThat(readerDuringSync.getRefCount()).isEqualTo(0);
      assertThat(readerDuringSync.tryIncRef()).isFalse();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
      assertThat(readerSteady.getRefCount()).isEqualTo(2);
      harness.manager.release(searcherSteady);
      assertThat(readerSteady.getRefCount()).isEqualTo(1);
    }

    try (Harness harness = newHarness()) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync =
          (DirectoryReader) searcherDuringSync.getIndexReader();
      assertThat(readerDuringSync.getRefCount()).isEqualTo(2);

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();
      assertThat(readerDuringSync.getRefCount()).isEqualTo(1);

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
      assertThat(readerSteady.getRefCount()).isEqualTo(2);

      harness.manager.release(searcherSteady);
      assertThat(readerSteady.getRefCount()).isEqualTo(1);

      harness.manager.release(searcherDuringSync);
      assertThat(readerDuringSync.getRefCount()).isEqualTo(0);
      assertThat(readerDuringSync.tryIncRef()).isFalse();
    }
  }

  private static Harness newHarness() throws IOException {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD))
        .thenReturn(true);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_IN_STEADY_STATE))
        .thenReturn(false);
    AtomicReference<IndexStatus> indexStatus = new AtomicReference<>(IndexStatus.initialSync());
    SearchIndexDefinition indexDefinition = getIndexDefinition();

    Directory directory = new ByteBuffersDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(
                LuceneCodec.Factory.forIndexWithBloomFilter(
                    Map.of(), () -> true, Optional.empty()))
            .setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(directory, config);
    Document doc = new Document();
    doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
    writer.addDocument(doc);
    writer.commit();

    BooleanSupplier useIdBloomFilter =
        BloomCodecPolicy.getBloomFilterEnabledForIdField(
            registry, true, indexDefinition, indexStatus::get);
    LuceneSearcherManager manager =
        LuceneSearcherManager.create(
            writer,
            new LuceneSearcherFactory(
                indexDefinition,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
            SearchIndex.mockMetricsFactory(),
            useIdBloomFilter);

    return new Harness(directory, writer, manager, indexStatus);
  }

  private static final class Harness implements AutoCloseable {
    final Directory directory;
    final IndexWriter writer;
    final LuceneSearcherManager manager;
    final AtomicReference<IndexStatus> indexStatus;

    Harness(
        Directory directory,
        IndexWriter writer,
        LuceneSearcherManager manager,
        AtomicReference<IndexStatus> indexStatus) {
      this.directory = directory;
      this.writer = writer;
      this.manager = manager;
      this.indexStatus = indexStatus;
    }

    @Override
    public void close() throws IOException {
      this.manager.close();
      this.writer.close();
      this.directory.close();
    }
  }

  private static Optional<FuzzySet> heapLoadedBloomForIdField(DirectoryReader reader) {
    SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
    return unwrapBloomFieldsProducer(segment.getPostingsReader(), ID_FIELD)
        .map(producer -> producer.getHeapLoadedBloomForField(ID_FIELD));
  }

  private static Optional<MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer>
      unwrapBloomFieldsProducer(FieldsProducer producer, String field) {
    if (producer
        instanceof MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer
            bloomProducer) {
      return Optional.of(bloomProducer);
    }
    try {
      java.lang.reflect.Field fieldsField = producer.getClass().getDeclaredField("fields");
      fieldsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, FieldsProducer> fields = (Map<String, FieldsProducer>) fieldsField.get(producer);
      FieldsProducer fieldProducer = fields.get(field);
      if (fieldProducer
          instanceof MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer
              bloomProducer) {
        return Optional.of(bloomProducer);
      }
    } catch (ReflectiveOperationException e) {
      // Not a PerFieldPostingsFormat.FieldsReader, or API mismatch.
    }
    return Optional.empty();
  }

  private static SearchIndexDefinition getIndexDefinition() {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }
}
