package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.FeatureFlags;
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
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies {@link LuceneSearcherManager} bloom heap eviction on initial-sync → steady-state
 * transition: only the {@link DirectoryReader} is reopened; the {@link IndexWriter} and {@link
 * Directory} stay open.
 */
public class LuceneSearcherManagerBloomEvictionTest {

  // Eviction is asserted against the input read path, so pin the storage mode rather than
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
  private static final boolean BLOOM_DISABLED_IN_STEADY_STATE = false;
  private static final boolean BLOOM_ENABLED_IN_STEADY_STATE = true;

  @Test
  public void initialSyncToSteadyState_onlyForcesReopenWhenPolicyChanges() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();

      harness.manager.maybeRefreshBlocking();
      LuceneIndexSearcher searcherAfterSyncRefresh = harness.manager.acquire();
      try {
        assertThat(searcherAfterSyncRefresh).isSameInstanceAs(searcherDuringSync);
      } finally {
        harness.manager.release(searcherAfterSyncRefresh);
      }

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();
      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
      assertThat(readerSteady).isNotSameInstanceAs(readerDuringSync);

      harness.manager.maybeRefreshBlocking();
      LuceneIndexSearcher searcherAfterSteadyRefresh = harness.manager.acquire();
      try {
        assertThat(searcherAfterSteadyRefresh).isSameInstanceAs(searcherSteady);
      } finally {
        harness.manager.release(searcherAfterSteadyRefresh);
      }

      harness.manager.release(searcherDuringSync);
      harness.manager.release(searcherSteady);
    }
  }

  @Test
  public void withBloomInSteadyStateEnabled_noForceReopenOnStatusTransition() throws IOException {
    try (Harness harness = newHarness(BLOOM_ENABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
      assertThat(heapLoadedBloomForIdField(readerDuringSync)).isPresent();

      harness.manager.maybeRefreshBlocking();
      LuceneIndexSearcher searcherAfterSyncRefresh = harness.manager.acquire();
      try {
        assertThat(searcherAfterSyncRefresh).isSameInstanceAs(searcherDuringSync);
      } finally {
        harness.manager.release(searcherAfterSyncRefresh);
      }

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();
      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
      assertThat(readerSteady).isSameInstanceAs(readerDuringSync);
      assertThat(heapLoadedBloomForIdField(readerSteady)).isPresent();

      harness.manager.release(searcherDuringSync);
      harness.manager.release(searcherSteady);
    }
  }

  @Test
  public void initialSyncToSteadyState_reopensReaderWithoutClosingWriter() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
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
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
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
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      harness.manager.release(searcherSteady);
      harness.manager.release(searcherDuringSync);

      assertThat(readerDuringSync.tryIncRef()).isFalse();
    }
  }

  /**
   * After the writer commits past the reader's commit point, the old segments_N file may be deleted
   * while the stale reader still serves in-memory segments. Bloom eviction must reopen from the
   * writer's latest commit, not the reader's stale commit.
   */
  @Test
  public void bloomEviction_afterWriterCommit_reopensFromLatestCommitOnDisk() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
      assertThat(heapLoadedBloomForIdField(readerDuringSync)).isPresent();
      harness.manager.release(searcherDuringSync);

      Document doc2 = new Document();
      doc2.add(new StringField(ID_FIELD, "doc2", Field.Store.YES));
      harness.writer.addDocument(doc2);
      harness.writer.commit();

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      LuceneIndexSearcher searcherSteady = harness.manager.acquire();
      try {
        DirectoryReader readerSteady = (DirectoryReader) searcherSteady.getIndexReader();
        assertThat(heapLoadedBloomForIdField(readerSteady)).isEmpty();
        assertThat(readerSteady.numDocs()).isEqualTo(2);
      } finally {
        harness.manager.release(searcherSteady);
      }
    }
  }

  /**
   * Bloom eviction must reopen via {@link DirectoryReader#open(IndexWriter)} so subsequent
   * refreshes still expose uncommitted writer changes.
   */
  @Test
  public void bloomEviction_afterSteadyTransition_stillExposesUncommittedChanges()
      throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      assertThat(heapLoadedBloomForIdField((DirectoryReader) searcherDuringSync.getIndexReader()))
          .isPresent();
      harness.manager.release(searcherDuringSync);

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      Document doc2 = new Document();
      doc2.add(new StringField(ID_FIELD, "doc2", Field.Store.YES));
      harness.writer.addDocument(doc2);

      harness.manager.maybeRefreshBlocking();

      LuceneIndexSearcher searcherAfterUncommittedRefresh = harness.manager.acquire();
      try {
        DirectoryReader reader = (DirectoryReader) searcherAfterUncommittedRefresh.getIndexReader();
        assertThat(heapLoadedBloomForIdField(reader)).isEmpty();
        assertThat(reader.numDocs()).isEqualTo(2);
      } finally {
        harness.manager.release(searcherAfterUncommittedRefresh);
      }
    }
  }

  /**
   * Step-by-step {@link DirectoryReader#getRefCount()} checks for bloom eviction via {@code
   * DirectoryReader.open(IndexCommit)}: the new reader's ref from {@code open()} is owned by {@link
   * LuceneSearcherManager} after swap; the old reader drops to zero only after the last {@link
   * LuceneSearcherManager#release}.
   */
  @Test
  public void bloomEviction_refresh_tracksDirectoryReaderRefCounts() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
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

    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      LuceneIndexSearcher searcherDuringSync = harness.manager.acquire();
      DirectoryReader readerDuringSync = (DirectoryReader) searcherDuringSync.getIndexReader();
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

  @Test
  public void initialSync_reportsBloomHeapGauges() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      // Bloom fast-fail is wired up during initial sync, so the loaded-segments gauge reflects the
      // live segment. The heap-bytes gauge is zero: bloom bitsets are read through the input
      // (OS page cache), not on the JVM heap. The mapped-bytes gauge carries the real footprint.
      assertThat(gauge(harness.metrics, LuceneSearcherManager.BLOOM_HEAP_LOADED_SEGMENTS_METRIC))
          .isAtLeast(1.0);
      assertThat(gauge(harness.metrics, LuceneSearcherManager.BLOOM_HEAP_BYTES_METRIC))
          .isEqualTo(0.0);
      assertThat(gauge(harness.metrics, LuceneSearcherManager.BLOOM_MAPPED_BYTES_METRIC))
          .isGreaterThan(0.0);
    }
  }

  /**
   * Production regression: during INITIAL_SYNC the periodic refresher does not refresh the searcher
   * (it gates on steady state), so the manager's {@code current} reader stays the empty reader
   * opened at construction and never opens the bloom segments written during the sync. A reader
   * walk would therefore report zero. The process-wide registry stats that back the heap gauges
   * must instead reflect bloom held on heap by <em>any</em> reader (here a reader opened separately
   * from the searcher manager), and must drop back to zero once that reader closes.
   */
  @Test
  public void bloomStats_reflectResidentBloomFromAnyReader() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  LuceneCodec.Factory.forIndexWithBloomFilter(
                      Map.of(), () -> true, Optional.empty(), FeatureFlags.getDefault()))
              .setUseCompoundFile(true);
      IndexWriter writer = new IndexWriter(directory, config);
      try {
        // No reader has opened bloom segments yet.
        assertThat(MongotBloomFieldsProducerRegistry.bloomStats().loadedSegments()).isEqualTo(0);

        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();

        // Initial-sync policy is active; a reader other than any searcher manager opens the segment
        // and loads bloom on heap, registering it in the process-wide registry.
        MongotBloomReadPolicy.setLoadBloomOnRead(writer.getDirectory(), true);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          MongotBloomEvictor.BloomStats stats = MongotBloomFieldsProducerRegistry.bloomStats();
          assertThat(stats.loadedSegments()).isAtLeast(1);
          // Bloom bitsets are read through the input: nothing on the JVM heap; the
          // footprint is in page cache.
          assertThat(stats.heapBytes()).isEqualTo(0L);
          assertThat(stats.mappedBytes()).isGreaterThan(0L);
        }
      } finally {
        // Closing the writer closes its pooled segment readers, unregistering their producers.
        writer.close();
        MongotBloomReadPolicy.setLoadBloomOnRead(directory, false);
      }

      // With every reader (including the writer's pool) closed, the registry reports zero again.
      assertThat(MongotBloomFieldsProducerRegistry.bloomStats().loadedSegments()).isEqualTo(0);
    }
  }

  @Test
  public void steadyStateTransition_recordsHeapEvictions() throws IOException {
    try (Harness harness = newHarness(BLOOM_DISABLED_IN_STEADY_STATE)) {
      assertThat(counter(harness.metrics, LuceneSearcherManager.BLOOM_HEAP_EVICTIONS_METRIC))
          .isEqualTo(0.0);

      harness.indexStatus.set(IndexStatus.steady());
      harness.manager.maybeRefreshBlocking();

      assertThat(counter(harness.metrics, LuceneSearcherManager.BLOOM_HEAP_EVICTIONS_METRIC))
          .isAtLeast(1.0);
    }
  }

  /**
   * Validates the directory-identity contract that the bloom heap path (and the {@code
   * bloomFilterHeapBytes}/{@code bloomFilterHeapLoadedSegments} gauges) depend on, against a
   * production-style {@link FilterDirectory}-wrapped index.
   *
   * <p>{@link LuceneSearcherManager} registers the read policy with {@code writer.getDirectory()}
   * (see {@code refreshIfNeeded}); the codec resolves it from {@code SegmentInfo#dir} (see {@code
   * MongotBloomFilteringPostingsFormat#fieldsProducer}). If those two handles did not resolve to
   * the same storage directory after unwrapping, the heap-loading producer would never be
   * constructed and both heap gauges would read zero even during initial sync. This test fails if
   * that bug is present and passes (gauges non-zero) if the contract holds.
   */
  @Test
  public void wrappedDirectory_policyRegisteredViaWriterDirectory_engagesHeapBloomOnRead()
      throws IOException {
    try (Directory base = new ByteBuffersDirectory();
        Directory wrapped = new FilterDirectory(base) {}) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  LuceneCodec.Factory.forIndexWithBloomFilter(
                      Map.of(), () -> true, Optional.empty(), FeatureFlags.getDefault()))
              .setUseCompoundFile(true);
      try (IndexWriter writer = new IndexWriter(wrapped, config)) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();

        // Initial-sync registration uses the writer's directory handle.
        MongotBloomReadPolicy.setLoadBloomOnRead(writer.getDirectory(), true);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();

          // Core invariant: the read-side handle resolves to the same registration.
          assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(segment.getSegmentInfo().info.dir))
              .isTrue();

          MongotBloomEvictor.BloomStats stats = MongotBloomEvictor.bloomStats(reader);
          assertThat(stats.loadedSegments()).isAtLeast(1);
          // Bloom bitsets are read through the input: nothing on the JVM heap; the
          // footprint is in page cache.
          assertThat(stats.heapBytes()).isEqualTo(0L);
          assertThat(stats.mappedBytes()).isGreaterThan(0L);
        }
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(base, false);
      }
    }
  }

  /**
   * Negative control documenting the <em>expected</em> zero case: with the steady-state policy (no
   * heap loading), the same wrapped index reports zero heap bloom. This distinguishes "designed
   * zero in steady state" from "always zero due to a directory-identity bug" (the test above).
   */
  @Test
  public void wrappedDirectory_steadyStatePolicy_reportsZeroHeapBloom() throws IOException {
    try (Directory base = new ByteBuffersDirectory();
        Directory wrapped = new FilterDirectory(base) {}) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  LuceneCodec.Factory.forIndexWithBloomFilter(
                      Map.of(), () -> true, Optional.empty(), FeatureFlags.getDefault()))
              .setUseCompoundFile(true);
      try (IndexWriter writer = new IndexWriter(wrapped, config)) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();

        // Steady state: policy not enabled, so segments open via the delegate without heap bloom.
        MongotBloomReadPolicy.setLoadBloomOnRead(writer.getDirectory(), false);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
          assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(segment.getSegmentInfo().info.dir))
              .isFalse();

          MongotBloomEvictor.BloomStats stats = MongotBloomEvictor.bloomStats(reader);
          assertThat(stats.loadedSegments()).isEqualTo(0);
          assertThat(stats.heapBytes()).isEqualTo(0L);
          assertThat(stats.mappedBytes()).isEqualTo(0L);
        }
      }
    }
  }

  private static double gauge(PerIndexMetricsFactory metrics, String name) {
    return metrics.get(name).gauge().value();
  }

  private static double counter(PerIndexMetricsFactory metrics, String name) {
    return metrics.get(name).counter().count();
  }

  private static Harness newHarness(boolean bloomFilterInSteadyState) throws IOException {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD))
        .thenReturn(true);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_IN_STEADY_STATE))
        .thenReturn(bloomFilterInSteadyState);
    AtomicReference<IndexStatus> indexStatus = new AtomicReference<>(IndexStatus.initialSync());
    SearchIndexDefinition indexDefinition = getIndexDefinition();

    Directory directory = new ByteBuffersDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(
                LuceneCodec.Factory.forIndexWithBloomFilter(
                    Map.of(), () -> true, Optional.empty(), FeatureFlags.getDefault()))
            .setUseCompoundFile(true);
    IndexWriter writer = new IndexWriter(directory, config);
    Document doc = new Document();
    doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
    writer.addDocument(doc);
    writer.commit();

    BooleanSupplier useIdBloomFilter =
        BloomCodecPolicy.getBloomFilterEnabledForIdField(
            registry, true, indexDefinition, indexStatus::get);
    PerIndexMetricsFactory metrics = SearchIndex.mockMetricsFactory();
    LuceneSearcherManager manager =
        LuceneSearcherManager.create(
            writer,
            new LuceneSearcherFactory(
                indexDefinition,
                BLOOM_DISABLED_IN_STEADY_STATE,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
            metrics,
            useIdBloomFilter);

    return new Harness(directory, writer, manager, indexStatus, metrics);
  }

  private static final class Harness implements AutoCloseable {
    final Directory directory;
    final IndexWriter writer;
    final LuceneSearcherManager manager;
    final AtomicReference<IndexStatus> indexStatus;
    final PerIndexMetricsFactory metrics;

    Harness(
        Directory directory,
        IndexWriter writer,
        LuceneSearcherManager manager,
        AtomicReference<IndexStatus> indexStatus,
        PerIndexMetricsFactory metrics) {
      this.directory = directory;
      this.writer = writer;
      this.manager = manager;
      this.indexStatus = indexStatus;
      this.metrics = metrics;
    }

    @Override
    public void close() throws IOException {
      this.manager.close();
      this.writer.close();
      this.directory.close();
    }
  }

  private static Optional<MongotFuzzySet> heapLoadedBloomForIdField(DirectoryReader reader) {
    SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
    return unwrapBloomFieldsProducer(segment, ID_FIELD)
        .map(producer -> producer.getLoadedBloomForField(ID_FIELD));
  }

  private static Optional<MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer>
      unwrapBloomFieldsProducer(SegmentReader segment, String field) {
    FieldsProducer producer = segment.getPostingsReader();
    if (producer
        instanceof
        MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer bloomProducer) {
      return Optional.of(bloomProducer);
    }
    return MongotBloomFieldsProducerRegistry.lookup(segment, field);
  }

  private static SearchIndexDefinition getIndexDefinition() {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }
}
