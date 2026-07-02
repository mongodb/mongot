package com.xgen.mongot.index.lucene.searcher;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFieldsProducerRegistry;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomHeapEvictor;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomReadPolicy;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import io.micrometer.core.instrument.Counter;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to safely share {@link LuceneIndexSearcher} instances across multiple threads,
 * while periodically reopening. This class ensures each searcher is closed only once all threads
 * have finished using it. This class does the same as {@link SearcherManager}, but generalized with
 * {@link LuceneIndexSearcher} and accepts {@link LuceneSearcherFactory} in the constructor.
 */
public class LuceneSearcherManager extends ReferenceManager<LuceneIndexSearcher> {

  @VisibleForTesting static final String SEGMENT_COUNT_METRIC = "segment_count";

  @VisibleForTesting public static final String BLOOM_HEAP_BYTES_METRIC = "bloomFilterHeapBytes";

  @VisibleForTesting
  public static final String BLOOM_HEAP_LOADED_SEGMENTS_METRIC = "bloomFilterHeapLoadedSegments";

  @VisibleForTesting
  public static final String BLOOM_HEAP_EVICTIONS_METRIC = "bloomFilterHeapEvictions";

  private static final Logger LOG = LoggerFactory.getLogger(LuceneSearcherManager.class);

  private final LuceneSearcherFactory factory;
  private final IndexWriter indexWriter;
  private final BooleanSupplier useIdBloomFilter;

  /**
   * Counts segments whose heap-resident bloom bitsets were evicted at the initial-sync → steady
   * state transition.
   */
  private final Counter bloomHeapEvictionCounter;

  /**
   * Tracks whether bloom bitsets were loaded on the last reader open. Used to detect the
   * initial-sync → steady-state transition so bloom can be proactively evicted from heap.
   *
   * <p>Accessed only inside {@link #refreshIfNeeded}, which {@link ReferenceManager} calls under
   * its refresh lock, so no additional synchronization is needed.
   */
  private volatile boolean lastBloomLoadState;

  /**
   * Constructs the manager and its bloom eviction counter, but does not register gauges: those hold
   * a reference to the fully-constructed instance, so {@link #create} registers them after this
   * returns. Protected to allow test subclasses.
   */
  protected LuceneSearcherManager(
      IndexWriter writer,
      LuceneSearcherFactory factory,
      BooleanSupplier useIdBloomFilter,
      PerIndexMetricsFactory metrics)
      throws IOException {
    this.factory = factory;
    this.indexWriter = writer;
    this.useIdBloomFilter = useIdBloomFilter;
    this.bloomHeapEvictionCounter =
        metrics.getProcessMetricsFactory().counter(BLOOM_HEAP_EVICTIONS_METRIC);

    boolean initialBloomLoad = useIdBloomFilter.getAsBoolean();
    this.lastBloomLoadState = initialBloomLoad;
    MongotBloomReadPolicy.setLoadBloomOnHeap(writer.getDirectory(), initialBloomLoad);

    this.current = getSearcher(DirectoryReader.open(writer));
  }

  /** Static factory method that constructs the manager and registers all gauges. */
  public static LuceneSearcherManager create(
      IndexWriter writer,
      LuceneSearcherFactory factory,
      PerIndexMetricsFactory metrics,
      BooleanSupplier useIdBloomFilter)
      throws IOException {

    LuceneSearcherManager manager =
        new LuceneSearcherManager(writer, factory, useIdBloomFilter, metrics);

    // Register gauges after construction is complete
    metrics.perIndexObjectValueGauge(
        SEGMENT_COUNT_METRIC,
        manager,
        CachedGauge.of(LuceneSearcherManager::getSegmentCount, Duration.ofMinutes(1)));

    // Bloom heap footprint changes on every refresh (segments open/merge, policy flips), and
    // summing FuzzySet.ramBytesUsed walks every registered producer. Cache one stats snapshot
    // shared by both gauges so the walk runs at most once a minute, not once per gauge.
    var cachedBloomHeapStats =
        Suppliers.memoizeWithExpiration(
            MongotBloomFieldsProducerRegistry::bloomHeapStats, Duration.ofMinutes(1));
    metrics
        .getProcessMetricsFactory()
        .objectValueGauge(
            BLOOM_HEAP_BYTES_METRIC, cachedBloomHeapStats, s -> (double) s.get().heapBytes());
    metrics
        .getProcessMetricsFactory()
        .objectValueGauge(
            BLOOM_HEAP_LOADED_SEGMENTS_METRIC,
            cachedBloomHeapStats,
            s -> (double) s.get().loadedSegments());

    return manager;
  }

  private double getSegmentCount() {
    try {
      return ((DirectoryReader) this.current.getIndexReader()).getIndexCommit().getSegmentCount();
    } catch (IOException e) {
      LOG.warn("Unable to get current segment count metric", e);
      return Double.NaN;
    }
  }

  @Nullable
  @Override
  protected LuceneIndexSearcher refreshIfNeeded(LuceneIndexSearcher referenceToRefresh)
      throws IOException {
    var oldReader = referenceToRefresh.getIndexReader();
    checkArg(oldReader instanceof DirectoryReader, "IndexReader should be a DirectoryReader");
    var oldDirectoryReader = (DirectoryReader) oldReader;

    boolean loadBloomOnHeap = this.useIdBloomFilter.getAsBoolean();
    MongotBloomReadPolicy.setLoadBloomOnHeap(this.indexWriter.getDirectory(), loadBloomOnHeap);
    boolean evictBloom = this.lastBloomLoadState && !loadBloomOnHeap;
    this.lastBloomLoadState = loadBloomOnHeap;
    @Var DirectoryReader newReader;
    if (evictBloom) {
      // Bloom eviction reopens only the DirectoryReader, not the Directory or IndexWriter.
      // The writer and on-disk index stay open for the life of the index partition.
      //
      // openIfChanged and DirectoryReader.open(IndexWriter) can reuse unchanged SegmentReaders
      // from a prior reader; those readers still hold heap-resident bloom bitsets opened under the
      // initial-sync policy. Clear heap bloom on the live segment readers, then reopen from the
      // writer so near-real-time refresh is preserved and we do not consult a stale IndexCommit
      // (which may reference a deleted segments_N file after the writer commits).
      //
      // ReferenceManager swaps to the returned searcher and release()s the previous one, which
      // decRefs and closes the old DirectoryReader. That closes bloom FieldsProducers and clears
      // their in-heap bitsets (see MongotBloomFilteredFieldsProducer#close).
      int evicted = MongotBloomHeapEvictor.evictHeapBloomFromReader(oldDirectoryReader);
      if (evicted > 0) {
        this.bloomHeapEvictionCounter.increment(evicted);
      }
      newReader = DirectoryReader.openIfChanged(oldDirectoryReader, this.indexWriter);
      if (newReader == null) {
        newReader = DirectoryReader.open(this.indexWriter);
      }
    } else {
      newReader = DirectoryReader.openIfChanged(oldDirectoryReader);
      if (newReader == null) {
        return null;
      }
    }
    return getSearcher(newReader);
  }

  @Override
  protected void decRef(LuceneIndexSearcher reference) throws IOException {
    reference.getIndexReader().decRef();
  }

  @Override
  protected boolean tryIncRef(LuceneIndexSearcher reference) {
    return reference.getIndexReader().tryIncRef();
  }

  @Override
  protected int getRefCount(LuceneIndexSearcher reference) {
    return reference.getIndexReader().getRefCount();
  }

  private LuceneIndexSearcher getSearcher(IndexReader reader) throws IOException {
    @Var var success = false;
    LuceneIndexSearcher searcher;
    try {
      searcher = this.factory.newSearcher(reader, Optional.ofNullable(this.current));
      if (searcher.getIndexReader() != reader) {
        throw new IllegalStateException(
            "SearcherFactory must wrap exactly the provided reader (got "
                + searcher.getIndexReader()
                + " but expected "
                + reader
                + ")");
      }
      success = true;
    } finally {
      if (!success) {
        reader.decRef();
      }
    }
    return searcher;
  }
}
