package com.xgen.mongot.index.lucene.searcher;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomEvictor;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFieldsProducerRegistry;
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

  /**
   * JVM heap bytes held by loaded {@code _id} bloom bitsets. Non-zero when {@link
   * com.xgen.mongot.featureflag.Feature#BLOOM_FILTER_ON_HEAP} is enabled, where each segment's
   * bitset is copied onto the heap at open; {@code 0} by default, where {@link
   * #BLOOM_MAPPED_BYTES_METRIC} carries the footprint instead.
   */
  @VisibleForTesting public static final String BLOOM_HEAP_BYTES_METRIC = "bloomFilterHeapBytes";

  /**
   * Page-cache footprint (bytes) of the {@code _id} bloom bitsets read via the memory-mapped {@code
   * .blm}, and the gauge the memory-pressure alert reads. The counterpart to {@link
   * #BLOOM_HEAP_BYTES_METRIC}: exactly one of the two is non-zero, depending on {@link
   * com.xgen.mongot.featureflag.Feature#BLOOM_FILTER_ON_HEAP}.
   */
  @VisibleForTesting
  public static final String BLOOM_MAPPED_BYTES_METRIC = "bloomFilterMappedBytes";

  @VisibleForTesting
  public static final String BLOOM_HEAP_LOADED_SEGMENTS_METRIC = "bloomFilterHeapLoadedSegments";

  @VisibleForTesting
  public static final String BLOOM_HEAP_EVICTIONS_METRIC = "bloomFilterHeapEvictions";

  private static final Logger LOG = LoggerFactory.getLogger(LuceneSearcherManager.class);

  private final LuceneSearcherFactory factory;
  private final IndexWriter indexWriter;
  private final BooleanSupplier useIdBloomFilter;

  /**
   * Counts segments whose loaded bloom sets were evicted at the initial-sync → steady state
   * transition. Eviction drops the fast-fail references; the {@code .blm} stays mapped.
   */
  private final Counter bloomEvictionCounter;

  /**
   * Tracks whether bloom bitsets were loaded on the last reader open. Used to detect the
   * initial-sync → steady-state transition so the fast-fail sets can be proactively dropped from
   * live readers, which stops bloom probing (see {@link MongotBloomEvictor}).
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
    this.bloomEvictionCounter =
        metrics.getProcessMetricsFactory().counter(BLOOM_HEAP_EVICTIONS_METRIC);

    boolean initialBloomLoad = useIdBloomFilter.getAsBoolean();
    this.lastBloomLoadState = initialBloomLoad;
    MongotBloomReadPolicy.setLoadBloomOnRead(writer.getDirectory(), initialBloomLoad);

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

    // The bloom footprint snapshot (mapped page-cache bytes, loaded segment count, heap bytes)
    // changes on every refresh (segments open/merge, policy flips), and computing it walks every
    // registered producer. Cache one snapshot shared by all three gauges so the walk runs at most
    // once a minute, not once per gauge.
    var cachedBloomStats =
        Suppliers.memoizeWithExpiration(
            MongotBloomFieldsProducerRegistry::bloomStats, Duration.ofMinutes(1));
    metrics
        .getProcessMetricsFactory()
        .objectValueGauge(
            BLOOM_HEAP_BYTES_METRIC, cachedBloomStats, s -> (double) s.get().heapBytes());
    metrics
        .getProcessMetricsFactory()
        .objectValueGauge(
            BLOOM_MAPPED_BYTES_METRIC, cachedBloomStats, s -> (double) s.get().mappedBytes());
    metrics
        .getProcessMetricsFactory()
        .objectValueGauge(
            BLOOM_HEAP_LOADED_SEGMENTS_METRIC,
            cachedBloomStats,
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

    boolean loadBloomOnRead = this.useIdBloomFilter.getAsBoolean();
    MongotBloomReadPolicy.setLoadBloomOnRead(this.indexWriter.getDirectory(), loadBloomOnRead);
    boolean evictBloom = this.lastBloomLoadState && !loadBloomOnRead;
    this.lastBloomLoadState = loadBloomOnRead;
    @Var DirectoryReader newReader;
    if (evictBloom) {
      // Bloom eviction reopens only the DirectoryReader, not the Directory or IndexWriter.
      // The writer and on-disk index stay open for the life of the index partition.
      //
      // openIfChanged and DirectoryReader.open(IndexWriter) can reuse unchanged SegmentReaders
      // from a prior reader; those readers still have bloom sets wired up under the initial-sync
      // policy. Drop the bloom sets on the live segment readers, then reopen from the writer so
      // near-real-time refresh is preserved and we do not consult a stale IndexCommit (which may
      // reference a deleted segments_N file after the writer commits).
      //
      // ReferenceManager swaps to the returned searcher and release()s the previous one, which
      // decRefs and closes the old DirectoryReader. That closes bloom FieldsProducers and unmaps
      // their .blm inputs (see MongotBloomFilteredFieldsProducer#close).
      int evicted = MongotBloomEvictor.evictBloomFromReader(oldDirectoryReader);
      if (evicted > 0) {
        this.bloomEvictionCounter.increment(evicted);
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
