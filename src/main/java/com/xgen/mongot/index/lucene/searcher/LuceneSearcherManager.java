package com.xgen.mongot.index.lucene.searcher;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomReadPolicy;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
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

  private static final Logger LOG = LoggerFactory.getLogger(LuceneSearcherManager.class);

  private final LuceneSearcherFactory factory;
  private final IndexWriter indexWriter;
  private final BooleanSupplier useIdBloomFilter;

  /**
   * Tracks whether bloom bitsets were loaded on the last reader open. Used to detect the
   * initial-sync → steady-state transition so bloom can be proactively evicted from heap.
   *
   * <p>Accessed only inside {@link #refreshIfNeeded}, which {@link ReferenceManager} calls under
   * its refresh lock, so no additional synchronization is needed.
   */
  private volatile boolean lastBloomLoadState;

  /**
   * Protected constructor - does not register gauges. Use {@link #create} to construct instances.
   * Protected to allow test subclasses.
   */
  protected LuceneSearcherManager(
      IndexWriter writer, LuceneSearcherFactory factory, BooleanSupplier useIdBloomFilter)
      throws IOException {
    this.factory = factory;
    this.indexWriter = writer;
    this.useIdBloomFilter = useIdBloomFilter;

    boolean initialBloomLoad = useIdBloomFilter.getAsBoolean();
    this.lastBloomLoadState = initialBloomLoad;
    MongotBloomReadPolicy.setLoadBloomOnHeap(writer.getDirectory(), initialBloomLoad);

    this.current = getSearcher(DirectoryReader.open(writer));
  }

  /** Static factory method that constructs the manager and registers all gauges. */
  @VisibleForTesting
  public static LuceneSearcherManager create(
      IndexWriter writer, LuceneSearcherFactory factory, PerIndexMetricsFactory metrics)
      throws IOException {
    return create(writer, factory, metrics, () -> false);
  }

  /** Static factory method that constructs the manager and registers all gauges. */
  public static LuceneSearcherManager create(
      IndexWriter writer,
      LuceneSearcherFactory factory,
      PerIndexMetricsFactory metrics,
      BooleanSupplier useIdBloomFilter)
      throws IOException {

    LuceneSearcherManager manager = new LuceneSearcherManager(writer, factory, useIdBloomFilter);

    // Register gauges after construction is complete
    metrics.perIndexObjectValueGauge(
        SEGMENT_COUNT_METRIC,
        manager,
        CachedGauge.of(LuceneSearcherManager::getSegmentCount, Duration.ofMinutes(1)));

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

    boolean loadBloomOnHeap = this.useIdBloomFilter.getAsBoolean();
    MongotBloomReadPolicy.setLoadBloomOnHeap(this.indexWriter.getDirectory(), loadBloomOnHeap);
    boolean evictBloom = this.lastBloomLoadState && !loadBloomOnHeap;
    this.lastBloomLoadState = loadBloomOnHeap;
    DirectoryReader newReader;
    if (evictBloom) {
      // Bloom eviction reopens only the DirectoryReader, not the Directory or IndexWriter.
      // The writer and on-disk index stay open for the life of the index partition.
      //
      // openIfChanged and DirectoryReader.open(IndexWriter) can reuse unchanged SegmentReaders
      // from a prior reader; those readers still hold heap-resident bloom bitsets opened under the
      // initial-sync policy. Reopening from the current IndexCommit snapshot creates new segment
      // readers that honor the updated MongotBloomReadPolicy (bloom sidecar remains on disk; heap
      // bitsets are not loaded when policy is false).
      //
      // ReferenceManager swaps to the returned searcher and release()s the previous one, which
      // decRefs and closes the old DirectoryReader. That closes bloom FieldsProducers and clears
      // their in-heap bitsets (see MongotBloomFilteredFieldsProducer#close).
      newReader = DirectoryReader.open(((DirectoryReader) oldReader).getIndexCommit());
    } else {
      newReader = DirectoryReader.openIfChanged((DirectoryReader) oldReader);
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
