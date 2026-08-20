package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.util.Optional;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;

/**
 * Drops the loaded {@code _id} bloom fast-fail sets from open segment readers without closing them,
 * so that {@code _id} lookups stop consulting bloom once the read policy flips to steady state.
 *
 * <p><b>What this reclaims depends on the storage mode.</b> Under {@link
 * MongotFuzzySet.StorageMode#HEAP} the per-field sets hold heap {@code long[]}s, so dropping them
 * frees that heap outright. Under {@link MongotFuzzySet.StorageMode#INPUT} the words live in the OS
 * page cache behind the mapped {@code .blm}, so dropping the references leaves the mapping in place
 * until the segment reader closes or the segment is merged away.
 *
 * <p>Stopping the probing is the part that holds in both modes, and is the point: in steady state a
 * bloom probe is a net cost rather than a saving, because for an in-memory working set the delegate
 * {@code _id} lookup is already cheap, and under memory pressure a page-cache-backed probe can
 * fault a reclaimed {@code .blm} page back in, turning a "fast-fail" into a disk seek. Dropping the
 * sets stops that and lets the untouched pages go cold and be reclaimed by the OS.
 *
 * <p>Called from {@link com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager} when the bloom
 * read policy flips from initial sync (load bloom) to steady state (skip bloom). That transition is
 * driven by dynamic feature flags at searcher refresh time, not by Lucene merges.
 *
 * <p>A merge-scheduler hook is not sufficient: {@code DirectoryReader.openIfChanged} can reuse
 * existing {@link SegmentReader} instances that already wired up bloom sets, and initial-sync bloom
 * segments may remain in the index until background merges compact them away. Eviction must run on
 * those live readers at policy flip time so the fast-fail structures are dropped without breaking
 * near-real-time refresh.
 */
public final class MongotBloomEvictor {

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();

  private MongotBloomEvictor() {}

  /**
   * Footprint of the {@code _id} bloom bitsets currently loaded across a reader's segments.
   *
   * @param loadedSegments number of segments with a bloom set wired up for fast-fail
   * @param heapBytes bytes resident on the JVM heap — the bloom footprint under {@link
   *     MongotFuzzySet.StorageMode#HEAP}, and {@code 0} under {@link
   *     MongotFuzzySet.StorageMode#INPUT}
   * @param mappedBytes bytes in the OS page cache behind the memory-mapped {@code .blm} — the bloom
   *     footprint under {@link MongotFuzzySet.StorageMode#INPUT}, and {@code 0} under {@link
   *     MongotFuzzySet.StorageMode#HEAP}
   */
  public record BloomStats(int loadedSegments, long heapBytes, long mappedBytes) {}

  /**
   * Drops the loaded {@code _id} bloom fast-fail sets from each segment of {@code reader}.
   *
   * @return the number of segments that had a bloom set loaded before eviction
   */
  public static int evictBloomFromReader(DirectoryReader reader) {
    @Var int evicted = 0;
    for (LeafReaderContext leaf : reader.leaves()) {
      if (leaf.reader() instanceof SegmentReader segmentReader) {
        Optional<MongotBloomFilteredFieldsProducer> producer =
            findIdBloomFieldsProducer(segmentReader.getPostingsReader(), segmentReader);
        if (producer.isPresent() && producer.get().hasLoadedBloom()) {
          producer.get().evictBloom();
          evicted++;
        }
      }
    }
    return evicted;
  }

  /**
   * Counts segments with a loaded {@code _id} bloom and sums their footprints. Exactly one of
   * {@code heapBytes} and {@code mappedBytes} is non-zero, depending on the storage mode. Under
   * {@link MongotFuzzySet.StorageMode#INPUT}, {@code mappedBytes} is unaffected by {@link
   * #evictBloomFromReader}, which stops probing without unmapping the {@code .blm}.
   */
  public static BloomStats bloomStats(DirectoryReader reader) {
    @Var int loadedSegments = 0;
    @Var long heapBytes = 0;
    @Var long mappedBytes = 0;
    for (LeafReaderContext leaf : reader.leaves()) {
      if (leaf.reader() instanceof SegmentReader segmentReader) {
        Optional<MongotBloomFilteredFieldsProducer> producer =
            findIdBloomFieldsProducer(segmentReader.getPostingsReader(), segmentReader);
        if (producer.isPresent() && producer.get().hasLoadedBloom()) {
          loadedSegments++;
          heapBytes += producer.get().heapBloomRamBytesUsed();
          mappedBytes += producer.get().mappedBloomBytes();
        }
      }
    }
    return new BloomStats(loadedSegments, heapBytes, mappedBytes);
  }

  private static Optional<MongotBloomFilteredFieldsProducer> findIdBloomFieldsProducer(
      FieldsProducer producer, SegmentReader segmentReader) {
    if (producer instanceof MongotBloomFilteredFieldsProducer bloomProducer) {
      return Optional.of(bloomProducer);
    }
    return MongotBloomFieldsProducerRegistry.lookup(segmentReader, ID_FIELD);
  }
}
