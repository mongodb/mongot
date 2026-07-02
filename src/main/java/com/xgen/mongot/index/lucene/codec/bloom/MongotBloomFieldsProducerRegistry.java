package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomHeapEvictor.BloomHeapStats;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;

/**
 * Tracks heap-loaded {@link MongotBloomFilteredFieldsProducer} instances by segment and field.
 *
 * <p>Lucene's {@code PerFieldPostingsFormat.FieldsReader} nests per-field producers without a
 * public accessor (unlike {@code PerFieldKnnVectorsFormat.FieldsReader#getFieldReader}).
 * Registration lets {@link MongotBloomHeapEvictor} reach the {@code _id} bloom producer without
 * reflection.
 */
public final class MongotBloomFieldsProducerRegistry {

  private static final ConcurrentHashMap<Key, MongotBloomFilteredFieldsProducer> PRODUCERS =
      new ConcurrentHashMap<>();

  private MongotBloomFieldsProducerRegistry() {}

  static Key register(
      SegmentReadState state, String fieldName, MongotBloomFilteredFieldsProducer producer) {
    Key key = Key.from(state, fieldName);
    PRODUCERS.put(key, producer);
    return key;
  }

  static void unregister(Key key, MongotBloomFilteredFieldsProducer producer) {
    PRODUCERS.remove(key, producer);
  }

  static Optional<MongotBloomFilteredFieldsProducer> lookup(
      SegmentReader reader, String fieldName) {
    return Optional.ofNullable(PRODUCERS.get(Key.from(reader, fieldName)));
  }

  /**
   * Sums the heap-resident {@code _id} bloom footprint across <em>every</em> registered producer on
   * the process, regardless of index. Backs the process-wide bloom heap gauges: bloom is a
   * feature-level concern (see PR discussion on this metric being process- rather than
   * index-scoped) and aggregating here keeps the exported timeseries count independent of the
   * number of indexes.
   *
   * <p>Reading from this registry rather than a {@link org.apache.lucene.index.DirectoryReader}
   * lets the heap gauges report resident bloom during initial sync, when the searcher's reader is
   * not refreshed and therefore never opens the bloom segments. The scan is {@code O(registered
   * producers)} but the gauges cache it (see {@code LuceneSearcherManager}), so it runs at most a
   * couple times a minute off any hot path.
   */
  public static BloomHeapStats bloomHeapStats() {
    @Var int loadedSegments = 0;
    @Var long heapBytes = 0;
    for (MongotBloomFilteredFieldsProducer producer : PRODUCERS.values()) {
      if (producer.hasHeapLoadedBloom()) {
        loadedSegments++;
        heapBytes += producer.heapBloomRamBytesUsed();
      }
    }
    return new BloomHeapStats(loadedSegments, heapBytes);
  }

  static final class Key {
    private final Directory directory;
    private final String segmentName;
    private final byte[] segmentId;
    private final String fieldName;

    private Key(Directory directory, String segmentName, byte[] segmentId, String fieldName) {
      this.directory = directory;
      this.segmentName = segmentName;
      this.segmentId = segmentId;
      this.fieldName = fieldName;
    }

    static Key from(SegmentReadState state, String fieldName) {
      return new Key(
          state.segmentInfo.dir, state.segmentInfo.name, state.segmentInfo.getId(), fieldName);
    }

    static Key from(SegmentReader reader, String fieldName) {
      var segmentInfo = reader.getSegmentInfo().info;
      return new Key(reader.directory(), segmentInfo.name, segmentInfo.getId(), fieldName);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Key that)) {
        return false;
      }
      return this.directory == that.directory
          && this.segmentName.equals(that.segmentName)
          && Arrays.equals(this.segmentId, that.segmentId)
          && this.fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          System.identityHashCode(this.directory),
          this.segmentName,
          Arrays.hashCode(this.segmentId),
          this.fieldName);
    }
  }
}
