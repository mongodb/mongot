/*
 * Derived from lucene-mongot 10.1.0 FuzzySet.
 */

package com.xgen.mongot.index.lucene.codec.bloom;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.codecs.bloom.HashFunction;
import org.apache.lucene.codecs.bloom.MurmurHash64;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * A class used to represent a set of many, potentially large, values (e.g. many long strings such
 * as URLs), using a significantly smaller amount of memory.
 *
 * <p>The set is "lossy" in that it cannot definitively state that is does contain a value but it
 * <em>can</em> definitively say if a value is <em>not</em> in the set. It can therefore be used as
 * a Bloom Filter. Another application of the set is that it can be used to perform fuzzy counting
 * because it can estimate reasonably accurately how many unique values are contained in the set.
 *
 * <p>Where the bitset lives depends on whether the set is being built or read:
 *
 * <ul>
 *   <li><b>Write / build path</b> (see {@link #createOptimalSet}, {@link #addValue}, {@link
 *       #serialize}, {@link #downsize}): a mutable bitset backed by a heap {@code long[]}. This
 *       allocation is transient — it lives only for the duration of a flush or merge. Once a client
 *       has finished recording values, {@link #downsize(float)} can produce a suitably smaller set
 *       for the number of values recorded and desired saturation.
 *   <li><b>Input read path</b> (see {@link #fromInput}, used under {@link StorageMode#INPUT}): an
 *       immutable, read-only view over the serialized bitset words of a {@code .blm} file, accessed
 *       through a {@link RandomAccessInput}. The words are read in place through the directory's
 *       input rather than being copied onto the JVM heap.
 *   <li><b>Heap read path</b> (see {@link #deserialize}, used under {@link StorageMode#HEAP}): the
 *       serialized words are copied out of the {@code .blm} into a heap {@code long[]} at segment
 *       open, and that array persists for the life of the segment reader.
 * </ul>
 *
 * <p>Which read path applies is chosen by {@link BloomStoragePolicy} from the {@link
 * com.xgen.mongot.featureflag.Feature#BLOOM_FILTER_ON_HEAP} static feature flag. Both read the
 * identical on-disk format; only the mutating build-path methods are unsupported on a read-path
 * set.
 *
 * <p><b>Thread safety</b> depends on the path, not the storage mode. Build-path sets are mutable
 * and single-threaded: only the flushing thread may call {@link #addValue}, {@link #downsize} or
 * {@link #serialize}. Read-path sets — from either {@link #deserialize} or {@link #fromInput} — are
 * immutable once constructed and safe for concurrent {@link #contains(BytesRef)} from any number of
 * query threads.
 *
 * @lucene.experimental
 */
public class MongotFuzzySet implements Accountable {

  /**
   * Result from {@link MongotFuzzySet#contains(BytesRef)}: can never return definitively YES
   * (always MAYBE), but can sometimes definitely return NO.
   */
  public enum ContainsResult {
    MAYBE,
    NO
  };

  /** Where a read-path bloom bitset's words live. */
  public enum StorageMode {
    /** Words are copied into a heap {@code long[]} at segment open. */
    HEAP,
    /**
     * Words are read in place through the directory's {@link org.apache.lucene.store.IndexInput}
     * instead of being copied onto the heap.
     *
     * <p>In production {@link com.xgen.mongot.index.lucene.directory.FileSystemDirectory} routes
     * the {@code .blm} extension to an {@code MMapDirectory}, so these reads are served from the OS
     * page cache. That is not guaranteed: an {@code NRTCachingDirectory} may serve a freshly
     * written {@code .blm} from a heap-backed buffer instead. The read is correct either way, and
     * even then it avoids the second heap copy {@link #HEAP} would make.
     */
    INPUT
  }

  /**
   * The words of the bitset, and how they are addressed. Implementations differ only in where the
   * bytes live; the serialized form on disk is identical for both.
   */
  private sealed interface Bitset {
    /** Returns word {@code wordNum} of the bitset. */
    long word(int wordNum);

    /** ORs {@code bitmask} into word {@code wordNum}. Read-only backings reject this. */
    default void or(int wordNum, long bitmask) {
      throw new UnsupportedOperationException(
          "bloom bitset is read-only; mutating build-path methods must not be called on a"
              + " read-path set");
    }

    /** JVM heap bytes occupied by the words. */
    long heapBytes();
  }

  /**
   * Heap-resident words. Used for the build-path bitset and for reads under {@link
   * StorageMode#HEAP}.
   */
  private record HeapBitset(long[] words) implements Bitset {
    @Override
    public long word(int wordNum) {
      return words[wordNum];
    }

    @Override
    public void or(int wordNum, long bitmask) {
      words[wordNum] |= bitmask;
    }

    @Override
    public long heapBytes() {
      return (long) words.length * Long.BYTES;
    }
  }

  /**
   * Read-only words within a {@code .blm}, at {@code [baseOffset, baseOffset + numWords * 8)}, read
   * through the directory's input. Used for the read path under {@link StorageMode#INPUT}.
   */
  private record InputBitset(RandomAccessInput words, long baseOffset) implements Bitset {
    @Override
    public long word(int wordNum) {
      try {
        return words.readLong(baseOffset + (long) wordNum * Long.BYTES);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read bloom filter word from .blm", e);
      }
    }

    @Override
    public long heapBytes() {
      // Read-path words are read through the directory's input, not held on the JVM heap.
      return 0;
    }
  }

  private HashFunction hashFunction;

  private final Bitset bitset;

  // Where this set's words live. The build path is always HEAP, and read-path
  // sets record which of the two read paths produced them.
  private final StorageMode readPathStorageMode;

  private int bloomSize;
  private final int hashCount;
  private final int numWords;

  // The sizes of BitSet used are all numbers that, when expressed in binary form,
  // are all ones. This is to enable fast downsizing from one bitset to another
  // by simply ANDing each set index in one bitset with the size of the target bitset
  // - this provides a fast modulo of the number. Values previously accumulated in
  // a large bitset and then mapped to a smaller set can be looked up using a single
  // AND operation of the query term's hash rather than needing to perform a 2-step
  // translation of the query term that mirrors the stored content's reprojections.
  static final int[] usableBitSetSizes;

  static {
    usableBitSetSizes = new int[26];
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      usableBitSetSizes[i] = (1 << (i + 6)) - 1;
    }
  }

  /**
   * Rounds down required maxNumberOfBits to the nearest number that is made up of all ones as a
   * binary number. Use this method where controlling memory use is paramount.
   */
  public static int getNearestSetSize(int maxNumberOfBits) {
    int result = usableBitSetSizes[0];
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      if (usableBitSetSizes[i] <= maxNumberOfBits) {
        result = usableBitSetSizes[i];
      }
    }
    return result;
  }

  public static MongotFuzzySet createSetBasedOnMaxMemory(int maxNumBytes) {
    int setSize = getNearestSetSize(maxNumBytes);
    return new MongotFuzzySet(allocateBuildBitset(setSize + 1), StorageMode.HEAP, setSize, 1);
  }

  public static MongotFuzzySet createOptimalSet(int maxNumUniqueValues, float targetMaxFpp) {
    int setSize =
        (int)
            Math.ceil(
                (maxNumUniqueValues * Math.log(targetMaxFpp))
                    / Math.log(1 / Math.pow(2, Math.log(2))));
    setSize = getNearestSetSize(2 * setSize);
    int optimalK = (int) Math.round(((double) setSize / maxNumUniqueValues) * Math.log(2));
    return new MongotFuzzySet(
        allocateBuildBitset(setSize + 1), StorageMode.HEAP, setSize, optimalK);
  }

  /** Allocates a zeroed, mutable heap bitset of {@code numBits} bits. */
  private static Bitset allocateBuildBitset(int numBits) {
    return new HeapBitset(new long[FixedBitSet.bits2words(numBits)]);
  }

  private MongotFuzzySet(
      Bitset bitset, StorageMode readPathStorageMode, int bloomSize, int hashCount) {
    this(bitset, readPathStorageMode, bloomSize, hashCount, FixedBitSet.bits2words(bloomSize + 1));
  }

  private MongotFuzzySet(
      Bitset bitset, StorageMode readPathStorageMode, int bloomSize, int hashCount, int numWords) {
    this.bitset = bitset;
    this.readPathStorageMode = readPathStorageMode;
    this.bloomSize = bloomSize;
    this.hashFunction = MurmurHash64.INSTANCE;
    this.hashCount = hashCount;
    this.numWords = numWords;
  }

  /**
   * Creates a read-only set that reads its bitset words directly from {@code words} rather than
   * copying them onto the heap. {@code words} must cover exactly the {@code numWords} serialized
   * longs for one field (i.e. a {@link RandomAccessInput#length()} of {@code numWords * 8}), so the
   * base offset within the slice is {@code 0}. The caller owns the underlying {@link
   * org.apache.lucene.store.IndexInput} and must keep it open for the lifetime of the returned set.
   *
   * @param words random-access view over the serialized bitset words for a single field
   * @param hashCount the number of hash functions (k) read from the {@code .blm} header
   * @param bloomSize the modulo mask read from the {@code .blm} header
   * @param numWords the number of 64-bit words read from the {@code .blm} header
   * @return a read-only set backed by the given input
   */
  public static MongotFuzzySet fromInput(
      RandomAccessInput words, int hashCount, int bloomSize, int numWords) {
    return new MongotFuzzySet(
        new InputBitset(words, 0L), StorageMode.INPUT, bloomSize, hashCount, numWords);
  }

  private long word(int wordNum) {
    return bitset.word(wordNum);
  }

  private boolean get(int index) {
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    return (word(wordNum) & bitmask) != 0;
  }

  private void set(int index) {
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    bitset.or(wordNum, bitmask);
  }

  private int cardinality() {
    long tot = 0;
    for (int i = 0; i < numWords; ++i) {
      tot += Long.bitCount(word(i));
    }
    return Math.toIntExact(tot);
  }

  private int nextSetBit(int start) {
    int i = start >> 6;
    long w = word(i) >> start;

    if (w != 0) {
      return start + Long.numberOfTrailingZeros(w);
    }

    int limit = numWords;
    while (++i < limit) {
      w = word(i);
      if (w != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(w);
      }
    }

    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * The main method required for a Bloom filter which, given a value determines set membership.
   * Unlike a conventional set, the fuzzy set returns NO or MAYBE rather than true or false. Hash
   * generation follows the same principles as {@link #addValue(BytesRef)}
   *
   * @return NO or MAYBE
   */
  public ContainsResult contains(BytesRef value) {
    long hash = hashFunction.hash(value);
    int msb = (int) (hash >>> Integer.SIZE);
    int lsb = (int) hash;
    for (int i = 0; i < hashCount; i++) {
      int bloomPos = (lsb + i * msb);
      if (!mayContainValue(bloomPos)) {
        return ContainsResult.NO;
      }
    }
    return ContainsResult.MAYBE;
  }

  /**
   * Serializes the data set to file using the following format:
   *
   * <ul>
   *   <li>FuzzySet --&gt;hashCount,BloomSize, NumBitSetWords,BitSetWord<sup>NumBitSetWords</sup>
   *   <li>hashCount --&gt; {@link DataOutput#writeVInt Uint32} The number of hash functions (k).
   *   <li>BloomSize --&gt; {@link DataOutput#writeInt Uint32} The modulo value used to project
   *       hashes into the field's Bitset
   *   <li>NumBitSetWords --&gt; {@link DataOutput#writeInt Uint32} The number of longs
   *   <li>BitSetWord --&gt; {@link DataOutput#writeLong Long} A long from the bitset
   * </ul>
   *
   * @param out Data output stream
   * @throws IOException If there is a low-level I/O error
   */
  public void serialize(DataOutput out) throws IOException {
    out.writeVInt(hashCount);
    out.writeInt(bloomSize);
    out.writeInt(numWords);
    for (int i = 0; i < numWords; i++) {
      out.writeLong(word(i));
    }
  }

  /**
   * Reads a set serialized by {@link #serialize} into a heap {@code long[]}, copying the bitset
   * words onto the JVM heap. This is the read path used under {@link StorageMode#HEAP}; under
   * {@link StorageMode#INPUT} the words are read in place from the memory-mapped {@code .blm}
   * instead — see {@link #fromInput}. Both read the identical on-disk format.
   *
   * @param in positioned at the start of a serialized set
   * @return an immutable, heap-backed set safe for concurrent {@link #contains(BytesRef)}
   * @throws IOException If there is a low-level I/O error
   */
  public static MongotFuzzySet deserialize(DataInput in) throws IOException {
    int hashCount = in.readVInt();
    int bloomSize = in.readInt();
    int numWords = in.readInt();
    long[] words = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      words[i] = in.readLong();
    }
    return new MongotFuzzySet(
        new HeapBitset(words), StorageMode.HEAP, bloomSize, hashCount, numWords);
  }

  private boolean mayContainValue(int aHash) {
    // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
    int pos = aHash & bloomSize;
    return get(pos);
  }

  /**
   * Records a value in the set. The referenced bytes are hashed. From the 64-bit generated hash,
   * two 32-bit hashes are derived from the msb and lsb which can be used to derive more hashes (see
   * https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf). Finally, each generated hash
   * is modulo n'd where n is the chosen size of the internal bitset.
   *
   * @param value the key value to be hashed
   * @throws IOException If there is a low-level I/O error
   */
  public void addValue(BytesRef value) throws IOException {
    long hash = hashFunction.hash(value);
    int msb = (int) (hash >>> Integer.SIZE);
    int lsb = (int) hash;
    for (int i = 0; i < hashCount; i++) {
      // Bitmasking using bloomSize is effectively a modulo operation.
      int bloomPos = (lsb + i * msb) & bloomSize;
      set(bloomPos);
    }
  }

  /**
   * @param targetMaxSaturation A number between 0 and 1 describing the % of bits that would ideally
   *     be set in the result. Lower values have better accuracy but require more space.
   * @return a smaller MongotFuzzySet or null if the current set is already over-saturated
   */
  public MongotFuzzySet downsize(float targetMaxSaturation) {
    int numBitsSet = cardinality();
    int rightSizedBitSetSize = bloomSize;
    // Hopefully find a smaller size bitset into which we can project accumulated values while
    // maintaining desired saturation level
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      int candidateBitsetSize = usableBitSetSizes[i];
      float candidateSaturation = (float) numBitsSet / (float) candidateBitsetSize;
      if (candidateSaturation <= targetMaxSaturation) {
        rightSizedBitSetSize = candidateBitsetSize;
        break;
      }
    }
    // Re-project the numbers to a smaller space if necessary
    if (rightSizedBitSetSize < bloomSize) {
      Bitset rightSized = allocateBuildBitset(rightSizedBitSetSize + 1);
      int bitIndex = 0;
      do {
        bitIndex = nextSetBit(bitIndex);
        if (bitIndex != DocIdSetIterator.NO_MORE_DOCS) {
          int downSizedBitIndex = bitIndex & rightSizedBitSetSize;
          int wordNum = downSizedBitIndex >> 6;
          long bitmask = 1L << downSizedBitIndex;
          rightSized.or(wordNum, bitmask);
          bitIndex++;
        }
      } while ((bitIndex >= 0) && (bitIndex <= bloomSize));
      return new MongotFuzzySet(
          rightSized, this.readPathStorageMode, rightSizedBitSetSize, hashCount);
    } else {
      return null;
    }
  }

  public float getTargetMaxSaturation() {
    return 0.5f;
  }

  public float getSaturation() {
    int numBitsSet = cardinality();
    return (float) numBitsSet / (float) bloomSize;
  }

  @Override
  public long ramBytesUsed() {
    return bitset.heapBytes();
  }

  /**
   * Size in bytes of the serialized bitset words backing this set, regardless of where they live.
   * Under {@link StorageMode#INPUT} these bytes are not on the JVM heap; with the usual mmap-backed
   * directory they are resident in the OS page cache backing the {@code .blm}. This is the
   * footprint reported by the {@code bloomFilterMappedBytes} gauge, which keeps its name for
   * dashboard continuity even though an {@code NRTCachingDirectory} can serve a small, recently
   * written {@code .blm} from the heap. Distinct from {@link #ramBytesUsed()}, which is heap-only
   * and therefore {@code 0} for these sets.
   */
  public long bitsetBytes() {
    return (long) numWords * Long.BYTES;
  }

  /** Where this set's bitset words live. */
  public StorageMode readPathStorageMode() {
    return readPathStorageMode;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(hash="
        + hashFunction
        + ", k="
        + hashCount
        + ", bits="
        + cardinality()
        + "/"
        + (bloomSize + 1)
        + ")";
  }
}
