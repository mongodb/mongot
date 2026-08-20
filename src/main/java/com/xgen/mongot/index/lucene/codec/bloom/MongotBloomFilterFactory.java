/*
 * Derived from lucene-mongot 10.1.0 BloomFilterFactory and DefaultBloomFilterFactory.
 */

package com.xgen.mongot.index.lucene.codec.bloom;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Creates index-time {@link MongotFuzzySet} instances appropriately configured for each field, and
 * right-sizes bitsets for serialization.
 *
 * <p>Merges Lucene's {@code BloomFilterFactory} (abstract base) and {@code
 * DefaultBloomFilterFactory} (the only implementation used) into a single concrete class that
 * operates on {@link MongotFuzzySet} instead of Lucene's {@code FuzzySet}. The default policy is to
 * allocate a bitset with ~10% saturation assuming a unique term per document. Bits are set via the
 * MurmurHash64 hashing function.
 *
 * @lucene.experimental
 */
public class MongotBloomFilterFactory {

  /**
   * Creates an appropriately sized bloom filter for the given field.
   *
   * @param state The content to be indexed
   * @param info the field requiring a BloomFilter
   * @return An appropriately sized set or null if no BloomFiltering required
   */
  public MongotFuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
    // Assume all of the docs have a unique term (e.g. a primary key) and we hope to maintain a set
    // with 10% of bits set
    return MongotFuzzySet.createOptimalSet(state.segmentInfo.maxDoc(), 0.1023f);
  }

  /**
   * Called when downsizing bitsets for serialization
   *
   * @param fieldInfo The field with sparse set bits
   * @param initialSet The bits accumulated
   * @return null or a hopefully more densely packed, smaller bitset
   */
  public MongotFuzzySet downsize(FieldInfo fieldInfo, MongotFuzzySet initialSet) {
    float targetMaxSaturation = initialSet.getTargetMaxSaturation();
    return initialSet.downsize(targetMaxSaturation);
  }

  /**
   * Used to determine if the given filter has reached saturation and should be retired i.e. not
   * saved any more
   *
   * @param bloomFilter The bloomFilter being tested
   * @param fieldInfo The field with which this filter is associated
   * @return true if the set has reached saturation and should be retired
   */
  public boolean isSaturated(MongotFuzzySet bloomFilter, FieldInfo fieldInfo) {
    // Don't bother saving bitsets if >90% of bits are set - we don't want to
    // throw any more memory at this problem.
    return bloomFilter.getSaturation() > 0.9f;
  }
}
