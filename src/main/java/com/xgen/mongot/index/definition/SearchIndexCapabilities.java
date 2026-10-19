package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;

/**
 * This class describes the set of optional features supported by a specific search index.
 *
 * <p>The feature set of a $search index is determined by the {@link IndexFormatVersion} and the
 * {@link #CURRENT_FEATURE_VERSION} of the binary that initially created the index.
 *
 * <p>Some features may not be available on old indexes, but we don't want to wait for the next mass
 * reindexing project to get features to market for new customers. QueryFactories can use this class
 * to reason about whether a given feature or optimization is valid for the current search index.
 */
public class SearchIndexCapabilities implements IndexCapabilities {

  /**
   * The index feature version number that this mongot supports. Search and Vector indexes each have
   * a separate IndexFeatureVersion.
   *
   * <p>This version number should be incremented when a change has been made that should require
   * indexes be rebuilt. An example of this could include adding a new data type that would show up
   * in newly indexed documents but not in already indexed documents.
   *
   * <p>Both the Feature Version and IndexFormatVersion signal that indexes should be rebuilt, but
   * IndexFeatureVersion differs in that Feature Version is meant to allow users to upgrade their
   * indexes via an opt-in mechanism whereas IndexFormatVersion forces index upgrades.
   */
  public static final int CURRENT_FEATURE_VERSION = 4;

  /**
   * Minimum search index feature version supported by mongot. All customers were forcefully
   * reindexed to at least feature version 3 when format version 6 was introduced. Value only used
   * in testing.
   */
  public static final int MIN_SUPPORTED_FEATURE_VERSION = 3;

  /**
   * The latest index capabilities corresponding to the combination of {@link
   * IndexFormatVersion#CURRENT} and {@link #CURRENT_FEATURE_VERSION}
   */
  public static final SearchIndexCapabilities CURRENT =
      new SearchIndexCapabilities(IndexFormatVersion.CURRENT, CURRENT_FEATURE_VERSION);

  private final int effectiveVersion;

  /**
   * Create a set of capabilities based on the IndexFormatVersion and indexFeatureVersion
   *
   * @param formatVersion - the {@link IndexFormatVersion} that the index was created under.
   * @param indexFeatureVersion - the index feature version associated with the binary that first
   *     created this index.
   */
  public SearchIndexCapabilities(IndexFormatVersion formatVersion, int indexFeatureVersion) {
    this.effectiveVersion =
        Integer.max(formatVersion.minSearchFeatureVersion(), indexFeatureVersion);
  }

  @Override
  public boolean supportsObjectIdAndBooleanDocValues() {
    return true;
  }

  @Override
  public boolean supportsFieldExistsQuery() {
    return true;
  }

  @Override
  public boolean isMetaIdSortable() {
    return true;
  }

  @Override
  public boolean supportsEmbeddedNumericAndDateV2() {
    return this.effectiveVersion >= 4;
  }

  @Override
  public boolean usesReducedAutocompleteIndexOptions() {
    return this.effectiveVersion >= 5;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchIndexCapabilities that = (SearchIndexCapabilities) o;
    return this.effectiveVersion == that.effectiveVersion;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(this.effectiveVersion);
  }

  @Override
  public String toString() {
    return "SearchIndexCapabilities(" + this.effectiveVersion + ')';
  }
}
