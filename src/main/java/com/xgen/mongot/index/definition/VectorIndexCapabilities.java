package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;

/**
 * VectorIndexes do not (currently) support format or feature versions, so all indexes have the same
 * capabilities. This implementation of {@link IndexCapabilities} allows VectorSearch to reuse the
 * same query logic as $search.
 */
public final class VectorIndexCapabilities implements IndexCapabilities {

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
   * Separate index feature version for vector indexes was introduced at index feature version 3 so
   * anything less than 3 is unsupported. Only used in integration tests for now.
   */
  public static final int MIN_SUPPORTED_FEATURE_VERSION = 3;

  private final int effectiveVersion;

  public VectorIndexCapabilities(IndexFormatVersion formatVersion, int indexFeatureVersion) {
    this.effectiveVersion =
        Integer.max(formatVersion.minVectorFeatureVersion(), indexFeatureVersion);
  }

  @Override
  public boolean supportsObjectIdAndBooleanDocValues() {
    return this.effectiveVersion >= 4;
  }

  @Override
  public boolean supportsFieldExistsQuery() {
    return this.effectiveVersion >= 4;
  }

  @Override
  public boolean isMetaIdSortable() {
    return false;
  }

  @Override
  public boolean supportsEmbeddedNumericAndDateV2() {
    // Vector indexes do not index embedded documents today, so it's vacuously true today that all
    // embedded documents support v2 fields. We also want to ensure that when embedded document
    // support is added, we do index v2 fields and prefer them at query time.
    return true;
  }

  @Override
  public boolean usesReducedAutocompleteIndexOptions() {
    // Vector indexes never index autocomplete fields, so the choice is moot; report the legacy
    // behavior for consistency with existing vector indexes.
    return false;
  }

  @Override
  public String toString() {
    return "VectorIndexCapabilities(" + this.effectiveVersion + ")";
  }
}
