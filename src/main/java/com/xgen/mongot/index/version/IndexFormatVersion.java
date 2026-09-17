package com.xgen.mongot.index.version;

import com.google.errorprone.annotations.DoNotMock;
import com.xgen.mongot.util.Check;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexFormatVersion does not have any methods corresponding to optional capabilities that are
 * supported for a given index (example: supportsUuidAndNull()). These are present in
 * IndexCapabilities.java
 */
@DoNotMock(value = "Prefer using a real object, such as IndexFormatVersion.CURRENT")
public final class IndexFormatVersion implements FormatVersion {

  private static class Builder {
    private int versionNumber;
    private int minSearchIndexFeatureVersion = 3;
    private int minVectorIndexFeatureVersion = 3;

    private Builder() {}

    private Builder versionNumber(int versionNumber) {
      this.versionNumber = versionNumber;
      return this;
    }

    private Builder minSearchIndexFeatureVersion(int minSearchIndexFeatureVersion) {
      this.minSearchIndexFeatureVersion = minSearchIndexFeatureVersion;
      return this;
    }

    private Builder minVectorIndexFeatureVersion(int minVectorIndexFeatureVersion) {
      this.minVectorIndexFeatureVersion = minVectorIndexFeatureVersion;
      return this;
    }

    private IndexFormatVersion build() {
      return new IndexFormatVersion(
          this.versionNumber, this.minSearchIndexFeatureVersion, this.minVectorIndexFeatureVersion);
    }
  }

  /** Format version of all indexes defined before index_feature_version was introduced. */
  public static final IndexFormatVersion FIVE =
      new Builder()
          .versionNumber(5)
          .minSearchIndexFeatureVersion(0)
          .minVectorIndexFeatureVersion(3)
          .build();

  /**
   * Format version introduced in 2024/05 with the Java 21 upgrade. All {@link IndexFormatVersion}
   * six indexes support features of at least index_feature_version=3.
   */
  public static final IndexFormatVersion SIX =
      new Builder()
          .versionNumber(6)
          .minSearchIndexFeatureVersion(3)
          .minVectorIndexFeatureVersion(3)
          .build();

  /**
   * The version number that indexes are supposed to be on, or should be resynced if not.
   *
   * <p>Lucene-backed indexes only. Vectorlite indexes are versioned by {@link
   * VectorliteFormatVersion} instead.
   *
   * <p>This version number should be incremented when a change has been made that should require
   * indexes be rebuilt. Examples of this could include:
   *
   * <ul>
   *   <li>Adding a new data type that would show up in newly indexed documents but not in already
   *       indexed documents (probably want a more graceful way to do this)
   *   <li>The underlying format of the index has been changed in a non-backwards compatible way
   * </ul>
   */
  public static final IndexFormatVersion CURRENT = SIX;

  /**
   * The minimum version number supported by this mongot version.
   *
   * <p>This version should be kept to the CURRENT index format version minus 1. While it is
   * technically possible to support an arbitrary number of past versions, to keep index read/write
   * logic simple, we limit by convention the number of supported index format versions at any time
   * to just CURRENT and CURRENT - 1.
   */
  public static final IndexFormatVersion MIN_SUPPORTED_VERSION = FIVE;

  /**
   * The maximum version number supported by this mongot version.
   *
   * <p>This version should be equal to the CURRENT index format version unless a project is taking
   * place that requires reindexing. In that case, the MAX_SUPPORTED_VERSION is equal to the
   * CANDIDATE index format version.
   */
  public static final IndexFormatVersion MAX_SUPPORTED_VERSION = CURRENT;

  private static final Logger LOG = LoggerFactory.getLogger(IndexFormatVersion.class);

  public final int versionNumber;
  private final int minSearchIndexFeatureVersion;
  private final int minVectorIndexFeatureVersion;

  public static IndexFormatVersion create(int versionNumber) {

    if (versionNumber == 5) {
      return FIVE;
    }

    if (versionNumber == 6) {
      return SIX;
    }

    // If index format version is unknown, assume the index format version has no minimum index
    // feature version. This should never happen, though we can safely assume that this index format
    // version doesn't by itself guarantee additional index features from index feature versions >0.
    LOG.atWarn()
        .addKeyValue("indexFormatVersion", versionNumber)
        .log("unknown index format version");
    return new Builder()
        .versionNumber(versionNumber)
        .minSearchIndexFeatureVersion(0)
        .minVectorIndexFeatureVersion(0)
        .build();
  }

  /**
   * Constructor pairs the integer value for this {@link IndexFormatVersion} with the minimum {@link
   * IndexCapabilities} that this {@link IndexFormatVersion} supports. An {@link IndexFormatVersion}
   * with a minimum {@link IndexCapabilities}=X set will always support features in {@link
   * IndexCapabilities} X, regardless of the {@link IndexCapabilities} configured in a user's {@link
   * com.xgen.mongot.index.definition.SearchIndexDefinition}.
   *
   * <p>Example:
   *
   * <pre>
   *
   *   // ivfFive has no minimum IndexFeatureVersion, and always will use the indexFeatureVersion
   *   // configured in its index definition.
   *
   *   IndexFormatVersion ivfFive = new IndexFormatVersion(5, 0, 0);
   *   Assert.assertEqual(2, ivfFive.IndexCapabilitiesGivenParsedFeatureVersion(2).versionNumber)
   *
   *
   *   // ivfSix has a minimum IndexFeatureVersion = 3 - so the IndexFeatureVersion of
   *   // an indexFormatVersion == 6 index will effectively be:
   *   //   == max(3, index feature version in index definition)
   *
   *   IndexFormatVersion ivfSix = new IndexFormatVersion(6, 3, 3);
   *   Assert.assertEqual(3, ivfSix.indexFeatureVersionGivenParsedFeatureVersion(2).versionNumber);
   *   Assert.assertEqual(3, ivfSix.indexFeatureVersionGivenParsedFeatureVersion(3).versionNumber);
   *   Assert.assertEqual(4, ivfSix.indexFeatureVersionGivenParsedFeatureVersion(4).versionNumber);
   *  </pre>
   */
  // TODO(CLOUDP-313521): Create a builder for this class
  private IndexFormatVersion(
      int versionNumber, int minSearchIndexFeatureVersion, int minVectorIndexFeatureVersion) {
    Check.argIsPositive(versionNumber, "versionNumber");
    this.versionNumber = versionNumber;
    this.minSearchIndexFeatureVersion = minSearchIndexFeatureVersion;
    this.minVectorIndexFeatureVersion = minVectorIndexFeatureVersion;
  }

  @Override
  public IndexEngine engine() {
    return IndexEngine.LUCENE;
  }

  @Override
  public int versionNumber() {
    return this.versionNumber;
  }

  @Override
  public boolean isCurrent() {
    return this.equals(CURRENT);
  }

  @Override
  public boolean isSupported() {
    return this.versionNumber >= MIN_SUPPORTED_VERSION.versionNumber
        && this.versionNumber <= MAX_SUPPORTED_VERSION.versionNumber;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof IndexFormatVersion)) {
      return false;
    }

    return this.versionNumber == ((IndexFormatVersion) other).versionNumber;
  }

  @Override
  public int hashCode() {
    return this.versionNumber;
  }

  /**
   * Getter for the min supported search index feature version
   *
   * @return The minimum search index feature version supported but this index format version
   */
  public int minSearchFeatureVersion() {
    return this.minSearchIndexFeatureVersion;
  }

  /**
   * Getter for the min supported vector index feature version
   *
   * @return The minimum vector index feature version supported but this index format version
   */
  public int minVectorFeatureVersion() {
    return this.minVectorIndexFeatureVersion;
  }
}
