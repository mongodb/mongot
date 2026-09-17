package com.xgen.mongot.index.version;

import com.xgen.mongot.util.Check;

/**
 * On-disk format version of a vectorlite index, separate from the Lucene {@link
 * IndexFormatVersion}. It is also the {@code f} segment of the SLS spec id, so a bump gives every
 * vectorlite index a fresh log. Min supported equals current because the engine has one reader. See
 * docs/internal/development/vectorlite-format-version.md.
 */
public record VectorliteFormatVersion(int versionNumber) implements FormatVersion {

  /** The format this build writes. */
  public static final VectorliteFormatVersion CURRENT = new VectorliteFormatVersion(1);

  /** The oldest format this build opens. Always equal to {@link #CURRENT}. */
  public static final VectorliteFormatVersion MIN_SUPPORTED_VERSION = CURRENT;

  public VectorliteFormatVersion {
    Check.argIsPositive(versionNumber, "versionNumber");
  }

  @Override
  public IndexEngine engine() {
    return IndexEngine.VECTORLITE;
  }

  @Override
  public boolean isCurrent() {
    return this.equals(CURRENT);
  }

  @Override
  public boolean isSupported() {
    return this.versionNumber >= MIN_SUPPORTED_VERSION.versionNumber
        && this.versionNumber <= CURRENT.versionNumber;
  }
}
