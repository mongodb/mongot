package com.xgen.mongot.index.version;

import com.xgen.mongot.util.Check;

/**
 * Format version of an index generation. The engine is the type; the number is read in its line.
 */
public sealed interface FormatVersion permits IndexFormatVersion, VectorliteFormatVersion {

  IndexEngine engine();

  int versionNumber();

  /** Whether this is the version the engine writes now. */
  boolean isCurrent();

  /** Whether this build's engine can open this version. */
  boolean isSupported();

  default IndexFormatVersion asLucene() {
    Check.expectedType(IndexEngine.LUCENE, engine());
    return (IndexFormatVersion) this;
  }

  default VectorliteFormatVersion asVectorlite() {
    Check.expectedType(IndexEngine.VECTORLITE, engine());
    return (VectorliteFormatVersion) this;
  }

  static FormatVersion of(IndexEngine engine, int versionNumber) {
    return switch (engine) {
      case LUCENE -> IndexFormatVersion.create(versionNumber);
      case VECTORLITE -> new VectorliteFormatVersion(versionNumber);
    };
  }

  static FormatVersion current(IndexEngine engine) {
    return switch (engine) {
      case LUCENE -> IndexFormatVersion.CURRENT;
      case VECTORLITE -> VectorliteFormatVersion.CURRENT;
    };
  }
}
