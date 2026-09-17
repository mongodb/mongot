package com.xgen.mongot.index.version;

/** Storage engine of an index generation; decides how {@link Generation#formatVersion} is read. */
public enum IndexEngine {
  LUCENE,
  VECTORLITE
}
