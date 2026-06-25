package com.xgen.mongot.index;

import java.io.IOException;

/** An IndexReader is capable of reading an index and producing results for queries. */
public interface IndexReader {

  /**
   * Refreshes the IndexReader such that future calls to query() should see all updates to the Index
   * that occurred prior to the refresh() call.
   */
  void refresh() throws IOException, ReaderClosedException;

  void open();

  void close();
}
