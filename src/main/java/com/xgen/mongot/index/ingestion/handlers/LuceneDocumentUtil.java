package com.xgen.mongot.index.ingestion.handlers;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.lucene.document.Document;

public final class LuceneDocumentUtil {

  private LuceneDocumentUtil() {
    // Utility class
  }

  /**
   * Modifies the `left` document by appending fields from `right`.
   *
   * @return returns the modified `left` argument for chaining calls
   */
  @CanIgnoreReturnValue
  public static Document add(Document left, Document right) {
    right.getFields().forEach(left::add);
    return left;
  }
}
