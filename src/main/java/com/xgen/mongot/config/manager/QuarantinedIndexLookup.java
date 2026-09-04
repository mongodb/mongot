package com.xgen.mongot.config.manager;

import com.xgen.mongot.index.IndexUnavailableException;
import java.util.UUID;

/**
 * The query path's view of the indexes whose config journal entry could not be parsed.
 *
 * <p>Deliberately narrow: a quarantined entry holds its journal BSON verbatim so it can be written
 * back untouched, and the query path has no business reading it. All it needs is the parse error to
 * report.
 */
public interface QuarantinedIndexLookup {

  /**
   * Throws if a quarantined live index is the one being queried, and does nothing otherwise.
   *
   * <p>Called where the index is absent from {@link com.xgen.mongot.catalog.IndexCatalog}, to tell
   * a quarantined index apart from one that never existed - the latter yields an empty cursor.
   */
  void throwIfQuarantined(String database, UUID collectionUuid, String indexName)
      throws IndexUnavailableException;
}
