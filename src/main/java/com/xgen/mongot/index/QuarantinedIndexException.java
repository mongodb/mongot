package com.xgen.mongot.index;

/**
 * A query named an index whose config journal entry could not be parsed on this node.
 *
 * <p>Distinct from its parent so the gRPC layer can tell it apart from the other reasons an index
 * is unavailable (initial sync, not yet ready to query). A peer may have parsed the same entry
 * fine, which is why search envoy is told {@code UNAVAILABLE} for this case alone.
 */
public class QuarantinedIndexException extends IndexUnavailableException {

  public QuarantinedIndexException(String message) {
    super(message);
  }
}
