package com.xgen.mongot.index;

import java.util.concurrent.CompletionException;

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

  /**
   * Preserves a quarantined-index failure for the gRPC layer to answer with {@code UNAVAILABLE} so
   * envoy retries a peer, when envoy is in the request path. Quarantine is node-local: a peer may
   * have parsed the same journal entry, and a rollout halts at the first decoupled node that has
   * not.
   */
  public static void rethrowIfEnvoyCanRetry(Throwable cause, boolean envoyInRequestPath) {
    if (cause instanceof QuarantinedIndexException && envoyInRequestPath) {
      throw new CompletionException(cause);
    }
  }
}
