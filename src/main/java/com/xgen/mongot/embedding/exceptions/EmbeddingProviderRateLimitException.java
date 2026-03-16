package com.xgen.mongot.embedding.exceptions;

/**
 * Exception thrown when the embedding provider returns a rate limit error (HTTP 429).
 *
 * <p>This exception is used to signal congestion to the AIMD congestion control algorithm. Unlike
 * other transient exceptions which may warrant immediate retry, rate limit exceptions should
 * trigger a multiplicative decrease in the congestion window before retrying.
 *
 * @see com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl
 */
public class EmbeddingProviderRateLimitException extends EmbeddingProviderTransientException {

  public EmbeddingProviderRateLimitException(String message) {
    super(message);
  }

  public EmbeddingProviderRateLimitException(Throwable throwable) {
    super(throwable);
  }
}
