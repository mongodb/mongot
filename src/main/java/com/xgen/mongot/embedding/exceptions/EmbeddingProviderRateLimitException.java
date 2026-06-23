package com.xgen.mongot.embedding.exceptions;

import com.xgen.mongot.util.UserFacingException;

/**
 * Exception thrown when the embedding provider returns a rate limit error (HTTP 429).
 *
 * <p>This exception is used to signal congestion to the AIMD congestion control algorithm. Unlike
 * other transient exceptions which may warrant immediate retry, rate limit exceptions should
 * trigger a multiplicative decrease in the congestion window before retrying.
 *
 * <p>Implements {@link UserFacingException} so the rate-limit failure is surfaced to the customer.
 *
 * @see com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl
 */
public class EmbeddingProviderRateLimitException extends EmbeddingProviderTransientException
    implements UserFacingException {

  public EmbeddingProviderRateLimitException(String message) {
    super(message, Reason.RATE_LIMIT_EXCEEDED);
  }

  public EmbeddingProviderRateLimitException(Throwable throwable) {
    super(throwable, Reason.RATE_LIMIT_EXCEEDED);
  }
}
