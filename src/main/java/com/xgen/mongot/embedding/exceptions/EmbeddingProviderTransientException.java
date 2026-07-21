package com.xgen.mongot.embedding.exceptions;

public class EmbeddingProviderTransientException extends RuntimeException {
  private final Reason reason;

  public EmbeddingProviderTransientException(String message) {
    super(message);
    this.reason = Reason.UNKNOWN;
  }

  public EmbeddingProviderTransientException(Throwable throwable) {
    super(throwable);
    this.reason = Reason.UNKNOWN;
  }

  public EmbeddingProviderTransientException(String message, Reason reason) {
    super(message);
    this.reason = reason == null ? Reason.UNKNOWN : reason;
  }

  public EmbeddingProviderTransientException(Throwable throwable, Reason reason) {
    super(throwable);
    this.reason = reason == null ? Reason.UNKNOWN : reason;
  }

  public Reason getReason() {
    return this.reason;
  }

  /** Categorizes the cause of a transient embedding provider failure. */
  public enum Reason {
    /** Embedding service not yet initialized when vector search is requested. */
    SERVICE_NOT_INITIALIZED,
    /** Provider returned HTTP 429 or client-side rate limit triggered. */
    RATE_LIMIT_EXCEEDED,
    /** Failed to acquire the AIMD congestion-control semaphore (interrupted during acquire). */
    CLIENT_SIDE_CONGESTION_CONTROL,
    /** Malformed HTTP request (IllegalArgumentException from HttpClient). */
    HTTP_REQUEST_ERROR,
    /** HTTP request timed out (HttpTimeoutException or HTTP 408). */
    HTTP_TIMEOUT,
    /** Thread interrupted while waiting for HTTP response. */
    THREAD_INTERRUPTED,
    /** I/O error during HTTP communication (connection failure, SSL error, etc.). */
    HTTP_IO_EXCEPTION,
    /** Unable to extract tenant ID or find credentials for MTM cluster. */
    TENANT_CREDENTIALS_FAILURE,
    /** Provider returned a non-OK HTTP status (> 400, excluding 408/429). */
    HTTP_NON_OK_STATUS,
    /** Provider returned HTTP 400 for a reason other than an oversized batch. */
    HTTP_INVALID_REQUEST,
    /** Failed to parse the provider's response body (BsonParseException). */
    RESPONSE_PARSE_ERROR,
    /** Default for legacy constructors or unclassified transient failures. */
    UNKNOWN,
  }
}
