package com.xgen.mongot.replication.mongodb.common;

/**
 * NamespaceResolutionException is a checked exception used to wrap any expected exceptions or
 * failure modes during namespace resolution and classify the type of error. It is used when the
 * collection that is being resolved does not exist.
 */
public class NamespaceResolutionException extends Exception {

  private NamespaceResolutionException(Exception cause) {
    super(cause);
  }

  private NamespaceResolutionException() {}

  public static NamespaceResolutionException create() {
    return new NamespaceResolutionException();
  }

  public static NamespaceResolutionException createWithCause(Exception cause) {
    return new NamespaceResolutionException(cause);
  }
}
