package com.xgen.mongot.server.message;

/** Size limits applied to wire-protocol messages. */
public final class MessageLimits {

  private MessageLimits() {}

  // Max size in bytes of a single wire-protocol message on one connection, enforced on receive by
  // MessageDecoder.
  public static final int MAX_MESSAGE_SIZE_BYTES = 48_000_000;
}
