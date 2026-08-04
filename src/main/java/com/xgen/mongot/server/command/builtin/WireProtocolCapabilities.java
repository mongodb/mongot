package com.xgen.mongot.server.command.builtin;

import com.mongodb.AuthenticationMechanism;
import java.util.List;

/** Limits and capabilities advertised to clients via the hello/isMaster handshake. */
public final class WireProtocolCapabilities {

  private WireProtocolCapabilities() {}

  public static final int MAX_BSON_OBJECT_SIZE = 16_777_216;
  public static final int MAX_WRITE_BATCH_SIZE = 100_000;

  // Mongo server uses wire version to determine if driver/mongod/mongos can interact,
  // It represents the message syntax and logical capabilities. See HELP-22883 for more information.
  // Setting the range to [0, max int) means that any client can interact with mongot. The changes
  // in the mongod-mongot protocol are not guarded by this protocol version. So this constraint
  // is mostly irrelevant for mongot.
  public static final int MIN_WIRE_VERSION = 0;
  public static final int MAX_WIRE_VERSION = Integer.MAX_VALUE - 1;

  public static final List<AuthenticationMechanism> SASL_SUPPORTED_MECHANISMS =
      List.of(AuthenticationMechanism.SCRAM_SHA_1, AuthenticationMechanism.SCRAM_SHA_256);
}
