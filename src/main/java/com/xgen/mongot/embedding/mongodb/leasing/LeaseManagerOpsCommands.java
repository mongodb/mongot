package com.xgen.mongot.embedding.mongodb.leasing;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregates ops commands for the lease manager (e.g. give-up lease for rebalance). Extensible for
 * future command types.
 *
 * <p>{@link DynamicLeaderLeaseManager} stores this payload at construction (without executing it)
 * and applies it from {@link LeaseManager#executeOpsCommandsAfterInitializeLease(String)} once the
 * lease client and in-memory lease state for a materialized view are available.
 */
public record LeaseManagerOpsCommands(
    Optional<OpsGiveUpLeaseCommand> opsGiveUpLease,
    Optional<OpsRepairLeaseCommand> opsRepairLease) {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseManagerOpsCommands.class);

  /** No ops command to apply. */
  public static final LeaseManagerOpsCommands NONE =
      new LeaseManagerOpsCommands(Optional.empty(), Optional.empty());

  /**
   * Creates from raw config values. Used to avoid circular dependency with mms package.
   *
   * @param instance the instance name from config
   * @param leaseNames the lease names from config
   * @param expiresAtStr the expiresAt string from config (ISO-8601 or epoch ms)
   * @return LeaseManagerOpsCommands with the parsed command, or NONE if invalid
   */
  public static LeaseManagerOpsCommands create(
      String instance, List<String> leaseNames, String expiresAtStr) {
    Optional<OpsGiveUpLeaseCommand> cmd =
        getOpsGiveUpLeaseCommand(instance, leaseNames, expiresAtStr);
    return cmd.isEmpty() ? NONE : new LeaseManagerOpsCommands(cmd, Optional.empty());
  }

  /**
   * Creates from already-parsed commands. The bootstrapper uses this so both commands can be
   * supplied together; either may be empty.
   */
  public static LeaseManagerOpsCommands create(
      Optional<OpsGiveUpLeaseCommand> giveUpLease, Optional<OpsRepairLeaseCommand> repairLease) {
    return giveUpLease.isEmpty() && repairLease.isEmpty()
        ? NONE
        : new LeaseManagerOpsCommands(giveUpLease, repairLease);
  }

  /**
   * Builds the repair-lease command from raw config values, or empty when {@code expiresAtStr} is
   * unparseable. Used to avoid a circular dependency with the mms package.
   */
  public static Optional<OpsRepairLeaseCommand> getOpsRepairLeaseCommand(
      String instance, List<String> leaseIds, String expiresAtStr) {
    return parseExpiresAt(expiresAtStr)
        .map(exp -> new OpsRepairLeaseCommand(instance, leaseIds, exp));
  }

  /**
   * Builds the ops give-up lease command from config values, or empty when {@code expiresAtStr} is
   * unparseable. Used to avoid a circular dependency with the mms package.
   */
  public static Optional<OpsGiveUpLeaseCommand> getOpsGiveUpLeaseCommand(
      String instance, List<String> leaseNames, String expiresAtStr) {
    return parseExpiresAt(expiresAtStr)
        .map(exp -> new OpsGiveUpLeaseCommand(instance, leaseNames, exp));
  }

  /**
   * Parses expiresAt from config: ISO-8601 string or epoch milliseconds string.
   *
   * @return Optional of Instant, or empty if unparseable
   */
  @VisibleForTesting
  static Optional<Instant> parseExpiresAt(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Instant.parse(value));
    } catch (DateTimeParseException e) {
      try {
        return Optional.of(Instant.ofEpochMilli(Long.parseLong(value.trim())));
      } catch (NumberFormatException numberFormatException) {
        // Shared by both commands, so the message must not name one of them. An operator following
        // either runbook greps for this line.
        LOG.atError()
            .addKeyValue("expiresAt", value)
            .log(
                "Could not parse ops command expiresAt (expected ISO-8601 or epoch ms); "
                    + "ignoring the command");
        return Optional.empty();
      }
    }
  }

  /**
   * Ops command to give up specific leases (e.g. for rebalancing). Passed at bootstrap from
   * MmsConfig; only applied when instance matches self and now <= expiresAt.
   */
  public record OpsGiveUpLeaseCommand(
      String instance, List<String> leaseNames, Instant expiresAt) {}

  /**
   * Ops command for auto-embedding: delete lease documents the load path rejects as unusable, so
   * the next lease initialization recreates them and the materialized view stops being blocked.
   * Only applied when instance matches self and now <= expiresAt.
   *
   * <p>The delete does not require this instance to have recorded the lease during an earlier scan.
   * The caller names an explicit lease id, so the guarantee is the re-check at delete time: the
   * document is re-read and the load path re-run against it, and the delete proceeds only on a
   * positive re-rejection, past expiry, with a matching leaseVersion.
   *
   * <p>Deleting a lease discards its replication checkpoint, so the materialized view is rebuilt
   * from scratch. See docs/internal/autoembedding/ops/repair-corrupt-lease-runbook.md.
   */
  public record OpsRepairLeaseCommand(String instance, List<String> leaseIds, Instant expiresAt) {}
}
