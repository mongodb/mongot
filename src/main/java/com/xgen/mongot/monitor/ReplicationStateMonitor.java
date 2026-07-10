package com.xgen.mongot.monitor;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ReplicationStateMonitor {
  private final Gate initialSyncGate;
  private final Gate replicationGate;
  private final Gate autoEmbeddingReplicationGate;

  private ReplicationStateMonitor(
      Gate initialSyncGate, Gate replicationGate, Gate autoEmbeddingReplicationGate) {
    this.initialSyncGate = initialSyncGate;
    this.replicationGate = replicationGate;
    this.autoEmbeddingReplicationGate = autoEmbeddingReplicationGate;
  }

  public Gate getInitialSyncGate() {
    return this.initialSyncGate;
  }

  public Gate getReplicationGate() {
    return this.replicationGate;
  }

  public Gate getAutoEmbeddingReplicationGate() {
    return this.autoEmbeddingReplicationGate;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ReplicationStateMonitor enabled() {
    return new ReplicationStateMonitor(
        ToggleGate.opened(), ToggleGate.opened(), ToggleGate.opened());
  }

  public static ReplicationStateMonitor disabled() {
    return new ReplicationStateMonitor(
        ToggleGate.closed(), ToggleGate.closed(), ToggleGate.closed());
  }

  public static ReplicationStateMonitor diskBased(
      HysteresisConfig replicationConfig,
      Optional<HysteresisConfig> initialSyncConfig,
      DiskMonitor diskMonitor) {
    var replicationGate = registeredHysteresisGate(replicationConfig, diskMonitor);
    Gate initialSyncGate =
        initialSyncConfig
            .<Gate>map(config -> registeredHysteresisGate(config, diskMonitor))
            .orElseGet(ToggleGate::opened);
    return builder()
        .setReplicationGate(replicationGate)
        .setInitialSyncGate(initialSyncGate)
        .build();
  }

  /**
   * Disk-based monitor that also gates auto-embedding materialized-view replication. Auto-embedding
   * is gated by a {@link HysteresisGate} registered to {@code autoEmbeddingDiskMonitor} using
   * {@code autoEmbeddingConfig}; when either is empty, auto-embedding replication is left ungated
   * (open). The caller selects the disk monitor and thresholds for the node's topology.
   */
  public static ReplicationStateMonitor diskBased(
      HysteresisConfig replicationConfig,
      Optional<HysteresisConfig> initialSyncConfig,
      Optional<HysteresisConfig> autoEmbeddingConfig,
      DiskMonitor sharedDiskMonitor,
      Optional<DiskMonitor> autoEmbeddingDiskMonitor) {
    var replicationGate = registeredHysteresisGate(replicationConfig, sharedDiskMonitor);
    Gate initialSyncGate =
        initialSyncConfig
            .<Gate>map(config -> registeredHysteresisGate(config, sharedDiskMonitor))
            .orElseGet(ToggleGate::opened);
    Gate autoEmbeddingReplicationGate =
        autoEmbeddingReplicationGate(autoEmbeddingConfig, autoEmbeddingDiskMonitor);
    return builder()
        .setReplicationGate(replicationGate)
        .setInitialSyncGate(initialSyncGate)
        .setAutoEmbeddingReplicationGate(autoEmbeddingReplicationGate)
        .build();
  }

  private static Gate autoEmbeddingReplicationGate(
      Optional<HysteresisConfig> autoEmbeddingConfig,
      Optional<DiskMonitor> autoEmbeddingDiskMonitor) {
    if (autoEmbeddingConfig.isEmpty() || autoEmbeddingDiskMonitor.isEmpty()) {
      return ToggleGate.opened();
    }
    return registeredHysteresisGate(autoEmbeddingConfig.get(), autoEmbeddingDiskMonitor.get());
  }

  private static HysteresisGate registeredHysteresisGate(
      HysteresisConfig config, DiskMonitor diskMonitor) {
    var gate = new HysteresisGate(config.openThreshold(), config.closeThreshold());
    diskMonitor.register(gate);
    return gate;
  }

  public static class Builder {
    Optional<Gate> initialSyncGate = Optional.empty();
    Optional<Gate> replicationGate = Optional.empty();
    Gate autoEmbeddingReplicationGate = ToggleGate.opened();

    public Builder setInitialSyncGate(Gate gate) {
      this.initialSyncGate = Optional.of(gate);
      return this;
    }

    public Builder setReplicationGate(Gate gate) {
      this.replicationGate = Optional.of(gate);
      return this;
    }

    public Builder setAutoEmbeddingReplicationGate(Gate gate) {
      this.autoEmbeddingReplicationGate = gate;
      return this;
    }

    public ReplicationStateMonitor build() {
      var initialSyncGate = Check.isPresent(this.initialSyncGate, "initialSyncGate");
      var replicationGate = Check.isPresent(this.replicationGate, "replicationGate");
      return new ReplicationStateMonitor(
          initialSyncGate, replicationGate, this.autoEmbeddingReplicationGate);
    }
  }
}
