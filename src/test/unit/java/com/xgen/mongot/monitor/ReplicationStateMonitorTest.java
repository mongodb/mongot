package com.xgen.mongot.monitor;

import com.xgen.mongot.config.util.HysteresisConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationStateMonitorTest {

  @Test
  public void diskBased_emptyAutoEmbeddingConfigLeavesAutoEmbeddingUngated() {
    var diskMonitor = new TestDiskMonitor();
    var monitor =
        ReplicationStateMonitor.diskBased(
            new HysteresisConfig(0.5, 0.7),
            Optional.empty(),
            Optional.empty(),
            diskMonitor,
            Optional.empty());

    diskMonitor.update(1.0);

    Assert.assertTrue(monitor.getReplicationGate().isClosed());
    Assert.assertTrue(monitor.getAutoEmbeddingReplicationGate().isOpen());
  }

  @Test
  public void diskBased_autoEmbeddingConfigUsesSelectedDiskMonitor() {
    var diskMonitor = new TestDiskMonitor();
    var autoEmbeddingDiskMonitor = new TestDiskMonitor();
    var monitor =
        ReplicationStateMonitor.diskBased(
            new HysteresisConfig(0.5, 0.7),
            Optional.empty(),
            Optional.of(new HysteresisConfig(0.3, 0.4)),
            diskMonitor,
            Optional.of(autoEmbeddingDiskMonitor));

    diskMonitor.update(1.0);

    Assert.assertTrue(monitor.getReplicationGate().isClosed());
    Assert.assertTrue(monitor.getAutoEmbeddingReplicationGate().isOpen());

    autoEmbeddingDiskMonitor.update(1.0);

    Assert.assertTrue(monitor.getReplicationGate().isClosed());
    Assert.assertTrue(monitor.getAutoEmbeddingReplicationGate().isClosed());
  }

  private static class TestDiskMonitor implements DiskMonitor {
    private final List<Gate> gates = new ArrayList<>();

    @Override
    public void register(Gate gate) {
      this.gates.add(gate);
    }

    @Override
    public void start(Duration diskMonitorDuration) {}

    @Override
    public void stop() {}

    void update(double usage) {
      for (Gate gate : this.gates) {
        gate.update(usage);
      }
    }
  }
}
