package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.junit.Test;

public class InitialSyncConfigMapperTest {

  private static CommunityReplicationConfig replicationWithNaturalOrderScan(
      Optional<Boolean> value) {
    return new CommunityReplicationConfig(
        Optional.empty(),
        Optional.of(
            new CommunityReplicationConfig.InitialSyncConfig(
                Optional.of(
                    new CommunityReplicationConfig.InitialSyncConfig.BufferlessInitialSyncConfig(
                        value)))));
  }

  @Test
  public void absentReplicationConfig_defaultsToFalse() {
    assertFalse(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.empty()).enableNaturalOrderScan());
  }

  @Test
  public void absentInitialSyncBlock_defaultsToFalse() {
    var replicationConfig = new CommunityReplicationConfig(Optional.empty(), Optional.empty());
    assertFalse(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.of(replicationConfig))
            .enableNaturalOrderScan());
  }

  @Test
  public void absentBufferlessBlock_defaultsToFalse() {
    var replicationConfig =
        new CommunityReplicationConfig(
            Optional.empty(),
            Optional.of(new CommunityReplicationConfig.InitialSyncConfig(Optional.empty())));
    assertFalse(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.of(replicationConfig))
            .enableNaturalOrderScan());
  }

  @Test
  public void absentEnableNaturalOrderScanField_defaultsToFalse() {
    var replicationConfig = replicationWithNaturalOrderScan(Optional.empty());
    assertFalse(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.of(replicationConfig))
            .enableNaturalOrderScan());
  }

  @Test
  public void enableNaturalOrderScanTrue_isPropagated() {
    var replicationConfig = replicationWithNaturalOrderScan(Optional.of(true));
    assertTrue(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.of(replicationConfig))
            .enableNaturalOrderScan());
  }

  @Test
  public void enableNaturalOrderScanFalse_isPropagated() {
    var replicationConfig = replicationWithNaturalOrderScan(Optional.of(false));
    assertFalse(
        InitialSyncConfigMapper.toInitialSyncConfig(Optional.of(replicationConfig))
            .enableNaturalOrderScan());
  }

  @Test
  public void fcisResyncImprovement_staysOffForCommunity() {
    var config =
        InitialSyncConfigMapper.toInitialSyncConfig(
            Optional.of(replicationWithNaturalOrderScan(Optional.of(true))));
    // FCIS is not supported in Community now.
    assertFalse(config.avoidNaturalOrderScanSyncSourceChangeResync());
  }
}
