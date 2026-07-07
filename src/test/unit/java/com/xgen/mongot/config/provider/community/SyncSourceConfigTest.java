package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.google.common.net.HostAndPort;
import com.mongodb.ReadPreference;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class SyncSourceConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));

  private static ReplicaSetConfig replicaSet() {
    return new ReplicaSetConfig(HOSTS, Optional.empty(), Optional.empty(), Optional.empty());
  }

  private static SyncSourceConfig syncSource(Optional<ReadPreferenceConfig> replicationReader) {
    return new SyncSourceConfig(replicaSet(), Optional.empty(), replicationReader);
  }

  @Test
  public void getReplicationReaderReadPreference_replicationReaderPresent_usesIt() {
    var config =
        syncSource(
            Optional.of(
                new ReadPreferenceConfig(MongoReadPreferenceName.NEAREST, Optional.empty())));
    assertEquals(ReadPreference.nearest(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void
      getReplicationReaderReadPreference_replicationReaderAbsent_defaultsToSecondaryPreferred() {
    var config = syncSource(Optional.empty());
    assertEquals(ReadPreference.secondaryPreferred(), config.getReplicationReaderReadPreference());
  }
}
