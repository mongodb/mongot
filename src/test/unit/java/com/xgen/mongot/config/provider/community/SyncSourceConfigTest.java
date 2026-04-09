package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.google.common.net.HostAndPort;
import com.mongodb.ReadPreference;
import com.xgen.mongot.util.mongodb.Databases;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class SyncSourceConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));

  private static ReplicaSetConfig replicaSet(Optional<MongoReadPreferenceName> readPreference) {
    return new ReplicaSetConfig(
        HOSTS,
        Optional.empty(),
        Optional.empty(),
        Databases.ADMIN,
        false,
        readPreference,
        Optional.empty());
  }

  private static SyncSourceConfig syncSource(
      Optional<MongoReadPreferenceName> replicaSetReadPreference,
      Optional<ReadPreferenceConfig> replicationReader) {
    return new SyncSourceConfig(
        replicaSet(replicaSetReadPreference),
        Optional.empty(),
        Optional.empty(),
        replicationReader);
  }

  @Test
  public void getReplicationReaderReadPreference_replicationReaderPresent_usesIt() {
    var config =
        syncSource(
            Optional.empty(),
            Optional.of(
                new ReadPreferenceConfig(MongoReadPreferenceName.NEAREST, Optional.empty())));
    assertEquals(ReadPreference.nearest(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void getReplicationReaderReadPreference_replicationReaderAbsent_fallsBackToReplicaSet() {
    var config = syncSource(Optional.of(MongoReadPreferenceName.PRIMARY), Optional.empty());
    assertEquals(ReadPreference.primary(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void getReplicationReaderReadPreference_neitherPresent_defaultsToSecondaryPreferred() {
    var config = syncSource(Optional.empty(), Optional.empty());
    assertEquals(ReadPreference.secondaryPreferred(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void getReplicationReaderReadPreference_bothSet_replicationReaderTakesPrecedence() {
    var config =
        syncSource(
            Optional.of(MongoReadPreferenceName.PRIMARY),
            Optional.of(
                new ReadPreferenceConfig(MongoReadPreferenceName.NEAREST, Optional.empty())));
    assertEquals(ReadPreference.nearest(), config.getReplicationReaderReadPreference());
  }
}
