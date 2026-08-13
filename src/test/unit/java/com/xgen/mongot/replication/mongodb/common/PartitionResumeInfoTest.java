package com.xgen.mongot.replication.mongodb.common;

import com.google.common.testing.EqualsTester;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the BSON round trip across the combinations of absent bounds and absent progress that a
 * real partition set contains. Entries are not self-validating, so there is nothing to reject here:
 * whether a set of them describes a collection is up to the partition planner.
 */
public class PartitionResumeInfoTest {

  private static PartitionResumeInfo middlePartition(Optional<Long> resumeRecordId) {
    return new PartitionResumeInfo(
        1, Optional.of(1000L), Optional.of(2000L), resumeRecordId, false);
  }

  private static PartitionResumeInfo deserialize(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return PartitionResumeInfo.deserialize(parser);
    }
  }

  private static void assertRoundTrips(PartitionResumeInfo partition) throws Exception {
    Assert.assertEquals(partition, deserialize(partition.toBson()));
  }

  @Test
  public void roundTripsAFullyBoundedPartitionWithProgress() throws Exception {
    assertRoundTrips(middlePartition(Optional.of(1500L)));
  }

  /** The first partition has no lower bound, the last no upper, and neither may have progress. */
  @Test
  public void roundTripsAPartitionWithoutBoundsOrProgress() throws Exception {
    assertRoundTrips(
        new PartitionResumeInfo(0, Optional.empty(), Optional.empty(), Optional.empty(), false));
  }

  /** A partition whose range held no documents consumes it without recording a position. */
  @Test
  public void roundTripsADonePartitionWithoutProgress() throws Exception {
    assertRoundTrips(
        new PartitionResumeInfo(1, Optional.of(1000L), Optional.of(2000L), Optional.empty(), true));
  }

  @Test
  public void accessorsReportWhatWasConstructed() {
    PartitionResumeInfo partition = middlePartition(Optional.of(1500L));

    Assert.assertEquals(1, partition.getPartitionId());
    Assert.assertEquals(Optional.of(1000L), partition.getLowerRecordId());
    Assert.assertEquals(Optional.of(2000L), partition.getUpperRecordId());
    Assert.assertEquals(Optional.of(1500L), partition.getResumeRecordId());
    Assert.assertFalse(partition.isDone());
  }

  @Test
  public void equalsAndHashCodeCoverEveryField() {
    new EqualsTester()
        .addEqualityGroup(middlePartition(Optional.of(1500L)), middlePartition(Optional.of(1500L)))
        .addEqualityGroup(middlePartition(Optional.of(1600L)))
        .addEqualityGroup(middlePartition(Optional.empty()))
        .addEqualityGroup(
            new PartitionResumeInfo(
                2, Optional.of(1000L), Optional.of(2000L), Optional.of(1500L), false))
        .addEqualityGroup(
            new PartitionResumeInfo(
                1, Optional.of(1000L), Optional.of(2000L), Optional.of(1500L), true))
        .testEquals();
  }
}
