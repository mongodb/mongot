package com.xgen.mongot.replication.mongodb.common;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.UuidRepresentation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      InitialSyncResumeInfoTest.DeserializationTest.class,
      InitialSyncResumeInfoTest.SerializationTest.class,
      InitialSyncResumeInfoTest.ParallelResumeTokenTest.class,
    })
public class InitialSyncResumeInfoTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "resume-info-deserialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";
    private static final BsonDeserializationTestSuite<Optional<InitialSyncResumeInfo>> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            SUITE_PATH, SUITE_NAME, InitialSyncResumeInfo::fromBson);

    private final TestSpecWrapper<Optional<InitialSyncResumeInfo>> testSpec;

    public DeserializationTest(TestSpecWrapper<Optional<InitialSyncResumeInfo>> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Optional<InitialSyncResumeInfo>>> data() {
      return TEST_SUITE.withExamples(
          empty(),
          collectionScan(),
          changeStream(),
          changeStreamWithoutResumeToken(),
          bufferless(),
          bufferlessNaturalOrder(),
          bufferlessParallelNaturalOrder());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid("empty", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        collectionScan() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection scan resume info", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        changeStream() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "change stream application resume info", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        changeStreamWithoutResumeToken() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "change stream application without batch resume token", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        bufferless() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless",
          Optional.of(
              new BufferlessIdOrderInitialSyncResumeInfo(
                  new BsonTimestamp(1234567890L), new BsonString("a"))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        bufferlessNaturalOrder() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferlessNaturalOrder",
          Optional.of(
              new BufferlessNaturalOrderInitialSyncResumeInfo(
                  new BsonTimestamp(1234567890L),
                  new BsonDocument()
                      .append("$recordId", new BsonInt64(1))
                      .append(
                          "$initialSyncId",
                          new BsonBinary(
                              UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                              UuidRepresentation.STANDARD)),
                  Optional.of("testHost"))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<InitialSyncResumeInfo>>
        bufferlessParallelNaturalOrder() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferlessParallelNaturalOrder", Optional.of(buildParallelExample()));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "resume-info-serialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";
    private static final BsonSerializationTestSuite<InitialSyncResumeInfo> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(SUITE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo>> data() {
      return List.of(bufferless(), bufferlessNaturalOrder(), bufferlessParallelNaturalOrder());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo> bufferless() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless",
          new BufferlessIdOrderInitialSyncResumeInfo(
              new BsonTimestamp(1234567890L), new BsonString("a")));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo>
        bufferlessNaturalOrder() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferlessNaturalOrder",
          new BufferlessNaturalOrderInitialSyncResumeInfo(
              new BsonTimestamp(1234567890L),
              new BsonDocument()
                  .append("$recordId", new BsonInt64(1))
                  .append(
                      "$initialSyncId",
                      new BsonBinary(
                          UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                          UuidRepresentation.STANDARD)),
              Optional.of("testHost")));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncResumeInfo>
        bufferlessParallelNaturalOrder() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferlessParallelNaturalOrder", buildParallelExample());
    }
  }

  /** Behavioral tests for the parallel variant that don't fit the BSON round-trip suites above. */
  @RunWith(JUnit4.class)
  public static class ParallelResumeTokenTest {

    /**
     * Returning a partial token instead would look like a natural-order token missing {@code
     * $recordId}, which a single-cursor caller would accept and then scan from the wrong position.
     */
    @Test
    public void getResumeToken_throws() {
      BufferlessParallelNaturalOrderInitialSyncResumeInfo resumeInfo = buildParallelExample();

      Assert.assertThrows(UnsupportedOperationException.class, resumeInfo::getResumeToken);
    }

    /**
     * The parallel variant must not answer to the single-cursor natural-order predicate. Callers
     * use that one to decide it is safe to read a resume token.
     */
    @Test
    public void onlyTheParallelPredicateMatchesTheParallelVariant() {
      InitialSyncResumeInfo parallel = buildParallelExample();
      InitialSyncResumeInfo idOrder =
          new BufferlessIdOrderInitialSyncResumeInfo(new BsonTimestamp(1L), new BsonInt64(7));
      InitialSyncResumeInfo naturalOrder =
          new BufferlessNaturalOrderInitialSyncResumeInfo(
              new BsonTimestamp(1L), new BsonDocument(), Optional.empty());

      Assert.assertTrue(parallel.isBufferlessParallelNaturalOrderInitialSyncResumeInfo());
      Assert.assertFalse(parallel.isBufferlessNaturalOrderInitialSyncResumeInfo());
      Assert.assertFalse(parallel.isBufferlessIdOrderInitialSyncResumeInfo());

      Assert.assertFalse(idOrder.isBufferlessParallelNaturalOrderInitialSyncResumeInfo());
      Assert.assertFalse(naturalOrder.isBufferlessParallelNaturalOrderInitialSyncResumeInfo());
    }

    /** The accessors that replace {@code getResumeToken()} keep working. */
    @Test
    public void perPartitionAccessorsRemainAvailable() {
      BufferlessParallelNaturalOrderInitialSyncResumeInfo resumeInfo = buildParallelExample();

      Assert.assertEquals(new BsonTimestamp(1234567890L), resumeInfo.getResumeOperationTime());
      Assert.assertEquals(4, resumeInfo.getPartitions().size());
      Assert.assertEquals(
          UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"), resumeInfo.getInitialSyncId());
    }
  }

  /**
   * K=4 mid-flight example covering every combination of empty bounds and progress: first partition
   * (no lower) in flight, a finished middle one, a middle one with no progress yet, and the last
   * partition (no upper).
   */
  private static BufferlessParallelNaturalOrderInitialSyncResumeInfo buildParallelExample() {
    List<PartitionResumeInfo> partitions = new ArrayList<>(4);
    partitions.add(
        new PartitionResumeInfo(0, Optional.empty(), Optional.of(1000L), Optional.of(950L), false));
    partitions.add(
        new PartitionResumeInfo(
            1, Optional.of(1000L), Optional.of(2000L), Optional.of(2000L), true));
    partitions.add(
        new PartitionResumeInfo(
            2, Optional.of(2000L), Optional.of(3000L), Optional.empty(), false));
    partitions.add(
        new PartitionResumeInfo(
            3, Optional.of(3000L), Optional.empty(), Optional.of(3500L), false));
    return new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
        new BsonTimestamp(1234567890L),
        UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
        partitions,
        Optional.of("testHost"));
  }
}
