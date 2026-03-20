package com.xgen.mongot.cursor.serialization;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.cursor.serialization.MongotCursorResultBuilder;
import com.xgen.testing.mongot.index.SearchResultBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MongotCursorResultTest.DeserializationTest.class,
      MongotCursorResultTest.SerializationTest.class
    })
public class MongotCursorResultTest {

  static final String RESOURCES_PATH = "src/test/unit/resources/cursor/serialization";

  private static List<BsonValue> makeBatch(int size) {
    List<BsonValue> results = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      results.add(
          SearchResultBuilder.builder().id(new BsonInt32(i)).score((float) i).build().toBson());
    }

    return results;
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "mongot-cursor-result-deserialization";

    private static final BsonDeserializationTestSuite<MongotCursorResult> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, MongotCursorResult::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorResult> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorResult> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorResult>>
        data() {
      return TEST_SUITE.withExamples(simple(), emptyBatch());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<MongotCursorResult> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          MongotCursorResultBuilder.builder()
              .id(1L)
              .namespace("foo.bar")
              .nextBatch(makeBatch(2))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<MongotCursorResult> emptyBatch() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "emptyBatch",
          MongotCursorResultBuilder.builder()
              .id(1L)
              .namespace("foo.bar")
              .nextBatch(Collections.emptyList())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "mongot-cursor-result-serialization";
    private static final BsonSerializationTestSuite<MongotCursorResult> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<MongotCursorResult> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<MongotCursorResult> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<MongotCursorResult>> data() {
      return List.of(simple(), emptyBatch());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorResult> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          MongotCursorResultBuilder.builder()
              .id(1L)
              .namespace("foo.bar")
              .nextBatch(makeBatch(2))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorResult> emptyBatch() {
      return BsonSerializationTestSuite.TestSpec.create(
          "emptyBatch",
          MongotCursorResultBuilder.builder()
              .id(1L)
              .namespace("foo.bar")
              .nextBatch(Collections.emptyList())
              .build());
    }
  }
}
