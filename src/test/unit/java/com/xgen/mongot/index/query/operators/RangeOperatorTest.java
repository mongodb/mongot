package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RangeOperatorTest {

  private static final String SUITE_NAME = "range";
  private static final BsonDeserializationTestSuite<RangeOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, RangeOperator::fromBson);

  private final TestSpecWrapper<RangeOperator> testSpec;

  public RangeOperatorTest(TestSpecWrapper<RangeOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<RangeOperator>> data() {
    return TEST_SUITE.withExamples(
        doesNotAffect(),
        doesNotAffectString(),
        dateBounds(),
        longBounds(),
        doubleBounds(),
        objectIdBounds(),
        uuidBounds(),
        stringBounds(),
        withBoostScore(),
        ltAndGt(),
        ltAndGte(),
        lteAndGt(),
        lteAndGte(),
        lteAndGteSameValue());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<RangeOperator> doesNotAffect() {
    return TestSpec.valid(
        "doesNotAffect",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(Optional.empty(), Optional.of(new LongPoint(15L)), false, false)
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<RangeOperator> doesNotAffectString() {
    return TestSpec.valid(
        "doesNotAffectString",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(Optional.empty(), Optional.of(new LongPoint(15L)), false, false)
            .doesNotAffect("points")
            .build());
  }

  private static ValidSpec<RangeOperator> dateBounds() {
    return TestSpec.valid(
        "date bounds",
        OperatorBuilder.range()
            .path("point")
            .dateBounds(
                Optional.empty(),
                Optional.of(
                    new DatePoint(
                        new Calendar.Builder()
                            .setDate(2020, Calendar.APRIL, 3)
                            .setTimeOfDay(13, 13, 13, 0)
                            .setTimeZone(TimeZone.getTimeZone("UTC"))
                            .build()
                            .getTime())),
                false,
                false)
            .build());
  }

  private static ValidSpec<RangeOperator> longBounds() {
    return TestSpec.valid(
        "long bounds",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(Optional.empty(), Optional.of(new LongPoint(13L)), false, false)
            .build());
  }

  private static ValidSpec<RangeOperator> doubleBounds() {
    return TestSpec.valid(
        "double bounds",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(Optional.empty(), Optional.of(new DoublePoint(13d)), false, false)
            .build());
  }

  private static ValidSpec<RangeOperator> objectIdBounds() {
    return TestSpec.valid(
        "objectId bounds",
        OperatorBuilder.range()
            .path("point")
            .objectIdBounds(
                Optional.of(new ObjectIdPoint(new ObjectId("000000000000000000000001"))),
                Optional.of(new ObjectIdPoint(new ObjectId("000000000000000000000004"))),
                false,
                false)
            .build());
  }

  private static ValidSpec<RangeOperator> uuidBounds() {
    return TestSpec.valid(
        "uuid bounds",
        OperatorBuilder.range()
            .path("point")
            .uuidBounds(
                Optional.of(new UuidPoint(UUID.fromString("00000000-0000-0000-0000-000000000001"))),
                Optional.of(new UuidPoint(UUID.fromString("00000000-0000-0000-0000-000000000004"))),
                false,
                false)
            .build());
  }

  private static ValidSpec<RangeOperator> stringBounds() {
    return TestSpec.valid(
        "string bounds",
        OperatorBuilder.range()
            .path("point")
            .stringBounds(
                Optional.of(new StringPoint("a")), Optional.of(new StringPoint("z")), false, false)
            .build());
  }

  private static ValidSpec<RangeOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(Optional.empty(), Optional.of(new DoublePoint(13d)), false, false)
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<RangeOperator> ltAndGt() {
    return TestSpec.valid(
        "lt and gt",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(
                Optional.of(new DoublePoint(13d)), Optional.of(new DoublePoint(26d)), false, false)
            .build());
  }

  private static ValidSpec<RangeOperator> ltAndGte() {
    return TestSpec.valid(
        "lt and gte",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(
                Optional.of(new DoublePoint(13d)), Optional.of(new DoublePoint(26d)), true, false)
            .build());
  }

  private static ValidSpec<RangeOperator> lteAndGt() {
    return TestSpec.valid(
        "lte and gt",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(
                Optional.of(new DoublePoint(13d)), Optional.of(new DoublePoint(26d)), false, true)
            .build());
  }

  private static ValidSpec<RangeOperator> lteAndGte() {
    return TestSpec.valid(
        "lte and gte",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(
                Optional.of(new DoublePoint(13d)), Optional.of(new DoublePoint(26d)), true, true)
            .build());
  }

  private static ValidSpec<RangeOperator> lteAndGteSameValue() {
    return TestSpec.valid(
        "lte and gte same value",
        OperatorBuilder.range()
            .path("point")
            .numericBounds(
                Optional.of(new DoublePoint(13d)), Optional.of(new DoublePoint(13d)), true, true)
            .build());
  }
}
