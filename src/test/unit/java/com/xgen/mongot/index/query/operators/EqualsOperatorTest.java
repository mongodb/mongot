package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EqualsOperatorTest {
  private static final String SUITE_NAME = "equals";
  private static final BsonDeserializationTestSuite<EqualsOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, EqualsOperator::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<EqualsOperator> testSpec;

  public EqualsOperatorTest(BsonDeserializationTestSuite.TestSpecWrapper<EqualsOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<EqualsOperator>> data() {
    return TEST_SUITE.withExamples(
        doesNotAffect(),
        doesNotAffectString(),
        booleanSimple(),
        booleanWithScore(),
        objectIdSimple(),
        objectIdWithScore(),
        numberDoubleSimple(),
        numberLongSimple(),
        numberDouble15Decimal(),
        numberDouble18Decimal(),
        numberWithScore(),
        dateSimple(),
        dateWithScore(),
        uuidSimple(),
        uuidWithScore(),
        nullSimple(),
        nullWithScore(),
        stringSimple(),
        stringWithScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> doesNotAffect() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "doesNotAffect",
        OperatorBuilder.equals()
            .path("point")
            .value(15L)
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> doesNotAffectString() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "doesNotAffectString",
        OperatorBuilder.equals().path("point").value(15L).doesNotAffect("points").build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> booleanSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "boolean-simple", OperatorBuilder.equals().path("a").value(true).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> booleanWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "boolean-with-score",
        OperatorBuilder.equals()
            .path("a")
            .value(false)
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> objectIdSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "objectId-simple",
        OperatorBuilder.equals().path("a").value(new ObjectId("507f1f77bcf86cd799439011")).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> objectIdWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "objectId-with-score",
        OperatorBuilder.equals()
            .path("a")
            .value(new ObjectId("507f1f77bcf86cd799439011"))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> numberLongSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberLong-simple", OperatorBuilder.equals().path("a").value(2).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> numberDoubleSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-simple", OperatorBuilder.equals().path("a").value(2.01).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> numberDouble15Decimal() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-15decimal",
        OperatorBuilder.equals().path("a").value(2.151515151515151).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> numberDouble18Decimal() {
    // this has 16 decimals, but the corresponding bson test has 18 decimals
    // java double loses precision after 15 decimal places
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-18decimal",
        OperatorBuilder.equals().path("a").value(2.1515151515151515).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> numberWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "number-with-score",
        OperatorBuilder.equals()
            .path("a")
            .value(2)
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> dateSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date-simple",
        OperatorBuilder.equals()
            .path("a")
            .value(
                new Calendar.Builder()
                    .setDate(2022, Calendar.DECEMBER, 6)
                    .setTimeOfDay(13, 13, 13)
                    .setTimeZone(TimeZone.getTimeZone("UTC"))
                    .build()
                    .getTime())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> dateWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date-with-score",
        OperatorBuilder.equals()
            .path("a")
            .value(
                new Calendar.Builder()
                    .setDate(2022, Calendar.DECEMBER, 6)
                    .setTimeOfDay(13, 13, 13)
                    .setTimeZone(TimeZone.getTimeZone("UTC"))
                    .build()
                    .getTime())
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> uuidSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "uuid-simple",
        OperatorBuilder.equals()
            .path("a")
            .uuidValue("00000000-1111-2222-3333-444444444444")
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> uuidWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "uuid-with-score",
        OperatorBuilder.equals()
            .path("a")
            .uuidValue("00000000-1111-2222-3333-444444444444")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> nullSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "null-simple", OperatorBuilder.equals().path("a").nullValue().build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> nullWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "null-with-score",
        OperatorBuilder.equals()
            .path("a")
            .nullValue()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> stringSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "string-simple", OperatorBuilder.equals().path("a").value("test").build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<EqualsOperator> stringWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "string-with-score",
        OperatorBuilder.equals()
            .path("a")
            .value("test")
            .score(ScoreBuilder.valueBoost().value(10).build())
            .build());
  }
}
