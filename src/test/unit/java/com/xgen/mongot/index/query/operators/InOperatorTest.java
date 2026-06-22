package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.text.ParseException;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class InOperatorTest {
  private static final String SUITE_NAME = "in";
  private static final BsonDeserializationTestSuite<InOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, InOperator::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<InOperator> testSpec;

  public InOperatorTest(BsonDeserializationTestSuite.TestSpecWrapper<InOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<InOperator>> data()
      throws ParseException {
    return TEST_SUITE.withExamples(
        doesNotAffect(),
        doesNotAffectString(),
        booleanSimple(),
        booleanWithScore(),
        objectIdSimple(),
        objectIdWithScore(),
        dateSimple(),
        dateWithScore(),
        numberLongSimple(),
        numberLongWithScore(),
        numberDoubleSimple(),
        numberDouble15Decimal(),
        numberDouble18Decimal(),
        numberWithScore(),
        stringSimple(),
        stringMultiple(),
        uuidSimple(),
        uuidWithScore(),
        multipleBoolean(),
        multipleObjectID(),
        multipleDate(),
        multipleNumber(),
        multipleUuid(),
        multiplePath());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> doesNotAffect() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "doesNotAffect",
        OperatorBuilder.in()
            .path("size")
            .strings(List.of("S", "M"))
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> doesNotAffectString() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "doesNotAffectString",
        OperatorBuilder.in()
            .path("size")
            .strings(List.of("S", "M"))
            .doesNotAffect("sizes")
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> booleanSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "boolean-single-value", OperatorBuilder.in().path("a").booleans(List.of(true)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> booleanWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "boolean-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .booleans(List.of(false))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> objectIdSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "objectId-single-value",
        OperatorBuilder.in()
            .path("a")
            .objectIds(List.of(new ObjectId("507f1f77bcf86cd799439011")))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> objectIdWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "objectId-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .objectIds(List.of(new ObjectId("507f1f77bcf86cd799439011")))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberLongSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberLong-single-value", OperatorBuilder.in().path("a").longs(List.of(2L)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberLongWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberLong-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .longs(List.of(2L))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberDoubleSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-single-value", OperatorBuilder.in().path("a").doubles(List.of(2.01)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberDouble15Decimal() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-15decimal",
        OperatorBuilder.in().path("a").doubles(List.of(2.151515151515151)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberDouble18Decimal() {
    // this has 16 decimals, but the corresponding bson test has 18 decimals
    // java double loses precision after 15 decimal places
    return BsonDeserializationTestSuite.TestSpec.valid(
        "numberDouble-18decimal",
        OperatorBuilder.in().path("a").doubles(List.of(2.1515151515151515)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> numberWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "number-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .longs(List.of(2L))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> dateSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date-single-value",
        OperatorBuilder.in()
            .path("a")
            .dates(
                List.of(
                    new Calendar.Builder()
                        .setDate(2022, Calendar.DECEMBER, 6)
                        .setTimeOfDay(13, 13, 13)
                        .setTimeZone(TimeZone.getTimeZone("UTC"))
                        .build()
                        .getTime()))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> dateWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .dates(
                List.of(
                    new Calendar.Builder()
                        .setDate(2022, Calendar.DECEMBER, 6)
                        .setTimeOfDay(13, 13, 13)
                        .setTimeZone(TimeZone.getTimeZone("UTC"))
                        .build()
                        .getTime()))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> stringSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "string-single-value", OperatorBuilder.in().path("a").strings(List.of("example")).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> stringMultiple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "string-multiple-values",
        OperatorBuilder.in().path("a").strings(List.of("example1", "example2")).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> uuidSimple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "uuid-single-value",
        OperatorBuilder.in()
            .path("a")
            .uuidStrings(List.of("00000000-1111-2222-3333-444444444444"))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> uuidWithScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "uuid-single-value-with-score",
        OperatorBuilder.in()
            .path("a")
            .uuidStrings(List.of("00000000-1111-2222-3333-444444444444"))
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multipleBoolean() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "boolean-multiple-values",
        OperatorBuilder.in().path("a").booleans(List.of(true, false)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multipleObjectID() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "objectId-multiple-values",
        OperatorBuilder.in()
            .path("a")
            .objectIds(
                List.of(
                    new ObjectId("507f1f77bcf86cd799439011"),
                    new ObjectId("507f191e810c19729de860ea")))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multipleDate() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date-multiple-values",
        OperatorBuilder.in()
            .path("a")
            .dates(
                List.of(
                    new Date(Instant.parse("2022-12-06T13:13:13Z").toEpochMilli()),
                    new Date(Instant.parse("2022-12-06T13:13:14Z").toEpochMilli())))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multipleNumber() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "number-multiple-values",
        OperatorBuilder.in().path("a").longs(List.of(1L, 2L, 3L)).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multipleUuid() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "uuid-multiple-values",
        OperatorBuilder.in()
            .path("a")
            .uuidStrings(
                List.of(
                    "00000000-1111-2222-3333-444444444444", "55555555-6666-7777-8888-999999999999"))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<InOperator> multiplePath() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "multiple-path",
        OperatorBuilder.in()
            .paths(List.of("a", "b"))
            .strings(List.of("example1", "example2"))
            .build());
  }
}
