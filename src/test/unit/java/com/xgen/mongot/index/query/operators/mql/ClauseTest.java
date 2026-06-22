package com.xgen.mongot.index.query.operators.mql;

import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;

import com.xgen.mongot.index.query.operators.value.NullValue;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {ClauseTest.DeserializationTest.class, ClauseTest.SerializationTest.class})
public class ClauseTest {
  static final String DEFAULT_PATH = "path";

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "clause-deserialization";
    private static final BsonDeserializationTestSuite<Clause> TEST_SUITE =
        fromValue(ClauseTests.RESOURCES_PATH, SUITE_NAME, Clause::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<Clause> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<Clause> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<Clause>> data()
        throws BsonParseException {
      return TEST_SUITE.withExamples(
          simple(),
          simpleNull(),
          simpleMultipleOperators(),
          and(),
          or(),
          nor(),
          nestedCompound(),
          andor(),
          andWithEmptyFilter(),
          andWithMultiKey(),
          orWithEmptyFilter(),
          orWithMultiKey(),
          norWithEmptyFilter(),
          norWithMultiKey(),
          gt(),
          gte(),
          lt(),
          lte(),
          eq(),
          ne(),
          in(),
          nin(),
          multipleOperators(),
          not(),
          nestednot(),
          notEqNe());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> not() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build()))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> nestednot()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nested $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.not()
                                      .value(
                                          new MqlFilterOperatorList(
                                              Arrays.asList(
                                                  MqlFilterOperatorBuilder.eq()
                                                      .value(ValueBuilder.doubleNumber(3))
                                                      .build())))
                                      .build()))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> notEqNe()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "eq ne $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build(),
                                  MqlFilterOperatorBuilder.ne()
                                      .value(ValueBuilder.doubleNumber(4))
                                      .build()))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple $eq",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> simpleNull() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple $eq null",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(MqlFilterOperatorBuilder.eq().value(new NullValue()).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> simpleMultipleOperators()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple multiple operators",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
              .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> and() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "and",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> or() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "or",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> nor() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nor",
          ClauseBuilder.norClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> nestedCompound()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nested compound",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num1"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.intNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.intNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num2"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.intNumber(1))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name1"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.in()
                                      .values(
                                          List.of(
                                              ValueBuilder.string("mongodb"),
                                              ValueBuilder.string("atlas"),
                                              ValueBuilder.string("search"),
                                              ValueBuilder.string("mongot")))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name2"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("mongodb"))
                                      .build())
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> andor()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "and or",
          ClauseBuilder.andClause()
              .explicitAnd(false)
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.intNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.intNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("mongodb"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.orClause()
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("category"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("tech"))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("product"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("database"))
                                      .build())
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> andWithEmptyFilter()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "and with empty filter",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .addClause(ClauseBuilder.andClause().explicitAnd(false).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> andWithMultiKey()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "and with multi key",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(false)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.intNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.intNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.city"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("san jose"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> orWithEmptyFilter()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "or with empty filter",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .addClause(ClauseBuilder.andClause().explicitAnd(false).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> orWithMultiKey()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "or with multi key",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(false)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.intNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.intNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.city"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("san jose"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> norWithEmptyFilter()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nor with empty filter",
          ClauseBuilder.norClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .addClause(ClauseBuilder.andClause().explicitAnd(false).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> norWithMultiKey()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nor with multi key",
          ClauseBuilder.norClause()
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(false)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.intNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.intNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.city"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("san jose"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> gt() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "gt",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> gte() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "gte",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> lt() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "lt",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> lte() throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "lte",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> eq() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "eq",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> ne() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "ne",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.ne().value(ValueBuilder.intNumber(3)).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> in() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "in",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.in()
                      .values(
                          Arrays.asList(
                              ValueBuilder.intNumber(1),
                              ValueBuilder.intNumber(2),
                              ValueBuilder.intNumber(3),
                              ValueBuilder.intNumber(4)))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> nin() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nin",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.nin()
                      .values(
                          Arrays.asList(
                              ValueBuilder.intNumber(1),
                              ValueBuilder.intNumber(2),
                              ValueBuilder.intNumber(3),
                              ValueBuilder.intNumber(4)))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Clause> multipleOperators()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple operators",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(1)).build())
              .addOperator(
                  MqlFilterOperatorBuilder.nin()
                      .values(Arrays.asList(ValueBuilder.intNumber(6), ValueBuilder.intNumber(3)))
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "clause-serialization";
    private static final BsonSerializationTestSuite<Clause> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(ClauseTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<Clause> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<Clause> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<Clause>> data()
        throws BsonParseException {
      return Arrays.asList(
          simpleEq(),
          simpleDouble(),
          and(),
          or(),
          nor(),
          nestedCompound(),
          andor(),
          andWithEmptyFilter(),
          andWithMultiKey(),
          orWithEmptyFilter(),
          orWithMultiKey(),
          gt(),
          gte(),
          lt(),
          lte(),
          eq(),
          ne(),
          in(),
          nin(),
          multipleOperators(),
          not(),
          nestednot(),
          notEqNe());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> not() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build()))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> nestednot()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "nested $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.not()
                                      .value(
                                          new MqlFilterOperatorList(
                                              Arrays.asList(
                                                  MqlFilterOperatorBuilder.eq()
                                                      .value(ValueBuilder.doubleNumber(3))
                                                      .build())))
                                      .build()))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> notEqNe() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "eq ne $not",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.not()
                      .value(
                          new MqlFilterOperatorList(
                              Arrays.asList(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build(),
                                  MqlFilterOperatorBuilder.ne()
                                      .value(ValueBuilder.doubleNumber(4))
                                      .build()))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> simpleEq() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple $eq",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.eq().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> simpleDouble()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple double",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse("path.num"))
              .addOperator(
                  MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
              .addOperator(
                  MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> and() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "and",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> or() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "or",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> nor() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "nor",
          ClauseBuilder.norClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> nestedCompound()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "nested compound",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num1"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.doubleNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num2"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.doubleNumber(1))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name1"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.in()
                                      .values(
                                          List.of(
                                              ValueBuilder.string("mongodb"),
                                              ValueBuilder.string("atlas"),
                                              ValueBuilder.string("search"),
                                              ValueBuilder.string("mongot")))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name2"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("mongodb"))
                                      .build())
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> andor() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "and or",
          ClauseBuilder.andClause()
              .explicitAnd(false)
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(true)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.doubleNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.name"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("mongodb"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.orClause()
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("category"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("tech"))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("product"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("database"))
                                      .build())
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> andWithEmptyFilter()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "and with empty filter",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .addClause(ClauseBuilder.andClause().explicitAnd(false).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> andWithMultiKey()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "and with multi key",
          ClauseBuilder.andClause()
              .explicitAnd(true)
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(false)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.doubleNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.city"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("san jose"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> orWithEmptyFilter()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "or with empty filter",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.num"))
                      .addOperator(
                          MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
                      .addOperator(
                          MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(5)).build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .addClause(ClauseBuilder.andClause().explicitAnd(false).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> orWithMultiKey()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "or with multi key",
          ClauseBuilder.orClause()
              .addClause(
                  ClauseBuilder.andClause()
                      .explicitAnd(false)
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.num"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.gt()
                                      .value(ValueBuilder.doubleNumber(3))
                                      .build())
                              .addOperator(
                                  MqlFilterOperatorBuilder.lt()
                                      .value(ValueBuilder.doubleNumber(5))
                                      .build())
                              .build())
                      .addClause(
                          ClauseBuilder.simpleClause()
                              .path(FieldPath.parse("path.city"))
                              .addOperator(
                                  MqlFilterOperatorBuilder.eq()
                                      .value(ValueBuilder.string("san jose"))
                                      .build())
                              .build())
                      .build())
              .addClause(
                  ClauseBuilder.simpleClause()
                      .path(FieldPath.parse("path.name"))
                      .addOperator(
                          MqlFilterOperatorBuilder.eq()
                              .value(ValueBuilder.string("mongodb"))
                              .build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> gt() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "gt",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> gte() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "gte",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.gte().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> lt() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "lt",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.lt().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> lte() throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "lte",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.lte().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> eq() {
      return BsonSerializationTestSuite.TestSpec.create(
          "eq",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.eq().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> ne() {
      return BsonSerializationTestSuite.TestSpec.create(
          "ne",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.ne().value(ValueBuilder.doubleNumber(3)).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> in() {
      return BsonSerializationTestSuite.TestSpec.create(
          "in",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.in()
                      .values(
                          Arrays.asList(
                              ValueBuilder.doubleNumber(1),
                              ValueBuilder.doubleNumber(2),
                              ValueBuilder.doubleNumber(3),
                              ValueBuilder.doubleNumber(4)))
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> nin() {
      return BsonSerializationTestSuite.TestSpec.create(
          "nin",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.nin()
                      .values(
                          Arrays.asList(
                              ValueBuilder.doubleNumber(1),
                              ValueBuilder.doubleNumber(2),
                              ValueBuilder.doubleNumber(3),
                              ValueBuilder.doubleNumber(4)))
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<Clause> multipleOperators()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "multiple operators",
          ClauseBuilder.simpleClause()
              .path(FieldPath.parse(DEFAULT_PATH))
              .addOperator(
                  MqlFilterOperatorBuilder.gt().value(ValueBuilder.doubleNumber(1)).build())
              .addOperator(
                  MqlFilterOperatorBuilder.nin()
                      .values(
                          Arrays.asList(ValueBuilder.doubleNumber(6), ValueBuilder.doubleNumber(3)))
                      .build())
              .build());
    }
  }
}
