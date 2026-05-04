package com.xgen.mongot.index.lucene.query.pushdown.match;

import static com.mongodb.assertions.Assertions.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.lucene.query.pushdown.match.MatchStageTest.InequalityTest;
import com.xgen.mongot.index.lucene.query.pushdown.match.MatchStageTest.MixedTypesTest;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperatorList;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.query.operators.mql.AndClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.EqOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ExistsOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.GtOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.GteOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.InOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.InequalityBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.LtOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.LteOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.NeOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.NinOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.NorClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.NotOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.OrClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.SimpleClauseBuilder;
import java.util.List;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@SuiteClasses({
  InequalityTest.class,
  MixedTypesTest.class,
})
@RunWith(Suite.class)
public class MatchStageTest {

  private static final RawBsonDocument doc =
      BsonUtils.documentToRaw(
          new BsonDocument()
              .append("$path", new BsonString("EscapedPath"))
              .append("dotted.path", new BsonString("Illegal"))
              .append("top", new BsonDouble(5.0))
              .append(
                  "array",
                  new BsonArray(
                      List.of(
                          new BsonDateTime(1),
                          new BsonDocument("foo", new BsonInt64(1)),
                          new BsonDocument("foo", new BsonInt64(2)),
                          new BsonArray(List.of(new BsonDocument("foo", new BsonInt64(3)))),
                          new BsonDocument(
                              "nested", new BsonArray(List.of(new BsonString("foo"))))))));

  private static SimpleClauseBuilder clause(String path, InequalityBuilder<?> builder)
      throws BsonParseException {
    return new SimpleClauseBuilder().path(path).addOperator(builder);
  }

  private static SimpleClauseBuilder clause(String path, MqlFilterOperator op) {
    return new SimpleClauseBuilder().path(path).addOperator(op);
  }

  public static class InequalityTest {

    @Test
    public void gte() throws BsonParseException {
      Clause clause = clause("top", new GteOperatorBuilder().value(4)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void gt() throws BsonParseException {
      Clause clause = clause("top", new GtOperatorBuilder().value(4)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void eq() throws BsonParseException {
      Clause clause = clause("top", new EqOperatorBuilder().value(5)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void lte() throws BsonParseException {
      Clause clause = clause("top", new LteOperatorBuilder().value(5)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void lt() throws BsonParseException {
      Clause clause = clause("top", new LtOperatorBuilder().value(6)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void in() throws BsonParseException {
      Clause clause = clause("top", new InOperatorBuilder().addValue(5).build()).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void nin() throws BsonParseException {
      Clause clause = clause("top", new NinOperatorBuilder().addValue(500).build()).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void not() throws BsonParseException {
      List<MqlFilterOperator> comparisonOperators =
          List.of(new NeOperatorBuilder().value(6).build());
      MqlFilterOperatorList comparisonOperatorsList =
          new MqlFilterOperatorList(comparisonOperators);
      Clause clause =
          clause("top", new NotOperatorBuilder().value(comparisonOperatorsList).build()).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void gte_no_match() throws BsonParseException {
      Clause clause = clause("top", new GteOperatorBuilder().value(9000)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void gt_no_match() throws BsonParseException {
      Clause clause = clause("top", new GtOperatorBuilder().value(9000)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void eq_no_match() throws BsonParseException {
      Clause clause = clause("top", new EqOperatorBuilder().value(9000)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void lte_no_match() throws BsonParseException {
      Clause clause = clause("top", new LteOperatorBuilder().value(-9000)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void lt_no_match() throws BsonParseException {
      Clause clause = clause("top", new LtOperatorBuilder().value(-9000)).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void in_no_match() throws BsonParseException {
      Clause clause = clause("top", new InOperatorBuilder().addValue(9000).build()).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void nin_no_match() throws BsonParseException {
      Clause clause = clause("top", new NinOperatorBuilder().addValue(5).build()).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void andMatch() throws BsonParseException {
      Clause clause =
          new AndClauseBuilder()
              .explicitAnd(true)
              .addClause(clause("top", new GtOperatorBuilder().value(4)).build())
              .addClause(clause("top", new GteOperatorBuilder().value(5)).build())
              .addClause(clause("top", new EqOperatorBuilder().value(5)).build())
              .addClause(clause("top", new NeOperatorBuilder().value(4)).build())
              .addClause(clause("top", new LtOperatorBuilder().value(6)).build())
              .addClause(clause("top", new LteOperatorBuilder().value(5)).build())
              .addClause(
                  clause("top", new InOperatorBuilder().addValue(5).addValue(4).build()).build())
              .addClause(
                  clause("top", new NinOperatorBuilder().addValue(1).addValue(2).build()).build())
              .build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void orMatch() throws BsonParseException {
      Clause clause =
          new OrClauseBuilder()
              .addClause(clause("top", new GtOperatorBuilder().value(500)).build())
              .addClause(clause("top", new GtOperatorBuilder().value(400)).build())
              .addClause(clause("top", new GteOperatorBuilder().value(5)).build())
              .addClause(clause("top", new EqOperatorBuilder().value(5)).build())
              .addClause(clause("top", new NeOperatorBuilder().value(4)).build())
              .addClause(clause("top", new LtOperatorBuilder().value(6)).build())
              .addClause(clause("top", new LteOperatorBuilder().value(5)).build())
              .addClause(
                  clause("top", new InOperatorBuilder().addValue(3).addValue(4).build()).build())
              .addClause(
                  clause("top", new NinOperatorBuilder().addValue(3).addValue(4).build()).build())
              .build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    // NOTE: come back here and add test for not
  }

  @Test
  public void norMatch() throws BsonParseException {
    Clause clause =
        new NorClauseBuilder()
            .addClause(clause("top", new GtOperatorBuilder().value(500)).build())
            .addClause(clause("top", new GtOperatorBuilder().value(400)).build())
            .addClause(clause("top", new GteOperatorBuilder().value(5)).build())
            .addClause(clause("top", new EqOperatorBuilder().value(5)).build())
            .addClause(clause("top", new NeOperatorBuilder().value(4)).build())
            .addClause(clause("top", new LtOperatorBuilder().value(6)).build())
            .addClause(clause("top", new LteOperatorBuilder().value(5)).build())
            .addClause(
                clause("top", new InOperatorBuilder().addValue(3).addValue(4).build()).build())
            .addClause(
                clause("top", new NinOperatorBuilder().addValue(3).addValue(4).build()).build())
            .build();

    MatchStage matcher = MatchStage.build(clause);
    boolean result = matcher.test(doc);

    assertTrue(result);
  }

  @Test
  public void existsTrueMatch() throws BsonParseException {
    Clause clause = clause("array.foo", new ExistsOperatorBuilder().value(true).build()).build();

    MatchStage matcher = MatchStage.build(clause);
    boolean result = matcher.test(doc);

    assertTrue(result);
  }

  @Test
  public void existsFalseMatch() throws BsonParseException {
    Clause clause = clause("array.foo", new ExistsOperatorBuilder().value(false).build()).build();

    MatchStage matcher = MatchStage.build(clause);
    boolean result = matcher.test(doc);

    assertFalse(result);
  }

  @RunWith(Theories.class)
  public static class MixedTypesTest {

    @DataPoints
    public static ImmutableList<Supplier<InequalityBuilder<?>>> builders() {
      return ImmutableList.of(
          GteOperatorBuilder::new,
          GtOperatorBuilder::new,
          EqOperatorBuilder::new,
          LteOperatorBuilder::new,
          LteOperatorBuilder::new);
    }

    @Theory
    public void inequalityReturnsFalseForTypeMismatch(Supplier<InequalityBuilder<?>> operator)
        throws BsonParseException {
      var builder = operator.get();
      Clause clause = clause("top", builder.value("NotANumber")).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      Assert.assertFalse("Failure in " + builder, result);
    }

    @Test
    public void inReturnsFalseForTypeMismatch() throws BsonParseException {
      var in = new InOperatorBuilder().addValue("NotANumber").build();
      Clause clause = clause("top", in).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertFalse(result);
    }

    @Test
    public void ninReturnsTrueForTypeMismatch() throws BsonParseException {
      var nin = new NinOperatorBuilder().addValue("NotANumber").build();
      Clause clause = clause("top", nin).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }

    @Test
    public void neReturnsTrueForTypeMismatch() throws BsonParseException {
      var ne = new NeOperatorBuilder().value("NotANumber").build();
      Clause clause = clause("top", ne).build();

      MatchStage matcher = MatchStage.build(clause);
      boolean result = matcher.test(doc);

      assertTrue(result);
    }
  }
}
