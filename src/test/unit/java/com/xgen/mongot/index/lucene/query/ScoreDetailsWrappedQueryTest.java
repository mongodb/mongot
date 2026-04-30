package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.filterClause;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.mustClause;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.shouldClause;

import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToChildBlockJoinQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.lucene.query.ScoreDetailsWrappedQueryBuilder;
import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Test;

public class ScoreDetailsWrappedQueryTest {
  @Test
  public void testWrapBooleanQuery() {
    TermQuery termQuery = termQuery("hello");
    BooleanQuery wrappedBooleanQuery =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    ScoreDetailsWrappedQueryBuilder.builder().query(termQuery).build(),
                    BooleanClause.Occur.MUST))
            .build();
    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder().query(wrappedBooleanQuery).build();

    ScoreDetailsWrappedQuery actual =
        ScoreDetailsWrappedQuery.wrap(
            new BooleanQuery.Builder()
                .add(new BooleanClause(termQuery, BooleanClause.Occur.MUST))
                .build());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWrapNestedBooleanQuery() {
    TermQuery innerTermQuery = termQuery("inner");
    BooleanQuery innerBooleanQuery =
        new BooleanQuery.Builder()
            .add(new BooleanClause(innerTermQuery, BooleanClause.Occur.MUST))
            .build();

    TermQuery outerTermQuery = termQuery("outer");
    BooleanQuery outerBooleanQuery =
        new BooleanQuery.Builder()
            .add(new BooleanClause(outerTermQuery, BooleanClause.Occur.MUST))
            .add(new BooleanClause(innerBooleanQuery, BooleanClause.Occur.MUST))
            .build();

    BooleanQuery wrappedInnerBooleanQuery =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    ScoreDetailsWrappedQueryBuilder.builder().query(innerTermQuery).build(),
                    BooleanClause.Occur.MUST))
            .build();

    BooleanQuery wrappedOuterBooleanQuery =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    ScoreDetailsWrappedQueryBuilder.builder().query(outerTermQuery).build(),
                    BooleanClause.Occur.MUST))
            .add(
                new BooleanClause(
                    ScoreDetailsWrappedQueryBuilder.builder()
                        .query(wrappedInnerBooleanQuery)
                        .build(),
                    BooleanClause.Occur.MUST))
            .build();

    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder().query(wrappedOuterBooleanQuery).build();

    ScoreDetailsWrappedQuery actual = ScoreDetailsWrappedQuery.wrap(outerBooleanQuery);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWrapEmbeddedQuery() {
    TermQuery termQuery = termQuery("hello");
    QueryBitSetProducer parentsFilter = new QueryBitSetProducer(termQuery("parent"));
    ScoreMode scoreMode = ScoreMode.Total;

    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder()
            .query(
                new WrappedToParentBlockJoinQuery(
                    ScoreDetailsWrappedQueryBuilder.builder().query(termQuery).build(),
                    parentsFilter,
                    scoreMode))
            .build();

    ScoreDetailsWrappedQuery actual =
        ScoreDetailsWrappedQuery.wrap(
            new WrappedToParentBlockJoinQuery(termQuery, parentsFilter, scoreMode));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWrapStripsDisableBulkScorerAroundBlockJoinChild() {
    TermQuery termQuery = termQuery("hello");
    QueryBitSetProducer parentsFilter = new QueryBitSetProducer(termQuery("parent"));
    ScoreMode scoreMode = ScoreMode.Total;

    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder()
            .query(
                new WrappedToParentBlockJoinQuery(
                    ScoreDetailsWrappedQueryBuilder.builder().query(termQuery).build(),
                    parentsFilter,
                    scoreMode))
            .build();

    ScoreDetailsWrappedQuery actual =
        ScoreDetailsWrappedQuery.wrap(
            new WrappedToParentBlockJoinQuery(
                new DisableBulkScorerQuery(termQuery), parentsFilter, scoreMode));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWrapStripsDisableBulkScorerAroundTerm() {
    TermQuery termQuery = termQuery("hello");
    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder().query(termQuery).build();
    Assert.assertEquals(
        expected, ScoreDetailsWrappedQuery.wrap(new DisableBulkScorerQuery(termQuery)));
  }

  @Test
  public void testWrapToChildBlockJoinQuery() {
    TermQuery termQuery = termQuery("hello");
    QueryBitSetProducer parentsFilter = new QueryBitSetProducer(termQuery("parent"));

    ScoreDetailsWrappedQuery expected =
        ScoreDetailsWrappedQueryBuilder.builder()
            .query(
                new WrappedToChildBlockJoinQuery(
                    ScoreDetailsWrappedQueryBuilder.builder().query(termQuery).build(),
                    parentsFilter))
            .build();

    ScoreDetailsWrappedQuery actual =
        ScoreDetailsWrappedQuery.wrap(
            new WrappedToChildBlockJoinQuery(termQuery, parentsFilter));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRewriteQueryReturnsSameWrappedQuery() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
      IndexReader reader = DirectoryReader.open(writer);
      ScoreDetailsWrappedQuery scoreDetailsWrappedQuery =
          ScoreDetailsWrappedQuery.wrap(termQuery("hello"));
      Query rewritten = scoreDetailsWrappedQuery.rewrite(new IndexSearcher(reader));
      Assert.assertSame(
          "wrapped TermQuery should not be rewritten", scoreDetailsWrappedQuery, rewritten);
    }
  }

  @Test
  public void testRewriteReturnsExpectedWrappedQuery() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      var writer = new IndexWriter(directory, new IndexWriterConfig());
      var reader = DirectoryReader.open(writer);
      ScoreDetailsWrappedQuery scoreDetailsWrappedQuery =
          ScoreDetailsWrappedQuery.wrap(new PrefixQuery(new Term("path", "hello")));

      var searcher = new IndexSearcher(reader);
      Query result = scoreDetailsWrappedQuery.rewrite(searcher);
      Assert.assertNotEquals(
          "wrapped PrefixQuery should be rewritten into a different query",
          scoreDetailsWrappedQuery,
          result);

      // Default rewrite method used in MultiTermQuery is CONSTANT_SCORE_BLENDED_REWRITE
      ScoreDetailsWrappedQuery expected =
          ScoreDetailsWrappedQueryBuilder.builder()
              .query(
                  MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE.rewrite(
                      searcher, new PrefixQuery(new Term("path", "hello"))))
              .build();

      Assert.assertEquals(
          "wrapped PrefixQuery has been rewritten into the expected query", expected, result);
    }
  }

  @Test
  public void testRewriteComplex() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      var writer = new IndexWriter(directory, new IndexWriterConfig());
      var reader = DirectoryReader.open(writer);

      Query innerQuery =
          new BooleanQuery.Builder()
              .add(shouldClause(termQuery("b")))
              .add(filterClause(termQuery("b")))
              .build();

      Query outerQuery =
          ScoreDetailsWrappedQuery.wrap(
              new BooleanQuery.Builder().add(filterClause(innerQuery)).build());

      Query firstRewrite = outerQuery.rewrite(new IndexSearcher(reader));
      Query firstRewriteExpected =
          ScoreDetailsWrappedQuery.wrap(
              new BoostQuery(
                  new ConstantScoreQuery(ScoreDetailsWrappedQuery.wrap(innerQuery)), 0.0f));
      Assert.assertEquals(
          "outerQuery should be rewritten to BoostQuery with score of 0.0",
          firstRewriteExpected,
          firstRewrite);

      Query secondRewrite = firstRewrite.rewrite(new IndexSearcher(reader));
      Query secondRewriteExpected =
          ScoreDetailsWrappedQuery.wrap(
              new BoostQuery(
                  new ConstantScoreQuery(
                      ScoreDetailsWrappedQuery.wrap(
                          new BooleanQuery.Builder().add(mustClause(termQuery("b"))).build())),
                  0.0f));
      Assert.assertEquals(
          "rewrite should turn duplicate Filter & Should clause into a single Must",
          secondRewriteExpected,
          secondRewrite);

      Query thirdRewrite = secondRewrite.rewrite(new IndexSearcher(reader));
      Query expected =
          ScoreDetailsWrappedQuery.wrap(
              new BoostQuery(
                  new ConstantScoreQuery(
                      ScoreDetailsWrappedQuery.wrap(ScoreDetailsWrappedQuery.wrap(termQuery("b")))),
                  0.0f));
      Assert.assertEquals(
          "should rewrite single Must clause into the query inside the clause",
          expected,
          thirdRewrite);

      Query result = thirdRewrite.rewrite(new IndexSearcher(reader));
      Assert.assertSame("query should not be rewritten anymore", thirdRewrite, result);
    }
  }

  @Test
  public void testExhaustRewrite() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      var writer = new IndexWriter(directory, new IndexWriterConfig());
      var reader = DirectoryReader.open(writer);
      ScoreDetailsWrappedQuery wrappedQuery =
          ScoreDetailsWrappedQuery.wrap(new PrefixQuery(new Term("a", "hello")));

      Query firstRewrite = wrappedQuery.rewrite(new IndexSearcher(reader));
      // should not change with the following rewrite
      Query secondRewrite = firstRewrite.rewrite(new IndexSearcher(reader));

      Assert.assertSame(
          "wrapped PrefixQuery should be done rewriting itself", firstRewrite, secondRewrite);
    }
  }

  @Test
  public void testEquals() {
    TestUtils.assertEqualityGroups(
        () -> ScoreDetailsWrappedQuery.wrap(termQuery("hello")),
        () -> ScoreDetailsWrappedQuery.wrap(termQuery("world")));
  }

  private static TermQuery termQuery(String value) {
    return new TermQuery(new Term("path", value));
  }
}
