package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DisableBulkScorerQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryChildren;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class DisableBulkScorerQuerySpecCreatorTest {

  @Test
  public void testQuerySpecFor() {
    TermQuery termQuery = new TermQuery(new Term("$type:string/foo", "bar"));
    QueryExecutionContextNode termNode = new MockQueryExecutionContextNode(termQuery);
    QueryExplainInformation termInfo =
        QueryExplainInformationCreator.fromNode(
            termNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    DisableBulkScorerQuerySpec expected = new DisableBulkScorerQuerySpec(termInfo);

    DisableBulkScorerQuery wrapped = new DisableBulkScorerQuery(termQuery);
    QueryChildren<QueryExecutionContextNode> children =
        new MockQueryChildren(List.of(termNode), List.of(), List.of(), List.of());
    QueryExecutionContextNode wrapperNode =
        new MockQueryExecutionContextNode(
            wrapped, ExplainTimings.builder().build(), Optional.of(children));

    LuceneQuerySpecification result =
        LuceneQuerySpecificationCreator.querySpecFor(
            wrapperNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            (DisableBulkScorerQuerySpec) result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
