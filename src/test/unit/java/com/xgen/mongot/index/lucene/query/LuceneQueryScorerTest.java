package com.xgen.mongot.index.lucene.query;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.SpanOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class LuceneQueryScorerTest {

  private static final SingleQueryContext MOCK_SINGLE_QUERY_CONTEXT =
      SingleQueryContext.createQueryRoot(mock(IndexReader.class));

  @Test
  public void testTermQueryScored() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("unused")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build();
    Query query = new TermQuery(new Term("fld", "text"));

    Query expected = new BoostQuery(query, 2);
    Query result =
        LuceneQueryScorer.create(getEmptyQueryFactoryContext())
            .score(operator, query, MOCK_SINGLE_QUERY_CONTEXT);

    Assert.assertEquals("Lucene query:", expected, result);
  }

  @Test
  public void testCompoundQueryScored() throws Exception {
    CompoundOperator operator =
        OperatorBuilder.compound()
            .must(OperatorBuilder.text().query("unused").build())
            .score(ScoreBuilder.constant().value(2).build())
            .build();
    Query query =
        new BooleanQuery.Builder()
            .add(BooleanComposer.mustClause(new TermQuery(new Term("fld", "text"))))
            .build();

    Query expected = new BoostQuery(new ConstantScoreQuery(query), 2);
    Query result =
        LuceneQueryScorer.create(getEmptyQueryFactoryContext())
            .score(operator, query, MOCK_SINGLE_QUERY_CONTEXT);

    Assert.assertEquals("Lucene query:", expected, result);
  }

  @Test
  public void testCompoundQueryDismaxScored() throws Exception {
    CompoundOperator operator =
        OperatorBuilder.compound()
            .must(OperatorBuilder.text().query("unused").build())
            .must(OperatorBuilder.text().query("more_unused").build())
            .score(ScoreBuilder.dismax().tieBreakerScore(0.3f).build())
            .build();
    Query query =
        new DisjunctionMaxQuery(
            List.of(
                new BooleanQuery.Builder()
                    .add(BooleanComposer.mustClause(new TermQuery(new Term("fld", "text"))))
                    .build(),
                new BooleanQuery.Builder()
                    .add(BooleanComposer.mustClause(new TermQuery(new Term("fld", "text"))))
                    .build()),
            0.3f);

    //noinspection UnnecessaryLocalVariable
    Query expected = query;
    Query result =
        LuceneQueryScorer.create(getEmptyQueryFactoryContext())
            .score(operator, query, MOCK_SINGLE_QUERY_CONTEXT);

    Assert.assertEquals("Lucene query:", expected, result);
  }

  @Test
  public void testSpanQueryScored() throws Exception {
    SpanOperator operator =
        SpanOperatorBuilder.term()
            .term(OperatorBuilder.term().query("unused").build())
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build();
    SpanTermQuery query = new SpanTermQuery(new Term("fld", "text"));

    Query expected = new BoostQuery(query, 2);
    Query result =
        LuceneQueryScorer.create(getEmptyQueryFactoryContext())
            .score(operator, query, MOCK_SINGLE_QUERY_CONTEXT);

    Assert.assertEquals("Lucene query:", expected, result);
  }

  private SearchQueryFactoryContext getEmptyQueryFactoryContext()
      throws InvalidAnalyzerDefinitionException {
    AnalyzerRegistry analyzerRegistry =
        AnalyzerRegistry.factory().create(Collections.emptyList(), true);
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, indexDefinition.getSynonymMap(), Optional.empty());
    return new SearchQueryFactoryContext(
        analyzerRegistry,
        LuceneAnalyzer.queryAnalyzer(indexDefinition, analyzerRegistry),
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        synonymRegistry,
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        FeatureFlags.getDefault());
  }
}
