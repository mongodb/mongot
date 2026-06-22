package com.xgen.mongot.index;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.cursor.batch.BatchSizeStrategySelector;
import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MeteredSearchIndexReaderTest.TestClass.class,
      QueryMetricsRecorderTest.class,
    })
public class MeteredSearchIndexReaderTest {

  public static class TestClass {
    private static final SearchQuery QUERY_DEFINITION =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .returnStoredSource(false)
            .build();

    @Test
    public void testQuery() throws Exception {
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(
              mock(SearchIndexReader.class), queryingMetricsUpdater, () -> false);

      meteredIndexReader.query(
          QUERY_DEFINITION,
          QueryCursorOptions.empty(),
          BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty()),
          QueryOptimizationFlags.DEFAULT_OPTIONS);

      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(
          1,
          queryingMetricsUpdater
              .getQueryFeaturesMetricsUpdater()
              .getOperatorTypeCounter(Operator.Type.EXISTS)
              .count(),
          0);
      Assert.assertEquals(
          1,
          queryingMetricsUpdater
              .getQueryFeaturesMetricsUpdater()
              .getScoreTypeCounter(Score.Type.CONSTANT)
              .count(),
          0);
    }

    @Test
    public void testIntermediateQuery() throws Exception {
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(
              mock(SearchIndexReader.class), queryingMetricsUpdater, () -> false);

      meteredIndexReader.intermediateQuery(
          QUERY_DEFINITION,
          QueryCursorOptions.empty(),
          BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty()),
          QueryOptimizationFlags.DEFAULT_OPTIONS);

      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(
          1,
          queryingMetricsUpdater
              .getQueryFeaturesMetricsUpdater()
              .getOperatorTypeCounter(Operator.Type.EXISTS)
              .count(),
          0);
      Assert.assertEquals(
          1,
          queryingMetricsUpdater
              .getQueryFeaturesMetricsUpdater()
              .getScoreTypeCounter(Score.Type.CONSTANT)
              .count(),
          0);
    }

    @Test
    public void testTotalQueryMetric()
        throws IOException, InvalidQueryException, InterruptedException {
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(
              mock(SearchIndexReader.class), queryingMetricsUpdater, () -> false);

      meteredIndexReader.query(
          QUERY_DEFINITION,
          QueryCursorOptions.empty(),
          BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty()),
          QueryOptimizationFlags.DEFAULT_OPTIONS);
      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
    }

    @Test
    public void testBadQueryMetric()
        throws IOException, InvalidQueryException, InterruptedException {
      SearchIndexReader mockIndexReader = mock(SearchIndexReader.class);
      var constantBatchSizeStrategy = new ConstantBatchSizeStrategy();
      when(mockIndexReader.query(
              QUERY_DEFINITION,
              QueryCursorOptions.empty(),
              constantBatchSizeStrategy,
              QueryOptimizationFlags.DEFAULT_OPTIONS))
          .thenThrow(IOException.class);
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(mockIndexReader, queryingMetricsUpdater, () -> false);

      Assert.assertThrows(
          IOException.class,
          () ->
              meteredIndexReader.query(
                  QUERY_DEFINITION,
                  QueryCursorOptions.empty(),
                  constantBatchSizeStrategy,
                  QueryOptimizationFlags.DEFAULT_OPTIONS));
      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getInvalidQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getInternallyFailedQueryCounter().count(), 0);
    }

    @Test
    public void testQueryThrowsNpe()
        throws IOException, InvalidQueryException, InterruptedException {
      SearchIndexReader mockIndexReader = mock(SearchIndexReader.class);
      var constantBatchSizeStrategy = new ConstantBatchSizeStrategy();
      when(mockIndexReader.query(
              QUERY_DEFINITION,
              QueryCursorOptions.empty(),
              constantBatchSizeStrategy,
              QueryOptimizationFlags.DEFAULT_OPTIONS))
          .thenThrow(NullPointerException.class);
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(mockIndexReader, queryingMetricsUpdater, () -> false);

      Assert.assertThrows(
          NullPointerException.class,
          () ->
              meteredIndexReader.query(
                  QUERY_DEFINITION,
                  QueryCursorOptions.empty(),
                  constantBatchSizeStrategy,
                  QueryOptimizationFlags.DEFAULT_OPTIONS));
      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getInvalidQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getInternallyFailedQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getNpeQueryCounter().count(), 0);
    }

    @Test
    public void testBadIntermediateQueryMetric()
        throws IOException, InvalidQueryException, InterruptedException {
      SearchIndexReader mockIndexReader = mock(SearchIndexReader.class);
      var constantBatchSizeStrategy = new ConstantBatchSizeStrategy();
      when(mockIndexReader.intermediateQuery(
              QUERY_DEFINITION,
              QueryCursorOptions.empty(),
              constantBatchSizeStrategy,
              QueryOptimizationFlags.DEFAULT_OPTIONS))
          .thenThrow(IOException.class);
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(mockIndexReader, queryingMetricsUpdater, () -> false);

      Assert.assertThrows(
          IOException.class,
          () ->
              meteredIndexReader.intermediateQuery(
                  QUERY_DEFINITION,
                  QueryCursorOptions.empty(),
                  constantBatchSizeStrategy,
                  QueryOptimizationFlags.DEFAULT_OPTIONS));
      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getInvalidQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getInternallyFailedQueryCounter().count(), 0);
    }

    @Test
    public void testThrowingInvalidQueryException()
        throws IOException, InvalidQueryException, InterruptedException {
      SearchIndexReader mockIndexReader = mock(SearchIndexReader.class);
      var constantBatchSizeStrategy = new ConstantBatchSizeStrategy();
      when(mockIndexReader.query(
              QUERY_DEFINITION,
              QueryCursorOptions.empty(),
              constantBatchSizeStrategy,
              QueryOptimizationFlags.DEFAULT_OPTIONS))
          .thenThrow(InvalidQueryException.class);
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
      MeteredSearchIndexReader meteredIndexReader =
          new MeteredSearchIndexReader(mockIndexReader, queryingMetricsUpdater, () -> false);

      Assert.assertThrows(
          InvalidQueryException.class,
          () ->
              meteredIndexReader.query(
                  QUERY_DEFINITION,
                  QueryCursorOptions.empty(),
                  constantBatchSizeStrategy,
                  QueryOptimizationFlags.DEFAULT_OPTIONS));
      Assert.assertEquals(1, queryingMetricsUpdater.getTotalQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getFailedQueryCounter().count(), 0);
      Assert.assertEquals(1, queryingMetricsUpdater.getInvalidQueryCounter().count(), 0);
      Assert.assertEquals(0, queryingMetricsUpdater.getInternallyFailedQueryCounter().count(), 0);
    }
  }
}
