package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import org.bson.BsonArray;
import org.junit.Test;

public class MeteredVectorIndexReaderTest {

  private static final String MOCK_INDEX_NAME = "mockIndex";
  private static final FieldPath VECTOR_PATH = FieldPath.newRoot("vector.path");
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 0f, 0f}, NATIVE);

  private static MaterializedVectorSearchQuery autoQuery(int limit) {
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index(MOCK_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(limit)
                    .queryVector(QUERY_VECTOR)
                    .path(VECTOR_PATH)
                    .build())
            .build();
    return new MaterializedVectorSearchQuery(query, QUERY_VECTOR);
  }

  private static MaterializedVectorSearchQuery explicitQuery(int limit, int numCandidates) {
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index(MOCK_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(limit)
                    .numCandidates(numCandidates)
                    .queryVector(QUERY_VECTOR)
                    .path(VECTOR_PATH)
                    .build())
            .build();
    return new MaterializedVectorSearchQuery(query, QUERY_VECTOR);
  }

  @Test
  public void query_autoNumCandidates_recordsCounterAndHistogramOncePerQuery() throws Exception {
    VectorIndexReader inner = mock(VectorIndexReader.class);
    when(inner.query(any(), any())).thenReturn(new BsonArray());
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH);
    var reader = new MeteredVectorIndexReader(inner, metricsUpdater);

    reader.query(autoQuery(100), QueryExecutionContext.empty());

    // The inner reader stands in for the whole (possibly multi-partition) reader, so a single user
    // query must record the counter and histogram exactly once here.
    assertThat(metricsUpdater.getAutoNumCandidatesCounter().count()).isEqualTo(1.0);
    assertThat(metricsUpdater.getAutoNumCandidatesValues().count()).isEqualTo(1L);
    // clamp(8 * 100, 100, 10000) = 800
    assertThat(metricsUpdater.getAutoNumCandidatesValues().totalAmount()).isEqualTo(800.0);
  }

  @Test
  public void query_explicitNumCandidates_doesNotRecordAutoMetrics() throws Exception {
    VectorIndexReader inner = mock(VectorIndexReader.class);
    when(inner.query(any(), any())).thenReturn(new BsonArray());
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH);
    var reader = new MeteredVectorIndexReader(inner, metricsUpdater);

    reader.query(explicitQuery(100, 200), QueryExecutionContext.empty());

    assertThat(metricsUpdater.getAutoNumCandidatesCounter().count()).isEqualTo(0.0);
    assertThat(metricsUpdater.getAutoNumCandidatesValues().count()).isEqualTo(0L);
  }

  @Test
  public void query_failedAutoQuery_doesNotRecordAutoMetrics() throws Exception {
    VectorIndexReader inner = mock(VectorIndexReader.class);
    when(inner.query(any(), any())).thenThrow(new IOException("boom"));
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH);
    var reader = new MeteredVectorIndexReader(inner, metricsUpdater);

    assertThrows(
        IOException.class, () -> reader.query(autoQuery(100), QueryExecutionContext.empty()));

    assertThat(metricsUpdater.getAutoNumCandidatesCounter().count()).isEqualTo(0.0);
    assertThat(metricsUpdater.getAutoNumCandidatesValues().count()).isEqualTo(0L);
  }
}
