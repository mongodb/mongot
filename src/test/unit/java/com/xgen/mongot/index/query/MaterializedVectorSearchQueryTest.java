package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for {@link MaterializedVectorSearchQuery}, focusing on the {@link
 * MaterializedVectorSearchQuery#materializedCriteria()} method and field path mapping when {@code
 * autoEmbeddingFieldsMapping} is not empty.
 */
public class MaterializedVectorSearchQueryTest {

  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath USER_PATH = FieldPath.parse("title");
  private static final FieldPath INTERNAL_PATH = FieldPath.parse("_autoEmbed.title");

  @Test
  public void materializedCriteria_approximateQuery_withFieldMapping_usesInternalPath() {
    // Arrange - user query with path "title"
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(USER_PATH)
                    .queryVector(QUERY_VECTOR)
                    .limit(10)
                    .numCandidates(100)
                    .build())
            .build();

    // Mapping: "title" → "_autoEmbed.title"
    Map<FieldPath, FieldPath> mapping = Map.of(USER_PATH, INTERNAL_PATH);
    MaterializedVectorSearchQuery materializedQuery =
        new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping);

    // Act
    VectorSearchCriteria materialized = materializedQuery.materializedCriteria();

    // Assert - the materialized criteria should use the internal path
    assertThat(materialized).isInstanceOf(ApproximateVectorSearchCriteria.class);
    ApproximateVectorSearchCriteria approximate = (ApproximateVectorSearchCriteria) materialized;
    assertThat(approximate.path()).isEqualTo(INTERNAL_PATH);
    assertThat(approximate.queryVector()).hasValue(QUERY_VECTOR);
    assertThat(approximate.limit()).isEqualTo(10);
    assertThat(approximate.numCandidates()).isEqualTo(100);
  }

  @Test
  public void materializedCriteria_exactQuery_withFieldMapping_usesInternalPath() {
    // Arrange - user query with path "title"
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .path(USER_PATH)
                    .queryVector(QUERY_VECTOR)
                    .limit(5)
                    .build())
            .build();

    // Mapping: "title" → "_autoEmbed.title"
    Map<FieldPath, FieldPath> mapping = Map.of(USER_PATH, INTERNAL_PATH);
    MaterializedVectorSearchQuery materializedQuery =
        new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping);

    // Act
    VectorSearchCriteria materialized = materializedQuery.materializedCriteria();

    // Assert - the materialized criteria should use the internal path
    assertThat(materialized).isInstanceOf(ExactVectorSearchCriteria.class);
    ExactVectorSearchCriteria exact = (ExactVectorSearchCriteria) materialized;
    assertThat(exact.path()).isEqualTo(INTERNAL_PATH);
    assertThat(exact.queryVector()).hasValue(QUERY_VECTOR);
    assertThat(exact.limit()).isEqualTo(5);
  }

  @Test
  public void materializedCriteria_withEmptyMapping_usesOriginalPath() {
    // Arrange - user query with path "title", but no mapping
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(USER_PATH)
                    .queryVector(QUERY_VECTOR)
                    .limit(10)
                    .numCandidates(100)
                    .build())
            .build();

    // Empty mapping
    MaterializedVectorSearchQuery materializedQuery =
        new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, Map.of());

    // Act
    VectorSearchCriteria materialized = materializedQuery.materializedCriteria();

    // Assert - the materialized criteria should use the original user path
    assertThat(materialized).isInstanceOf(ApproximateVectorSearchCriteria.class);
    ApproximateVectorSearchCriteria approximate = (ApproximateVectorSearchCriteria) materialized;
    assertThat(approximate.path()).isEqualTo(USER_PATH);
  }

  @Test
  public void materializedCriteria_withMappingForDifferentField_usesOriginalPath() {
    // Arrange - user query with path "title", but mapping is for "description"
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(USER_PATH)
                    .queryVector(QUERY_VECTOR)
                    .limit(10)
                    .numCandidates(100)
                    .build())
            .build();

    // Mapping for a different field: "description" → "_autoEmbed.description"
    Map<FieldPath, FieldPath> mapping =
        Map.of(FieldPath.parse("description"), FieldPath.parse("_autoEmbed.description"));
    MaterializedVectorSearchQuery materializedQuery =
        new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping);

    // Act
    VectorSearchCriteria materialized = materializedQuery.materializedCriteria();

    // Assert - the materialized criteria should use the original user path (no match in mapping)
    assertThat(materialized).isInstanceOf(ApproximateVectorSearchCriteria.class);
    ApproximateVectorSearchCriteria approximate = (ApproximateVectorSearchCriteria) materialized;
    assertThat(approximate.path()).isEqualTo(USER_PATH);
  }

  @Test
  public void internalPath_withMapping_returnsInternalPath() {
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(USER_PATH)
                    .queryVector(QUERY_VECTOR)
                    .limit(10)
                    .numCandidates(100)
                    .build())
            .build();

    Map<FieldPath, FieldPath> mapping = Map.of(USER_PATH, INTERNAL_PATH);
    MaterializedVectorSearchQuery materializedQuery =
        new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping);

    assertThat(materializedQuery.internalPath()).isEqualTo(INTERNAL_PATH);
  }
}
