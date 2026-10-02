package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.EqOperator;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static SimpleClause eq(FieldPath path, String value) {
    return new SimpleClause(path, List.of(new EqOperator(new StringValue(value))));
  }

  /** Materializes an approximate query carrying {@code filterClause} on {@code queryPath}. */
  private static VectorSearchCriteria materializeFilter(
      FieldPath queryPath, Clause filterClause, Map<FieldPath, FieldPath> mapping) {
    return materializeFilter(queryPath, filterClause, mapping, null);
  }

  private static VectorSearchCriteria materializeFilter(
      FieldPath queryPath,
      Clause filterClause,
      Map<FieldPath, FieldPath> mapping,
      Set<FieldPath> embedSourcePaths) {
    VectorSearchQuery rawQuery =
        VectorQueryBuilder.builder()
            .index("default")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(queryPath)
                    .queryVector(QUERY_VECTOR)
                    .limit(1)
                    .numCandidates(100)
                    .filter(new VectorSearchFilter.ClauseFilter(filterClause))
                    .build())
            .build();
    return (embedSourcePaths == null
            ? new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping)
            : new MaterializedVectorSearchQuery(rawQuery, QUERY_VECTOR, mapping, embedSourcePaths))
        .materializedCriteria();
  }

  private static Clause remappedClause(VectorSearchCriteria criteria) {
    return ((VectorSearchFilter.ClauseFilter) criteria.filter().get()).clause();
  }

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
    assertThat(approximate.getNumCandidates()).hasValue(100);
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

  // CLOUDP-402522: child-level filter clauses must be remapped to the materialized-view filter
  // path so that the filter resolves against the embedded child documents at query time.

  @Test
  public void materializedCriteria_filter_remapsNestedChildPath() {
    FieldPath sourceFilter = FieldPath.parse("sections.section_name");
    FieldPath matViewFilter = FieldPath.parse("_autoEmbed.sections.section_name");

    VectorSearchCriteria materialized =
        materializeFilter(
            FieldPath.parse("sections.section_content"),
            eq(sourceFilter, "book_12_section_1"),
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                sourceFilter,
                matViewFilter));

    assertThat(((SimpleClause) remappedClause(materialized)).path()).isEqualTo(matViewFilter);
  }

  @Test
  public void materializedCriteria_filter_leavesTopLevelPathsUntouched() {
    FieldPath nestedFilter = FieldPath.parse("sections.section_name");
    FieldPath nestedMatView = FieldPath.parse("_autoEmbed.sections.section_name");
    FieldPath topLevelFilter = FieldPath.parse("name");

    Clause root =
        new AndClause(
            List.of(eq(nestedFilter, "book_12_section_1"), eq(topLevelFilter, "book_12")), true);

    VectorSearchCriteria materialized =
        materializeFilter(
            FieldPath.parse("sections.section_content"),
            root,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                nestedFilter,
                nestedMatView));

    List<Clause> children = ((AndClause) remappedClause(materialized)).getClauses();
    assertThat(((SimpleClause) children.get(0)).path()).isEqualTo(nestedMatView);
    assertThat(((SimpleClause) children.get(1)).path()).isEqualTo(topLevelFilter);
  }

  @Test
  public void materializedCriteria_filter_recursesIntoCompoundClauses() {
    FieldPath nestedFilter = FieldPath.parse("sections.section_name");
    FieldPath nestedMatView = FieldPath.parse("_autoEmbed.sections.section_name");

    Clause inner = new OrClause(List.of(eq(nestedFilter, "s1"), eq(nestedFilter, "s2")));

    VectorSearchCriteria materialized =
        materializeFilter(
            FieldPath.parse("sections.section_content"),
            inner,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                nestedFilter,
                nestedMatView));

    Clause remappedRoot = remappedClause(materialized);
    assertThat(remappedRoot).isInstanceOf(OrClause.class);
    for (Clause child : ((OrClause) remappedRoot).getClauses()) {
      assertThat(((SimpleClause) child).path()).isEqualTo(nestedMatView);
    }
  }

  @Test
  public void materializedCriteria_filter_emptyMappingIsPreserved() {
    VectorSearchCriteria materialized =
        materializeFilter(USER_PATH, eq(FieldPath.parse("name"), "book_12"), Map.of());

    // With an empty mapping the filter must be returned as-is; assert the structure is unchanged.
    assertThat(((SimpleClause) remappedClause(materialized)).path())
        .isEqualTo(FieldPath.parse("name"));
  }

  /**
   * When the user query path equals the filter clause path, the schema mapping entry for that path
   * is the AUTO_EMBED source-to-MV remap. Top-level auto-embed indexes may declare an explicit
   * FILTER field at the same source path; the filter clause must hit that FILTER field at the
   * source path rather than be misrouted through the AUTO_EMBED entry into the vector field.
   * Regression test for the {@code
   * testApproximateVectorQueryWithAutoEmbeddingFieldsMappingAndFilter} case in {@code
   * LuceneVectorQueryFactoryDistributorTest}.
   */
  @Test
  public void materializedCriteria_filter_pathEqualsVectorPath_isNotRemapped() {
    VectorSearchCriteria materialized =
        materializeFilter(USER_PATH, eq(USER_PATH, "laptop"), Map.of(USER_PATH, INTERNAL_PATH));

    assertThat(materialized.path()).isEqualTo(INTERNAL_PATH);
    assertThat(((SimpleClause) remappedClause(materialized)).path()).isEqualTo(USER_PATH);
  }

  /**
   * CLOUDP-402522: when an index declares more than one auto-embed (vector) field, a filter clause
   * targeting a non-queried auto-embed source path must not be remapped into that field's MV vector
   * path. The schema mapping carries a remap entry for every auto-embed field, so the embed source
   * path set (not just the single queried path) is needed to leave such clauses untouched.
   */
  @Test
  public void materializedCriteria_filter_pathIsOtherAutoEmbedSourcePath_isNotRemapped() {
    FieldPath queriedVectorSource = FieldPath.parse("sections.section_content");
    FieldPath otherVectorSource = FieldPath.parse("chapters.chapter_content");
    FieldPath nestedFilterSource = FieldPath.parse("sections.section_name");
    FieldPath nestedFilterMatView = FieldPath.parse("_autoEmbed.sections.section_name");

    // Filter on a different auto-embed source path must stay put; the genuine nested filter field
    // is still remapped to the MV filter path.
    Clause root =
        new AndClause(
            List.of(eq(otherVectorSource, "chapter_1"), eq(nestedFilterSource, "section_1")), true);

    VectorSearchCriteria materialized =
        materializeFilter(
            queriedVectorSource,
            root,
            Map.of(
                queriedVectorSource,
                FieldPath.parse("_autoEmbed.sections.section_content"),
                otherVectorSource,
                FieldPath.parse("_autoEmbed.chapters.chapter_content"),
                nestedFilterSource,
                nestedFilterMatView),
            Set.of(queriedVectorSource, otherVectorSource));

    List<Clause> children = ((AndClause) remappedClause(materialized)).getClauses();
    assertThat(((SimpleClause) children.get(0)).path()).isEqualTo(otherVectorSource);
    assertThat(((SimpleClause) children.get(1)).path()).isEqualTo(nestedFilterMatView);
  }
}
