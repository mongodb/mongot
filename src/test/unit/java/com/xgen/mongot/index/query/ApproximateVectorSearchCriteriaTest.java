package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.junit.Test;

public class ApproximateVectorSearchCriteriaTest {

  private static final FieldPath PATH = FieldPath.newRoot("embedding");
  private static final Optional<Vector> QUERY_VECTOR =
      Optional.of(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE));

  @Test
  public void isAutoNumCandidates_returnsTrueWhenEmpty() {
    var criteria =
        new ApproximateVectorSearchCriteria(
            PATH,
            QUERY_VECTOR,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            10,
            Optional.empty(),
            Optional.empty(),
            false,
            Optional.empty());

    assertThat(criteria.isAutoNumCandidates()).isTrue();
  }

  @Test
  public void isAutoNumCandidates_returnsFalseWhenPresent() {
    var criteria =
        new ApproximateVectorSearchCriteria(
            PATH,
            QUERY_VECTOR,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            10,
            Optional.of(100),
            Optional.empty(),
            false,
            Optional.empty());

    assertThat(criteria.isAutoNumCandidates()).isFalse();
  }

  @Test
  public void getNumCandidates_throwsWhenAuto() {
    var criteria =
        new ApproximateVectorSearchCriteria(
            PATH,
            QUERY_VECTOR,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            10,
            Optional.empty(),
            Optional.empty(),
            false,
            Optional.empty());

    assertThrows(IllegalStateException.class, criteria::getNumCandidates);
  }

  @Test
  public void withNumCandidates_resolvesAutoMode() {
    var auto =
        new ApproximateVectorSearchCriteria(
            PATH,
            QUERY_VECTOR,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            10,
            Optional.empty(),
            Optional.empty(),
            false,
            Optional.empty());

    var resolved = auto.withNumCandidates(200);

    assertThat(resolved.isAutoNumCandidates()).isFalse();
    assertThat(resolved.getNumCandidates()).isEqualTo(200);
    assertThat(resolved.limit()).isEqualTo(10);
    assertThat(resolved.path()).isEqualTo(PATH);
  }

  @Test
  public void constructor_validatesLimitNotExceedingNumCandidates() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ApproximateVectorSearchCriteria(
                PATH,
                QUERY_VECTOR,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                100,
                Optional.of(10),
                Optional.empty(),
                false,
                Optional.empty()));
  }

  @Test
  public void constructor_allowsAutoWithAnyLimit() {
    // Auto mode should not throw even with large limit
    var criteria =
        new ApproximateVectorSearchCriteria(
            PATH,
            QUERY_VECTOR,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            10_000,
            Optional.empty(),
            Optional.empty(),
            false,
            Optional.empty());

    assertThat(criteria.isAutoNumCandidates()).isTrue();
    assertThat(criteria.limit()).isEqualTo(10_000);
  }
}
