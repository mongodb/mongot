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
  public void getNumCandidates_emptyWhenAuto() {
    assertThat(autoCriteria(10).getNumCandidates()).isEmpty();
  }

  @Test
  public void getNumCandidates_returnsRawValueWhenExplicit() {
    assertThat(explicitCriteria(10, 200).getNumCandidates()).hasValue(200);
  }

  @Test
  public void resolveNumCandidates_explicitReturnsRawValue() {
    assertThat(explicitCriteria(10, 200).resolveNumCandidates()).isEqualTo(200);
  }

  @Test
  public void resolveNumCandidates_autoBelowFloorClampsToMin() {
    // 8 * 5 = 40, below the 100 floor.
    assertThat(autoCriteria(5).resolveNumCandidates()).isEqualTo(100);
  }

  @Test
  public void resolveNumCandidates_autoMidRangeUsesRatio() {
    // 8 * 500 = 4000, within [100, 10000].
    assertThat(autoCriteria(500).resolveNumCandidates()).isEqualTo(4000);
  }

  @Test
  public void resolveNumCandidates_autoAboveCeilingClampsToMax() {
    // 8 * 5000 = 40000, above the 10000 ceiling.
    assertThat(autoCriteria(5000).resolveNumCandidates()).isEqualTo(10_000);
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

  @Test
  public void constructor_rejectsAutoWithLimitAboveMax() {
    // Auto mode has no valid numCandidates for a limit above the maximum.
    assertThrows(IllegalArgumentException.class, () -> autoCriteria(10_001));
  }

  private static ApproximateVectorSearchCriteria autoCriteria(int limit) {
    return new ApproximateVectorSearchCriteria(
        PATH,
        QUERY_VECTOR,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        limit,
        Optional.empty(),
        Optional.empty(),
        false,
        Optional.empty());
  }

  private static ApproximateVectorSearchCriteria explicitCriteria(int limit, int numCandidates) {
    return new ApproximateVectorSearchCriteria(
        PATH,
        QUERY_VECTOR,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        limit,
        Optional.of(numCandidates),
        Optional.empty(),
        false,
        Optional.empty());
  }
}
