package com.xgen.mongot.index.lucene.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.util.VectorSearchUtil.NormalizedVector;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class VectorSearchUtilTest {

  // Remove after next reindexing.
  private static final VectorIndexCapabilities PRESERVE_SIMILARITY =
      new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 4);

  // Pinned to 5, the first version that normalizes cosine vectors. CURRENT_FEATURE_VERSION is still
  // 4, so this gate is inert in production until the index feature version is bumped.
  private static final VectorIndexCapabilities OPTIMIZE_SIMILARITY =
      new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 5);

  @DataPoints public static final Vector.VectorType[] allTypes = Vector.VectorType.values();

  @DataPoints public static final VectorSimilarity[] similarities = VectorSimilarity.values();

  @DataPoints
  public static final List<IndexCapabilities> capabilities =
      List.of(PRESERVE_SIMILARITY, OPTIMIZE_SIMILARITY);

  @Test
  public void normalizeIfNeeded_newCosineFloatVector_returnsUnitCopyWithDotProduct() {
    FloatVector vector =
        FloatVector.fromFloats(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector result =
        VectorSearchUtil.normalizeIfNeeded(vector, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);

    assertThat(((FloatVector) result.vector()).getFloatVector())
        .usingTolerance(TestUtils.EPSILON)
        .containsExactly(new float[] {0.18257418f, 0.36514837f, 0.5477225f, 0.73029673f})
        .inOrder();
    assertEquals(VectorSimilarity.DOT_PRODUCT, result.similarity());
  }

  @Test
  public void normalizeIfNeeded_newCosineFloatVector_leavesInputUntouched() {
    FloatVector vector =
        FloatVector.fromFloats(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector result =
        VectorSearchUtil.normalizeIfNeeded(vector, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);

    assertThat(result.vector()).isNotSameInstanceAs(vector);
    assertThat(vector.getFloatVector()).isEqualTo(new float[] {1.0f, 2.0f, 3.0f, 4.0f});
  }

  @Test
  public void normalizeIfNeeded_newCosineFloatVector_preservesOriginalType() {
    FloatVector vector =
        FloatVector.fromFloats(new float[] {3.0f, 4.0f}, FloatVector.OriginalType.BSON);

    NormalizedVector result =
        VectorSearchUtil.normalizeIfNeeded(vector, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);

    assertEquals(FloatVector.OriginalType.BSON, ((FloatVector) result.vector()).getOriginalType());
  }

  @Test
  public void normalizeIfNeeded_newDotProductFloatVector_returnsSameInstanceAndSimilarity() {
    FloatVector vector =
        FloatVector.fromFloats(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector result =
        VectorSearchUtil.normalizeIfNeeded(
            vector, VectorSimilarity.DOT_PRODUCT, OPTIMIZE_SIMILARITY);

    assertThat(result.vector()).isSameInstanceAs(vector);
    assertEquals(VectorSimilarity.DOT_PRODUCT, result.similarity());
  }

  @Test
  public void normalizeIfNeeded_newEuclideanFloatVector_returnsSameInstanceAndSimilarity() {
    FloatVector vector =
        FloatVector.fromFloats(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector result =
        VectorSearchUtil.normalizeIfNeeded(vector, VectorSimilarity.EUCLIDEAN, OPTIMIZE_SIMILARITY);

    assertThat(result.vector()).isSameInstanceAs(vector);
    assertEquals(VectorSimilarity.EUCLIDEAN, result.similarity());
  }

  @Theory
  public void normalizeIfNeeded_oldFloatVector_neverNormalize(VectorSimilarity sim) {
    FloatVector vector =
        FloatVector.fromFloats(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector result = VectorSearchUtil.normalizeIfNeeded(vector, sim, PRESERVE_SIMILARITY);

    assertThat(result.vector()).isSameInstanceAs(vector);
    assertThat(vector.getFloatVector()).isEqualTo(new float[] {1.0f, 2.0f, 3.0f, 4.0f});
    assertEquals(sim, result.similarity());
  }

  @Theory
  public void normalizeIfNeeded_byteVector_neverNormalize(
      VectorSimilarity sim, IndexCapabilities capabilities) {
    Vector vector = FloatVector.fromBytes(new byte[] {1, 2, 3, 4});

    NormalizedVector result = VectorSearchUtil.normalizeIfNeeded(vector, sim, capabilities);

    assertThat(result.vector()).isSameInstanceAs(vector);
    assertThat(result.vector().getBytes()).isEqualTo(new byte[] {1, 2, 3, 4});
    assertEquals(sim, result.similarity());
  }

  @Theory
  public void normalizeIfNeeded_bitVector_neverNormalize(
      VectorSimilarity sim, IndexCapabilities capabilities) {
    Vector vector = Vector.fromBits(new byte[] {1, 2, 3, 4});

    NormalizedVector result = VectorSearchUtil.normalizeIfNeeded(vector, sim, capabilities);

    assertThat(result.vector()).isSameInstanceAs(vector);
    assertThat(result.vector().getVectorType()).isEqualTo(Vector.VectorType.BIT);
    assertThat(result.vector().getBytes()).isEqualTo(new byte[] {1, 2, 3, 4});
    assertEquals(sim, result.similarity());
  }

  @Test
  public void normalizeIfNeeded_repeatedCallsOnSharedVector_alwaysProduceTheSameUnitVector() {
    FloatVector shared =
        FloatVector.fromFloats(new float[] {3.0f, 4.0f}, FloatVector.OriginalType.NATIVE);

    NormalizedVector first =
        VectorSearchUtil.normalizeIfNeeded(shared, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);
    NormalizedVector second =
        VectorSearchUtil.normalizeIfNeeded(shared, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);

    assertThat(((FloatVector) first.vector()).getFloatVector())
        .usingTolerance(TestUtils.EPSILON)
        .containsExactly(new float[] {0.6f, 0.8f})
        .inOrder();
    assertThat(((FloatVector) second.vector()).getFloatVector())
        .usingTolerance(TestUtils.EPSILON)
        .containsExactly(new float[] {0.6f, 0.8f})
        .inOrder();
  }

  @Test
  public void normalizeIfNeeded_concurrentCallsOnSharedVector_allResultsAreUnitLength()
      throws Exception {
    FloatVector shared =
        FloatVector.fromFloats(
            new float[] {3.0f, 4.0f, 12.0f, 84.0f}, FloatVector.OriginalType.NATIVE);
    int threads = 16;
    var pool = Executors.newFixedThreadPool(threads);
    try {
      var start = new CountDownLatch(1);
      List<Future<NormalizedVector>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return VectorSearchUtil.normalizeIfNeeded(
                      shared, VectorSimilarity.COSINE, OPTIMIZE_SIMILARITY);
                }));
      }
      start.countDown();

      for (Future<NormalizedVector> future : futures) {
        float[] result = ((FloatVector) future.get().vector()).getFloatVector();
        @Var double norm = 0;
        for (float component : result) {
          norm += component * component;
        }
        assertThat(norm).isWithin(1e-5).of(1.0);
      }
      assertThat(shared.getFloatVector()).isEqualTo(new float[] {3.0f, 4.0f, 12.0f, 84.0f});
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void bitSimilarity_vectorizedNoPadding_returnsNormalizedMatches() {
    byte[] a = {(byte) 0xFF, 0x00, 0x7A, 0x78};
    byte[] b = {(byte) 0xF7, 0x71, 0x7A, 0x38};

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8);

    // 24 bits match out of 32
    assertEquals(0.8125, result, 0.0001);
  }

  @Test
  public void bitSimilarity_oddLength_returnsNormalizedMatches() {
    byte[] a = {0x40, 0x7A, 0x78}; // Too small to be vectorized
    byte[] b = {0x51, 0x7A, 0x38};

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8);

    // 21 bits match out of 24
    assertEquals(0.875, result, 0.0001);
  }

  @Test
  public void bitSimilarity_withPadding_assumesPaddingMatches() {
    byte[] a = {0x40, 0x7A, 0x78};
    byte[] b = {0x51, 0x7A, 0x38};
    int padding = 4;

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8 - padding);

    // 21 bits match out of 24, but 4 matches are ignored due to padding so 17/20
    assertEquals(0.85, result, 0.0001);
  }

  @Test
  public void bitSimilarity_isMonotonicInMatchedBits() {
    // This test ensures that computing bit similarity with float32 division doesn't harm recall.
    // We can compute similarity more precisely, but this is only an issue if dim > 8192*512
    // See comment in BitRandomVectorScorer to see why we need to multiply MAX_DIMENSIONS by 8
    int maxDimension = VectorFieldSpecification.MAX_DIMENSIONS * Byte.SIZE;

    byte[] allZeros = new byte[VectorFieldSpecification.MAX_DIMENSIONS];
    byte[] v = allZeros.clone();

    @Var float lastScore = 1.01f;
    for (int i = 0; i <= maxDimension; ++i) {
      float result = VectorSearchUtil.bitSimilarity(allZeros, v, maxDimension);

      if (result < 0 || result >= lastScore) {
        Assert.fail(
            String.format(
                "For i=%d, result should be in range [0, %f) but was %f", i, lastScore, result));
      }
      lastScore = result;

      if (i < maxDimension) {
        v[i / 8] |= (byte) (1 << (i % 8));
      }
    }
  }
}
