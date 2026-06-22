package com.xgen.mongot.index.lucene.vector;

import java.lang.foreign.Arena;
import java.util.Random;
import org.apache.lucene.util.VectorUtil;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class SimilarityTest {
  private static final long SEED = 0xdeadbeefL;
  // Use some dimensionalities that will test tail processing.
  @DataPoints public static final int[] DIMS = {4, 16, 64, 128, 256, 17, 257, 362, 81};

  private static byte[] randomBytes(Random rng, int n) {
    byte[] v = new byte[n];
    rng.nextBytes(v);
    return v;
  }

  @Theory
  public void dotI8MatchesReferenceImpl(int dim) {
    Random rng = new Random(SEED);
    try (Arena arena = Arena.ofConfined()) {
      for (int trial = 0; trial < 8; trial++) {
        byte[] a = randomBytes(rng, dim);
        byte[] b = randomBytes(rng, dim);
        int expected = VectorUtil.dotProduct(a, b);
        int actual =
            Similarity.dotI8(
                Similarity.prepareVector(arena, a), Similarity.prepareVector(arena, b));
        Assert.assertEquals("trial=" + trial + " dim=" + dim, expected, actual);
      }
    }
  }

  @Theory
  public void cosI8MatchesReferenceImpl(int dim) {
    Random rng = new Random(SEED);
    try (Arena arena = Arena.ofConfined()) {
      for (int trial = 0; trial < 8; trial++) {
        byte[] a = randomBytes(rng, dim);
        byte[] b = randomBytes(rng, dim);
        float expected = VectorUtil.cosine(a, b);
        float actual =
            Similarity.cosI8(
                Similarity.prepareVector(arena, a), Similarity.prepareVector(arena, b));
        Assert.assertEquals("trial=" + trial + " dim=" + dim, expected, actual, 1e-6f);
      }
    }
  }

  @Theory
  public void l2I8MatchesReferenceImpl(int dim) {
    Random rng = new Random(SEED);
    try (Arena arena = Arena.ofConfined()) {
      for (int trial = 0; trial < 8; trial++) {
        byte[] a = randomBytes(rng, dim);
        byte[] b = randomBytes(rng, dim);
        int expected = VectorUtil.squareDistance(a, b);
        int actual =
            Similarity.l2I8(Similarity.prepareVector(arena, a), Similarity.prepareVector(arena, b));
        Assert.assertEquals("trial=" + trial + " dim=" + dim, expected, actual);
      }
    }
  }
}
