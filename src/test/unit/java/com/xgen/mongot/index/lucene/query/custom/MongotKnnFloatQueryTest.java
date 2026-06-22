package com.xgen.mongot.index.lucene.query.custom;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MongotKnnFloatQueryTest {

  @Test
  public void expectedVectorsVisited_decreasingSelectivity_increasesVisits() {
    assertEquals(14, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, 1.0));
    assertEquals(22, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, .5));
    assertEquals(36, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, .25));
    assertEquals(67, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, .10));
    assertEquals(106, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, .05));
    assertEquals(288, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 10, .01));
    assertEquals(76_928, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000_000, 10, 1e-5));
  }

  @Test
  public void expectedVectorsVisited_increasingK_increasesVisits() {
    assertEquals(3, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 1, .5));
    assertEquals(30, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 10, .5));
    assertEquals(300, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 100, .5));
    assertEquals(2_632, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 1_000, .5));
    assertEquals(9_528, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 10_000, .5));
  }

  @Test
  public void expectedVectorsVisited_increasingN_increasesVisits() {
    assertEquals(9, (int) MongotKnnFloatQuery.expectedVectorsVisited(10, 100, .5));
    assertEquals(78, (int) MongotKnnFloatQuery.expectedVectorsVisited(100, 100, .5));
    assertEquals(204, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000, 100, .5));
    assertEquals(300, (int) MongotKnnFloatQuery.expectedVectorsVisited(10_000, 100, .5));
    assertEquals(458, (int) MongotKnnFloatQuery.expectedVectorsVisited(1_000_000, 100, .5));
  }

  @Test
  public void expectedVectorsVisited_largeInput_doesNotOverflow() {
    assertEquals(
        1_055_896, (int) MongotKnnFloatQuery.expectedVectorsVisited(2_000_000_000, 10_000, .01));
  }

  @Test
  public void expectedUniqueSamples_zeroSamples_returnsZero() {
    assertEquals(0.0, MongotKnnFloatQuery.expectedUniqueSamples(1_000, 0), 0.0);
  }

  @Test
  public void expectedUniqueSamples_oneSample_returnsOne() {
    assertEquals(1.0, MongotKnnFloatQuery.expectedUniqueSamples(10, 1), 1e-9);
    assertEquals(1.0, MongotKnnFloatQuery.expectedUniqueSamples(1_000, 1), 1e-9);
    assertEquals(1.0, MongotKnnFloatQuery.expectedUniqueSamples(1_000_000, 1), 1e-9);
  }

  @Test
  public void expectedUniqueSamples_increasingNumSamples_increasesUniques() {
    assertEquals(9, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 10));
    assertEquals(48, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 50));
    assertEquals(95, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 100));
    assertEquals(393, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 500));
  }

  @Test
  public void expectedUniqueSamples_increasingN_convergesToNumSamples() {
    // For a fixed number of samples, a larger universe produces more unique hits,
    // converging toward numSamples as n grows (fewer collisions expected).
    assertEquals(39, (int) MongotKnnFloatQuery.expectedUniqueSamples(100, 50));
    assertEquals(48, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 50));
    assertEquals(49, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000_000, 50));
  }

  @Test
  public void expectedUniqueSamples_largeNumSamples_approachesN() {
    assertEquals(9, (int) MongotKnnFloatQuery.expectedUniqueSamples(10, 100));
    assertEquals(99, (int) MongotKnnFloatQuery.expectedUniqueSamples(100, 1_000));
    assertEquals(999, (int) MongotKnnFloatQuery.expectedUniqueSamples(1_000, 10_000));
  }
}
