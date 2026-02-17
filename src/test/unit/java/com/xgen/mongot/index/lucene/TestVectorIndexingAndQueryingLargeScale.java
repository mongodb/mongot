package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.VectorIndexingAndQueryingTestHarness.buildVectorIndexDefinition;

import com.googlecode.junittoolbox.ParallelParameterized;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.bson.Vector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

// Keep the parameters explicit so the test matrix is visible and easy to tweak.
@RunWith(ParallelParameterized.class)
public class TestVectorIndexingAndQueryingLargeScale {

  static final int NUM_VECTORS = 10000;
  static final int NUM_QUERIES = 100;
  static final int NUM_CANDIDATES = 500;
  static final int LIMIT = 50;
  static final int DIMENSIONS = 1024;

  final VectorSimilarity vectorSimilarity;
  final boolean exact;
  final int numPartitions;
  final Vector.VectorType vectorType;
  final VectorQuantization vectorQuantization;
  final VectorIndexingAndQueryingTestHarness testHarness;
  List<BsonDocument> docs;
  List<? extends Vector> queryVectors;

  public TestVectorIndexingAndQueryingLargeScale(TestCase testCase) {
    this.vectorSimilarity = testCase.similarity();
    this.exact = testCase.exact();
    this.numPartitions = testCase.numPartitions();
    this.vectorType = testCase.vectorType();
    this.vectorQuantization = testCase.vectorQuantization();
    this.testHarness = new VectorIndexingAndQueryingTestHarness();
  }

  record InternalTestParams(VectorSimilarity similarity, boolean exact, int numPartitions) {}

  record TestCase(
      VectorSimilarity similarity,
      boolean exact,
      int numPartitions,
      Vector.VectorType vectorType,
      VectorQuantization vectorQuantization) {}

  static List<InternalTestParams> getInternalTestParams() {
    return List.of(
        new InternalTestParams(VectorSimilarity.DOT_PRODUCT, false, 1),
        new InternalTestParams(VectorSimilarity.DOT_PRODUCT, true, 1),
        new InternalTestParams(VectorSimilarity.DOT_PRODUCT, false, 2),
        new InternalTestParams(VectorSimilarity.DOT_PRODUCT, true, 2),
        new InternalTestParams(VectorSimilarity.COSINE, false, 1),
        new InternalTestParams(VectorSimilarity.COSINE, true, 1),
        new InternalTestParams(VectorSimilarity.COSINE, false, 2),
        new InternalTestParams(VectorSimilarity.COSINE, true, 2),
        new InternalTestParams(VectorSimilarity.EUCLIDEAN, false, 1),
        new InternalTestParams(VectorSimilarity.EUCLIDEAN, true, 1),
        new InternalTestParams(VectorSimilarity.EUCLIDEAN, false, 2),
        new InternalTestParams(VectorSimilarity.EUCLIDEAN, true, 2));
  }

  @Parameterized.Parameters
  public static List<TestCase> data() {
    List<TestCase> params = new ArrayList<>();
    for (InternalTestParams base : getInternalTestParams()) {
      VectorSimilarity similarity = base.similarity();
      boolean exact = base.exact();
      int numPartitions = base.numPartitions();

      params.add(
          new TestCase(
              similarity, exact, numPartitions, Vector.VectorType.FLOAT, VectorQuantization.NONE));
      params.add(
          new TestCase(
              similarity, exact, numPartitions, Vector.VectorType.BYTE, VectorQuantization.NONE));
      params.add(
          new TestCase(
              similarity, exact, numPartitions, Vector.VectorType.BIT, VectorQuantization.NONE));
      params.add(
          new TestCase(
              similarity,
              exact,
              numPartitions,
              Vector.VectorType.FLOAT,
              VectorQuantization.SCALAR));
      params.add(
          new TestCase(
              similarity,
              exact,
              numPartitions,
              Vector.VectorType.FLOAT,
              VectorQuantization.BINARY));
    }
    return params;
  }

  @Test
  public void runTest() throws Exception {
    if (this.vectorType == Vector.VectorType.BIT
        && this.vectorSimilarity != VectorSimilarity.EUCLIDEAN) {
      return;
    }
    try (var harness = this.testHarness) {
      harness.setNumCandidatesAndLimit(NUM_CANDIDATES, LIMIT);
      if (this.vectorQuantization == VectorQuantization.BINARY) {
        // Random vectors don't perform well with binary quantization.
        // Increasing num candidates helps with getting a reasonable recall value.
        this.testHarness.setNumCandidatesAndLimit(1000, 10);
        // Also reduce the target recall for binary quantization.
        this.testHarness.setTargetRecall(0.6);
      }
      harness.setUp(
          buildVectorIndexDefinition(
              this.vectorSimilarity, DIMENSIONS, this.numPartitions, this.vectorQuantization));
      switch (this.vectorType) {
        case FLOAT -> {
          float[][] floatVectors =
              harness.generateNormalizedRandomFloatVectors(NUM_VECTORS, DIMENSIONS);
          this.docs =
              harness.createVectorDocs(
                  floatVectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
          this.queryVectors =
              Arrays.stream(harness.generateNormalizedRandomFloatVectors(NUM_QUERIES, DIMENSIONS))
                  .map(VectorIndexingAndQueryingTestHarness::fromNativeFloats)
                  .toList();
        }
        case BYTE -> {
          byte[][] byteVectors = harness.generateRandomByteVectors(NUM_VECTORS, DIMENSIONS);
          this.docs = harness.createVectorDocs(byteVectors, 0, Vector::fromBytes);
          this.queryVectors =
              Arrays.stream(harness.generateRandomByteVectors(NUM_QUERIES, DIMENSIONS))
                  .map(Vector::fromBytes)
                  .toList();
        }
        case BIT -> {
          int vectorLength = DIMENSIONS / Byte.SIZE;
          byte[][] byteVectors = harness.generateRandomByteVectors(NUM_VECTORS, vectorLength);
          this.docs = harness.createVectorDocs(byteVectors, 0, Vector::fromBits);
          this.queryVectors =
              Arrays.stream(harness.generateRandomByteVectors(NUM_QUERIES, vectorLength))
                  .map(Vector::fromBits)
                  .toList();
        }
      }
      harness.runTestWithRecallCheck(
          this.docs, this.queryVectors, this.exact, this.vectorSimilarity);
    }
  }
}
