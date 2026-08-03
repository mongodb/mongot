package com.xgen.mongot.index.lucene.vector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Lucene99NativeMemorySegmentVectorScorerTest {
  private static final long SEED = 0xdeadbeefL;
  private static final int NUM_VECTORS = 8;
  private static final float DELTA = 1e-5f;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static byte[] randomBytes(Random rng, int dim) {
    byte[] v = new byte[dim];
    rng.nextBytes(v);
    return v;
  }

  private static List<byte[]> randomVectors(Random rng, int count, int dim) {
    List<byte[]> vectors = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      vectors.add(randomBytes(rng, dim));
    }
    return vectors;
  }

  /**
   * Writes vectors packed end-to-end into an MMap-backed file and returns the open IndexInput. The
   * returned input implements {@link MemorySegmentAccessInput}. The MMap arena is tied to the
   * input, not the directory, so the directory may be closed immediately after opening.
   */
  private IndexInput writeVectors(List<byte[]> vectors, String filename) throws IOException {
    Path dirPath = this.tempFolder.newFolder(filename + "_dir").toPath();
    try (MMapDirectory dir = new MMapDirectory(dirPath);
        IndexOutput out = dir.createOutput(filename, IOContext.DEFAULT)) {
      for (byte[] v : vectors) {
        out.writeBytes(v, v.length);
      }
    }
    try (MMapDirectory dir = new MMapDirectory(dirPath)) {
      IndexInput input = dir.openInput(filename, IOContext.DEFAULT);
      Assert.assertTrue(
          "MMapDirectory must produce a MemorySegmentAccessInput",
          input instanceof MemorySegmentAccessInput);
      return input;
    }
  }

  /**
   * {@link ByteVectorValues} that also implements {@link HasIndexSlice}, pointing at a flat packed
   * {@link IndexInput} as the slice so the native scorer can obtain MemorySegments.
   */
  private static final class HasSliceByteVectorValues extends ByteVectorValues
      implements HasIndexSlice {
    private final List<byte[]> vectors;
    private final int dim;
    private final IndexInput slice;

    HasSliceByteVectorValues(List<byte[]> vectors, int dim, IndexInput slice) {
      this.vectors = vectors;
      this.dim = dim;
      this.slice = slice;
    }

    @Override
    public int size() {
      return this.vectors.size();
    }

    @Override
    public int dimension() {
      return this.dim;
    }

    @Override
    public byte[] vectorValue(int ord) {
      return this.vectors.get(ord);
    }

    @Override
    public ByteVectorValues copy() {
      return this;
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public IndexInput getSlice() {
      return this.slice;
    }
  }

  private static ByteVectorValues asHasIndexSlice(List<byte[]> vectors, int dim, IndexInput slice) {
    return new HasSliceByteVectorValues(vectors, dim, slice);
  }

  @Test
  public void testScorerSupplierNativePath() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      for (int dim : new int[] {4, 16, 64, 128, 256}) {
        List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
        try (IndexInput input = writeVectors(vectors, "sup_" + fn + "_" + dim)) {
          ByteVectorValues values = asHasIndexSlice(vectors, dim, input);
          RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

          UpdateableRandomVectorScorer s = supplier.scorer();
          for (int ord = 0; ord < NUM_VECTORS; ord++) {
            s.setScoringOrdinal(ord);
            for (int node = 0; node < NUM_VECTORS; node++) {
              Assert.assertEquals(
                  "fn=" + fn + " dim=" + dim + " ord=" + ord + " node=" + node,
                  fn.compare(vectors.get(ord), vectors.get(node)),
                  s.score(node),
                  DELTA);
            }
          }
        }
      }
    }
  }

  @Test
  public void testScorerSupplierCopy() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      int dim = 64;
      List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
      try (IndexInput input = writeVectors(vectors, "copy_" + fn)) {
        ByteVectorValues values = asHasIndexSlice(vectors, dim, input);
        RandomVectorScorerSupplier copy = scorer.getRandomVectorScorerSupplier(fn, values).copy();

        UpdateableRandomVectorScorer s = copy.scorer();
        for (int ord = 0; ord < NUM_VECTORS; ord++) {
          s.setScoringOrdinal(ord);
          for (int node = 0; node < NUM_VECTORS; node++) {
            Assert.assertEquals(
                "fn=" + fn + " ord=" + ord + " node=" + node,
                fn.compare(vectors.get(ord), vectors.get(node)),
                s.score(node),
                DELTA);
          }
        }
      }
    }
  }

  @Test
  public void testQueryScorerNativePath() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      for (int dim : new int[] {4, 16, 64, 128, 256}) {
        List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
        try (IndexInput input = writeVectors(vectors, "qry_" + fn + "_" + dim)) {
          ByteVectorValues values = asHasIndexSlice(vectors, dim, input);

          for (int q = 0; q < NUM_VECTORS; q++) {
            byte[] query = vectors.get(q);
            RandomVectorScorer s = scorer.getRandomVectorScorer(fn, values, query);
            for (int node = 0; node < NUM_VECTORS; node++) {
              Assert.assertEquals(
                  "fn=" + fn + " dim=" + dim + " q=" + q + " node=" + node,
                  fn.compare(query, vectors.get(node)),
                  s.score(node),
                  DELTA);
            }
          }
        }
      }
    }
  }

  /** Verifies that vectors not backed by MemorySegmentAccessInput fall through to the delegate. */
  @Test
  public void testFallbackToDelegate() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      int dim = 32;
      List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
      // fromBytes() does not implement HasIndexSlice — must delegate
      ByteVectorValues values = ByteVectorValues.fromBytes(vectors, dim);
      RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

      UpdateableRandomVectorScorer s = supplier.scorer();
      for (int ord = 0; ord < NUM_VECTORS; ord++) {
        s.setScoringOrdinal(ord);
        for (int node = 0; node < NUM_VECTORS; node++) {
          Assert.assertEquals(
              "fn=" + fn + " ord=" + ord + " node=" + node,
              fn.compare(vectors.get(ord), vectors.get(node)),
              s.score(node),
              DELTA);
        }
      }
    }
  }
}
