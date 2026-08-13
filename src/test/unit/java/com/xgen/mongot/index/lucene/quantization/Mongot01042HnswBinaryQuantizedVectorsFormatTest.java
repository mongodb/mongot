package com.xgen.mongot.index.lucene.quantization;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

public class Mongot01042HnswBinaryQuantizedVectorsFormatTest extends BaseKnnVectorsFormatTestCase {
  private static final String LUCENE_PIN_REPO_PATH = "bazel/java/maven_search_query.MODULE.bazel";
  private static final Pattern LUCENE_FORK_VERSION_PATTERN =
      Pattern.compile("_LUCENE_FORK_VERSION = \"([^\"]+)\"");

  // tinySegmentsThreshold=0 forces HNSW for every segment; the base Lucene test asserts the
  // .vex file is non-empty even for tiny segments.
  private final KnnVectorsFormat format = new Mongot01042HnswBinaryQuantizedVectorsFormat();

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return Mongot01042HnswBinaryQuantizedVectorsFormatTest.this.format;
      }
    };
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  protected VectorSimilarityFunction randomSimilarity() {
    return VectorSimilarityFunction.EUCLIDEAN;
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase randomVector().
  public static float[] randomPositiveNegativeVector(int dim) {
    assert dim > 0;
    float[] v = new float[dim];
    @Var double squareSum = 0.0;
    // keep generating until we don't get a zero-length vector
    while (squareSum == 0.0) {
      squareSum = 0.0;
      for (int i = 0; i < dim; i++) {
        v[i] = (random().nextFloat() * 2.0f) - 1.0f;
        //        v[i] = random().nextFloat();
        squareSum += v[i] * v[i];
      }
    }
    return v;
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase randomNormalizedVector().
  public static float[] randomPositiveNegativeNormalizedVector(int dim) {
    float[] v = randomPositiveNegativeVector(dim);
    VectorUtil.l2normalize(v);
    return v;
  }

  /**
   * Canary that fails whenever the pinned Lucene version changes, as a reminder that the methods
   * copied from Lucene into this class may need upstream's changes ported. On failure, re-diff the
   * copied methods against Lucene's BaseKnnVectorsFormatTestCase, port any upstream test changes,
   * and then update the expected version here.
   */
  private static void assertLucenePinUnchanged() throws IOException {
    // TEST_SRCDIR and TEST_WORKSPACE locate the runfiles tree under bazel test; fall back to a
    // repo-relative path for non-bazel runners such as plain IDE JUnit run configurations.
    String testSrcdir = System.getenv("TEST_SRCDIR");
    String testWorkspace = System.getenv("TEST_WORKSPACE");
    Path lucenePinFile =
        testSrcdir != null && testWorkspace != null
            ? Path.of(testSrcdir, testWorkspace, LUCENE_PIN_REPO_PATH)
            : Path.of(LUCENE_PIN_REPO_PATH);
    Matcher lucenePinMatcher = LUCENE_FORK_VERSION_PATTERN.matcher(Files.readString(lucenePinFile));
    assertTrue(
        "Lucene version pin not found in " + lucenePinFile + "; update this canary",
        lucenePinMatcher.find());
    assertEquals(
        "The pinned Lucene version changed: re-diff the copied methods in this class against"
            + " Lucene's BaseKnnVectorsFormatTestCase and port any upstream test changes, then"
            + " update the expected version here",
        "10.4.0-3",
        lucenePinMatcher.group(1));
  }

  // Copied from Lucene 10.4.0's BaseKnnVectorsFormatTestCase to fix flaky test failures
  // CLOUDP-391552 and CLOUDP-432100. Lucene's testSearchWithVisitedLimit() asserts that the
  // relation is always GREATER_THAN_OR_EQUAL_TO, which holds for Lucene's codecs, but our valid
  // codec can rarely return EQUAL_TO: because mongot's binary quantization is simpler than
  // Lucene's BBQ and we have more collisions. So, here we copy the test function and tweak
  // the asserts to be more tolerant. Note that Lucene's add() is also copied into this file as
  // add2() because add() is private when it maybe should have been protected.
  @Override
  public void testSearchWithVisitedLimit() throws Exception {
    assertLucenePinUnchanged();

    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";

    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = 300;
      int dimension = 10;
      for (int i = 0; i < numDoc; i++) {
        float[] value;
        if (random().nextInt(7) != 3) {
          value = randomNormalizedVector(dimension);
        } else {
          value = null;
        }
        add2(iw, fieldName, i, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      iw.forceMerge(1);

      for (int i = 0; i < 30; i++) {
        int idToDelete = random().nextInt(numDoc);
        iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
      }

      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          AcceptDocs acceptDocs = AcceptDocs.fromLiveDocs(liveDocs, ctx.reader().maxDoc());
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }

          @Var int k = 5 + random().nextInt(45);
          @Var int visitedLimit = k + random().nextInt(5);
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, acceptDocs, visitedLimit);
          if (results.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
            int size = Lucene99HnswVectorsReader.EXHAUSTIVE_BULK_SCORE_ORDS;
            // visit limit is a "best effort" limit given our bulk scoring logic; assert that we
            // are within reasonable bounds
            assertTrue(
                results.totalHits.value() == visitedLimit
                    || results.totalHits.value()
                        <= ((visitedLimit + size - 1) / size) * ((long) size));
          } else {
            assertEquals(TotalHits.Relation.EQUAL_TO, results.totalHits.relation());
            assertTrue(results.totalHits.value() <= visitedLimit);
          }

          k = vectorValues.size();
          visitedLimit = k + 30;
          TopDocs secondResults =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, acceptDocs, visitedLimit);
          assertEquals(TotalHits.Relation.EQUAL_TO, secondResults.totalHits.relation());
          assertTrue(secondResults.totalHits.value() <= visitedLimit);
          assertOffHeapByteSize(ctx.reader(), fieldName);
        }
      }
    }
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase testRandomWithUpdatesAndGraph().
  // Note that Lucene's add() is also copied into this file as add2() because add() is private when
  // it maybe should have been protected.
  @Override
  public void testRandomWithUpdatesAndGraph() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      @Var int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      float[][] id2value = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        int id = random().nextInt(numDoc);
        float[] value;
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          value = randomPositiveNegativeNormalizedVector(dimension);
        } else {
          value = null;
        }
        id2value[id] = value;
        add2(iw, fieldName, id, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          StoredFields storedFields = ctx.reader().storedFields();
          DocIndexIterator iter = vectorValues.iterator();
          @Var int docId;
          @Var int numLiveDocsWithVectors = 0;
          while ((docId = iter.nextDoc()) != NO_MORE_DOCS) {
            float[] v = vectorValues.vectorValue(iter.index());
            assertEquals(dimension, v.length);
            String idString = storedFields.document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (liveDocs == null || liveDocs.get(docId)) {
              assertArrayEquals(
                  "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                  id2value[id],
                  v,
                  0);
              numLiveDocsWithVectors++;
            } else {
              if (id2value[id] != null) {
                assertFalse(Arrays.equals(id2value[id], v));
              }
            }
          }

          if (numLiveDocsWithVectors == 0) {
            continue;
          }

          // assert that searchNearestVectors returns the expected number of documents,
          // in descending score order
          int size = ctx.reader().getFloatVectorValues(fieldName).size();
          @Var int k = random().nextInt(size / 10 + 1) + 1;
          if (k > numLiveDocsWithVectors) {
            k = numLiveDocsWithVectors;
          }
          AcceptDocs acceptDocs = AcceptDocs.fromLiveDocs(liveDocs, ctx.reader().maxDoc());
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName,
                      randomNormalizedVector(dimension),
                      k,
                      acceptDocs,
                      Integer.MAX_VALUE);
          assertEquals(Math.min(k, size), results.scoreDocs.length);
          for (int i = 0; i < k - 1; i++) {
            assertTrue(results.scoreDocs[i].score >= results.scoreDocs[i + 1].score);
          }
          assertOffHeapByteSize(ctx.reader(), fieldName);
        }
      }
    }
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase add().
  private void add2(
      IndexWriter iw,
      String field,
      int id,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    add2(iw, field, id, random().nextInt(100), vector, similarityFunction);
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase add().
  private void add2(
      IndexWriter iw,
      String field,
      int id,
      int sortkey,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new KnnFloatVectorField(field, vector, similarityFunction));
    }
    doc.add(new NumericDocValuesField("sortkey", sortkey));
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    Term idTerm = new Term("id", idString);
    iw.updateDocument(idTerm, doc);
  }
}
