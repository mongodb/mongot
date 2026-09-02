package com.xgen.testing.mongot.index.lucene.vector;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Arrays;
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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.Bits;

/**
 * Base for tests running Lucene's {@link BaseKnnVectorsFormatTestCase} suite against a mongot HNSW
 * format, relaxing the assertions that only hold under Lucene's graph-threshold policy.
 *
 * <p>Mongot formats pass {@code tinySegmentsThreshold = 0}, since graph-less segments break 9.11 /
 * 10.1 rollback. Segments Lucene would scan exhaustively and exactly are therefore searched
 * approximately here, and may return fewer than k hits (CLOUDP-438424).
 *
 * <p>Relaxed methods are copied from Lucene and marked {@code RELAXED FROM UPSTREAM}; {@link
 * LucenePinCanary} forces a re-diff when the Lucene pin moves.
 */
public abstract class MongotBaseKnnVectorsFormatTestCase extends BaseKnnVectorsFormatTestCase {

  /**
   * Vectors indexed by {@link #testRandomWithUpdatesAndGraph}; the query vector still uses
   * Lucene's. Override when the format needs another distribution, e.g. binary quantization needs
   * negatives.
   */
  protected float[] randomIndexedVector(int dimension) {
    return randomNormalizedVector(dimension);
  }

  /** Copied from Lucene 10.4.0, with the hit count relaxed; see the class javadoc. */
  @Override
  public void testRandomWithUpdatesAndGraph() throws Exception {
    LucenePinCanary.assertLucenePinUnchanged(
        MongotBaseKnnVectorsFormatTestCase.class.getSimpleName());

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
          value = randomIndexedVector(dimension);
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
          @Var int docId;
          @Var int numLiveDocsWithVectors = 0;
          DocIndexIterator iterator = vectorValues.iterator();
          while (true) {
            if (!((docId = iterator.nextDoc()) != NO_MORE_DOCS)) {
              break;
            }
            float[] v = vectorValues.vectorValue(iterator.index());
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
          float[] query = randomNormalizedVector(dimension);
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName,
                      query,
                      k,
                      AcceptDocs.fromLiveDocs(liveDocs, ctx.reader().maxDoc()),
                      Integer.MAX_VALUE);
          // RELAXED FROM UPSTREAM: this search is approximate and may return fewer than k.
          for (int i = 0; i < results.scoreDocs.length - 1; i++) {
            assertTrue(results.scoreDocs[i].score >= results.scoreDocs[i + 1].score);
          }

          // Requesting k = size makes Lucene99HnswVectorsReader skip the hnsw graph,
          // so this search is exhaustive and must return every live vector,
          // in descending score order.
          TopDocs exhaustive =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName,
                      query,
                      size,
                      AcceptDocs.fromLiveDocs(liveDocs, ctx.reader().maxDoc()),
                      Integer.MAX_VALUE);
          assertEquals(numLiveDocsWithVectors, exhaustive.scoreDocs.length);
          for (int i = 0; i < exhaustive.scoreDocs.length - 1; i++) {
            assertTrue(exhaustive.scoreDocs[i].score >= exhaustive.scoreDocs[i + 1].score);
          }
          assertOffHeapByteSize(ctx.reader(), fieldName);
        }
      }
    }
  }

  /** Copied from Lucene's private {@code add()}. */
  private void add2(
      IndexWriter iw,
      String field,
      int id,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    add2(iw, field, id, random().nextInt(100), vector, similarityFunction);
  }

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
