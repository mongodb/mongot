package com.xgen.mongot.index.lucene.codec.flat;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.jetbrains.annotations.Nullable;

/**
 * A decorator for {@link FlatVectorsReader} that implements exhaustive k-nearest neighbor search.
 *
 * <p>This implementation performs brute-force vector search by scanning all vectors in the index
 * without using HNSW graphs or other approximate search structures. Each {@link #search} operation
 * iterates through every vector and computes similarity scores to find the k-nearest neighbors,
 * guaranteeing 100% recall with O(n) query time complexity where n is the number of vectors.
 *
 * <p>Supports both float32 and byte vector types.
 */
class FullScanFlatVectorsReaderDecorator extends KnnVectorsReader {

  private final FlatVectorsReader reader;

  FullScanFlatVectorsReaderDecorator(FlatVectorsReader reader) {
    this.reader = reader;
  }

  @Override
  public void checkIntegrity() throws IOException {
    this.reader.checkIntegrity();
  }

  @Override
  @Nullable
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return this.reader.getFloatVectorValues(field);
  }

  @Override
  @Nullable
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return this.reader.getByteVectorValues(field);
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    return this.reader.getOffHeapByteSize(fieldInfo);
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * Executes an exhaustive k-nearest neighbor search for float32 vectors.
   *
   * <p>Scans all vectors in the specified field and computes similarity scores against the query
   * vector to identify the k-nearest neighbors.
   *
   * @param field the vector field to search
   * @param target the query vector in float32 format
   * @param knnCollector collector that gathers the k-nearest neighbors
   * @param acceptDocs optional bitset filter specifying which documents to consider
   * @throws IOException if an I/O error occurs during the search
   */
  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    @Nullable FloatVectorValues values = getFloatVectorValues(field);
    if (values != null) {
      VectorScorer scorer = values.scorer(target);
      scanAllDocs(knnCollector, acceptDocs, scorer);
    }
  }

  /**
   * Performs exhaustive k-nearest neighbor search for byte vectors.
   *
   * <p>This method scans all vectors in the index and computes similarity scores against the target
   * vector to find the k-nearest neighbors.
   *
   * @param field the vector field to search
   * @param target the query vector (byte)
   * @param knnCollector collector for gathering the k-nearest neighbors
   * @param acceptDocs optional filter for which documents to consider
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    @Nullable ByteVectorValues values = getByteVectorValues(field);
    if (values != null) {
      VectorScorer scorer = values.scorer(target);
      scanAllDocs(knnCollector, acceptDocs, scorer);
    }
  }

  /**
   * Scans all documents in the index and collects similarity scores.
   *
   * <p>This is the core exhaustive search implementation that iterates through all vector ordinals,
   * computes similarity scores, and collects results. This has O(n) complexity (N is the number of
   * vectors in the index)
   *
   * @param knnCollector collector for gathering the k-nearest neighbors
   * @param acceptDocs set of documents to score accounting for live documents and pre-filters
   * @param scorer the scorer for the flat vector values (ideally supporting off-heap and bulk
   *     scoring)
   * @throws IOException if an I/O error occurs
   */
  private void scanAllDocs(KnnCollector knnCollector, AcceptDocs acceptDocs, VectorScorer scorer)
      throws IOException {
    // Adapted from AbstractKnnQuery's exactSearch logic to work with a KnnCollector
    VectorScorer.Bulk bulkScorer = scorer.bulk(acceptDocs.iterator());
    DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();

    for (float maxScore = bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer);
        buffer.size > 0;
        maxScore = bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer)) {

      if (maxScore >= knnCollector.minCompetitiveSimilarity()) {
        for (int i = 0; i < buffer.size; ++i) {
          float score = buffer.features[i];
          int doc = buffer.docs[i];
          knnCollector.collect(doc, score);
        }
      }

      knnCollector.incVisitedCount(buffer.size);
      if (knnCollector.earlyTerminated()) {
        // We only score matched documents, so this should indicate that
        // we exceeded maxTimeMs, in which case we return early to free resources.
        return;
      }
    }
  }
}
