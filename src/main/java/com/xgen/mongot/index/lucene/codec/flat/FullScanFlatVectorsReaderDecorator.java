package com.xgen.mongot.index.lucene.codec.flat;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

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
    super();
    this.reader = reader;
  }

  @Override
  public void checkIntegrity() throws IOException {
    this.reader.checkIntegrity();
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return this.reader.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return this.reader.getByteVectorValues(field);
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
    scanAllDocs(knnCollector, acceptDocs.bits(), this.reader.getRandomVectorScorer(field, target));
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
    scanAllDocs(knnCollector, acceptDocs.bits(), this.reader.getRandomVectorScorer(field, target));
  }

  /**
   * Scans all documents in the index and collects similarity scores.
   *
   * <p>This is the core exhaustive search implementation that iterates through all vector ordinals,
   * computes similarity scores, and collects results. This has O(n) complexity (N is the number of
   * vectors in the index)
   *
   * @param knnCollector collector for gathering the k-nearest neighbors
   * @param acceptDocs optional filter for which documents to consider
   * @param scorer the vector scorer for computing similarities
   * @throws IOException if an I/O error occurs
   */
  private void scanAllDocs(KnnCollector knnCollector, Bits acceptDocs, RandomVectorScorer scorer)
      throws IOException {
    OrdinalTranslatedKnnCollector collector =
        new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
    Optional<Bits> maybeAcceptedOrds = Optional.ofNullable(scorer.getAcceptOrds(acceptDocs));
    if (maybeAcceptedOrds.isEmpty()) {
      for (int i = 0; i < scorer.maxOrd(); i++) {
        collector.collect(i, scorer.score(i));
        collector.incVisitedCount(1);
      }
    } else {
      Bits bits = maybeAcceptedOrds.get();
      for (int i = 0; i < scorer.maxOrd(); i++) {
        if (bits.get(i)) {
          collector.collect(i, scorer.score(i));
          collector.incVisitedCount(1);
        }
      }
    }
  }
}
