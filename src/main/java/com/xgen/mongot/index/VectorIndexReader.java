package com.xgen.mongot.index;

import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import java.io.IOException;
import org.bson.BsonArray;

public interface VectorIndexReader extends IndexReader {

  class VectorProducerAndMetaResults {
    public final BatchProducer vectorBatchProducer;
    public final MetaResults metaResults; // unused for vector search

    /**
     * Combines vector search results (a batch producer) with meta results.
     *
     * @param vectorBatchProducer The batch producer for vector search results.
     * @param metaResults Currently unused for vector search. Future use: counts, explains, etc.
     */
    public VectorProducerAndMetaResults(
        BatchProducer vectorBatchProducer, MetaResults metaResults) {
      this.vectorBatchProducer = vectorBatchProducer;
      this.metaResults = metaResults;
    }
  }

  /**
   * Returns the number of root (parent) Lucene documents in a nested vector index. For non-nested
   * indexes, returns 0. This is used to compute an accurate vector count: {@code numDocs -
   * numEmbeddedRootDocuments} equals the number of actual vectors.
   */
  default long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    return 0L;
  }

  /** Returns a bson array of {@link VectorSearchResult} (single-batch results with no cursor). */
  BsonArray query(MaterializedVectorSearchQuery materializedQuery, QueryExecutionContext context)
      throws ReaderClosedException, IOException, InvalidQueryException;

  /**
   * Returns a VectorSearchBatchProducer that provides results that match the query, along with a
   * currently-unused MetaResults for the query.
   */
  VectorProducerAndMetaResults query(
      MaterializedVectorSearchQuery query,
      QueryExecutionContext context,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException;
}
