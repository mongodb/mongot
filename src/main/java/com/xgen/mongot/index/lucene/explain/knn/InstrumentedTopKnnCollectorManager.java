package com.xgen.mongot.index.lucene.explain.knn;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.MultiLeafKnnCollector;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;

/**
 * Almost identical to TopKnnCollectorManager; differs in that it can return an
 * InstrumentedTopKnnCollector when the segment contains target documents for tracing.
 */
public class InstrumentedTopKnnCollectorManager implements KnnCollectorManager {

  private final int k;
  private final Optional<BlockingFloatHeap> globalScoreQueue;

  private final KnnInstrumentationHelper instrumentationHelper;

  public InstrumentedTopKnnCollectorManager(
      int k, IndexSearcher indexSearcher, KnnInstrumentationHelper instrumentationHelper) {
    boolean isMultiSegments = indexSearcher.getIndexReader().leaves().size() > 1;
    this.k = k;
    this.globalScoreQueue =
        isMultiSegments ? Optional.of(new BlockingFloatHeap(k)) : Optional.empty();
    this.instrumentationHelper = instrumentationHelper;
  }

  @Override
  public KnnCollector newCollector(int visitedLimit, LeafReaderContext context) throws IOException {

    var knnCollector = this.instrumentationHelper.getKnnCollector(context, this.k, visitedLimit);
    if (this.globalScoreQueue.isEmpty()) {
      return knnCollector;
    }
    return new MultiLeafKnnCollector(this.k, this.globalScoreQueue.get(), knnCollector);
  }
}
