package com.xgen.mongot.index.lucene.explain.knn;

import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/** A wrapper around TopKnnCollector for query instrumentation. */
public class InstrumentedTopKnnCollector extends TopKnnCollector {

  private final int docBase;
  private final KnnInstrumentationHelper instrumentationHelper;

  public InstrumentedTopKnnCollector(
      int docBase,
      KnnInstrumentationHelper instrumentationHelper,
      int k,
      int visitLimit,
      KnnSearchStrategy searchStrategy) {
    super(k, visitLimit, searchStrategy);
    this.docBase = docBase;
    this.instrumentationHelper = instrumentationHelper;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    this.instrumentationHelper.examineCollect(this.docBase + docId, similarity);
    return super.collect(docId, similarity);
  }
}
