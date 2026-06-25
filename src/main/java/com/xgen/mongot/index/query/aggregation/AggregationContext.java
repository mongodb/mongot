package com.xgen.mongot.index.query.aggregation;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Shared state passed through the aggregation framework during planning and execution.
 *
 * <p>The same context flows through the entire spec / accumulator / result tree:
 *
 * <ul>
 *   <li>At plan time, specs read {@link #getRegistry()} to look up factory implementations and get
 *       FieldDefinition to discover field types when validating paths.
 *   <li>At execution time, the top-level accumulator's {@link
 *       com.xgen.mongot.index.query.aggregation.accumulator.BucketCollector#asCollector} wires the
 *       context into Lucene's query machinery. For every segment Lucene visits, the framework
 *       derives a per-segment view via {@link #withLeafReaderContext(LeafReaderContext)} so leaf
 *       collectors can resolve doc-values readers against the current segment.
 * </ul>
 *
 * <p>Instances are intended to be effectively immutable: {@link
 * #withLeafReaderContext(LeafReaderContext)} returns a derived context rather than mutating the
 * receiver. The same context can therefore be shared across concurrent leaf collectors safely.
 */
public interface AggregationContext {

  /**
   * Return a derived context bound to the given segment. Called by the framework once per segment
   * visited by Lucene; the returned context is what leaf collectors receive.
   */
  AggregationContext withLeafReaderContext(LeafReaderContext context);

  /**
   * Return the registry of aggregation factories. Plan-time code consults this to look up the
   * concrete {@code GroupSpecSupplier} / {@code OperatorSupplier} for a given aggregation name.
   */
  AggregationRegistry getRegistry();

  /**
   * Return the segment this context is currently bound to. Only valid after {@link
   * #withLeafReaderContext(LeafReaderContext)} has been called; throws otherwise.
   */
  LeafReaderContext getLeafReaderContext();

  /**
   * Return the top-level reader for the index being aggregated over. Used to inspect global state
   * (e.g. building a cross-segment ordinal map) before per-segment collection begins.
   */
  IndexReader getIndexReader();

  static AggregationContext createDefault(IndexReader indexReader) {
    return new AggregationContextImpl(indexReader, AggregationRegistry.getInstance());
  }
}
