package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Shared state passed through the aggregation framework during execution.
 *
 * <p>The same context flows through the entire spec / accumulator / result tree:
 *
 * <ul>
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
   * Supplies pre-built top-level (cross-segment) doc values for a lucene field, typically backed by
   * a cache of global ordinal maps maintained across queries (e.g. the token facets state cache).
   * Returns {@link Optional#empty()} when no cached view is available for the field, in which case
   * the caller builds one from scratch.
   */
  @FunctionalInterface
  interface CachedGlobalValueLookup {
    Optional<SortedSetDocValues> lookup(String luceneField) throws IOException;
  }

  /**
   * Return the lookup for cached top-level doc values. Consulted before building a global ordinal
   * map from scratch, since that build is expensive on large indexes.
   */
  CachedGlobalValueLookup getCachedGlobalValueLookup();

  /**
   * Return a derived context bound to the given segment. Called by the framework once per segment
   * visited by Lucene; the returned context is what leaf collectors receive.
   */
  AggregationContext withLeafReaderContext(LeafReaderContext context);

  /**
   * Return the registry of aggregation factories. Execution-time code consults this to look up the
   * concrete {@code OperatorSupplier} for a given aggregation name.
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
    return createDefault(indexReader, luceneField -> Optional.empty());
  }

  static AggregationContext createDefault(
      IndexReader indexReader, CachedGlobalValueLookup cachedGlobalValueLookup) {
    return new AggregationContextImpl(
        indexReader, AggregationRegistry.getInstance(), cachedGlobalValueLookup);
  }
}
