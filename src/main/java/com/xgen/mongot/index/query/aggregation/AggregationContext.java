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

  /**
   * Resolves aggregation metric paths to Lucene doc-values fields. Present at Lucene execution
   * time; empty in unit tests that supply raw field names directly.
   */
  Optional<AggregationFieldResolver> getFieldResolver();

  /** Marker for skipping metrics when an ancestor group field is multi-valued on a doc. */
  Optional<MultiValueDocMarker> getMultiValueDocMarker();

  /** Return a derived context that shares state with {@code marker} for this facet branch. */
  AggregationContext withMultiValueDocMarker(MultiValueDocMarker marker);

  /** Mark {@code doc} when a group field fans it into multiple buckets in this facet branch. */
  default void markMultiValued(int doc) {
    getMultiValueDocMarker().ifPresent(marker -> marker.markMultiValued(doc));
  }

  /** Whether {@code doc} was marked multi-valued by an ancestor group in this facet branch. */
  default boolean isMultiValued(int doc) {
    return getMultiValueDocMarker().map(marker -> marker.isMultiValued(doc)).orElse(false);
  }

  static AggregationContext createDefault(IndexReader indexReader) {
    return create(indexReader, Optional.empty());
  }

  static AggregationContext createDefault(
      IndexReader indexReader, CachedGlobalValueLookup cachedGlobalValueLookup) {
    return create(indexReader, Optional.empty(), cachedGlobalValueLookup);
  }

  /**
   * Creates an aggregation context for the given index reader.
   *
   * <p>{@code fieldResolver} is empty in unit tests that wire Lucene field names directly; Lucene
   * execution supplies a pre-resolved resolver for metric operators.
   */
  static AggregationContext create(
      IndexReader indexReader, Optional<AggregationFieldResolver> fieldResolver) {
    return create(indexReader, fieldResolver, luceneField -> Optional.empty());
  }

  static AggregationContext create(
      IndexReader indexReader,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup) {
    return new AggregationContextImpl(
        indexReader, AggregationRegistry.getInstance(), fieldResolver, cachedGlobalValueLookup);
  }
}
