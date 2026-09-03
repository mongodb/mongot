package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Sort;

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
 * receiver. The same context can therefore be shared across concurrent leaf collectors safely. The
 * observed-bucket total is the exception: it is shared across derived contexts so every search
 * slice of one partition search can contribute.
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

  /**
   * Index partition being queried, 0-based; the same value carried as {@code ScoreDoc.shardIndex}.
   *
   * <p>Always 0 for a non-partitioned index, and for every query against one; a partitioned index
   * has one reader per partition, each carrying its own id.
   *
   * <p>Doc ids are comparable only within a partition, so {@code first}/{@code last} order by
   * partition first. Every key from one partition shares this, leaving single-partition behavior
   * unchanged.
   */
  int getPartitionId();

  /**
   * The query's global sort. Empty when unsorted, where {@code first}/{@code last} use encounter
   * order.
   */
  Optional<Sort> getSort();

  /**
   * The query's global sort with every component ordered the other way, which {@code last} collects
   * under: a {@link org.apache.lucene.search.TopFieldCollector} only ever keeps a top document, and
   * {@code last} wants the bottom one.
   *
   * <p>Built by the query layer from the same sort spec as {@link #getSort}, via {@link
   * com.xgen.mongot.index.query.sort.SortSpec#invert()}, so both orientations resolve fields, types
   * and missing-value placement identically rather than one being re-derived from the other's
   * finished {@link Sort}.
   *
   * <p>Two inputs to that build are deliberately left out. A sequence token belongs to the forward
   * ordering -- its values are baked into each comparator -- and pagination bounds mean nothing to
   * a bucket's winner. The index sort is left out because an inverted sort never aligns with one
   * anyway, and passing it would overwrite the query's own {@code usesIndexSort} explain verdict.
   *
   * <p>Carries no doc-id tiebreak: the sort spec has no way to name one, and {@code last} needs the
   * undecorated sort for merging. {@code LastAccumulator} appends it for collection only.
   *
   * <p>Present exactly when {@link #getSort} is.
   */
  Optional<Sort> getInvertedSort();

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

  /**
   * Adds grouping buckets instantiated by this slice's {@code buildResults}. Shared across derived
   * contexts so every search slice of one partition search contributes to one total. Default is a
   * no-op for tests that do not assert the metric.
   */
  default void addObservedBuckets(long count) {}

  /**
   * Grouping buckets instantiated on this context for the current partition search, summed over
   * grouping nodes and search slices.
   */
  default long observedBuckets() {
    return 0L;
  }

  /**
   * Creates an aggregation context for the given index reader.
   *
   * <p>{@code fieldResolver} is empty in unit tests that wire Lucene field names directly; Lucene
   * execution supplies a pre-resolved resolver for metric operators. {@code sort} is the query's
   * global sort, which {@code first}/{@code last} order their buckets by, and {@code invertedSort}
   * is that same sort turned around for {@code last}; the two must be present or absent together.
   */
  static AggregationContext create(
      IndexReader indexReader,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup,
      Optional<Sort> sort,
      Optional<Sort> invertedSort,
      int partitionId) {
    return new AggregationContextImpl(
        indexReader,
        AggregationRegistry.getInstance(),
        fieldResolver,
        cachedGlobalValueLookup,
        sort,
        invertedSort,
        partitionId);
  }
}
