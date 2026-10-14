package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;

final class AggregationContextImpl implements AggregationContext {
  private final IndexReader indexReader;
  private final AggregationRegistry registry;
  private final Optional<AggregationFieldResolver> fieldResolver;
  private final CachedGlobalValueLookup cachedGlobalValueLookup;
  private final Optional<Sort> sort;
  private final Optional<Sort> invertedSort;
  private final int partitionId;
  private final Optional<MultiValueDocMarker> multiValueDocMarker;
  private final Optional<LeafReaderContext> leafReaderContext;
  private final AtomicLong observedBuckets;
  private final boolean recordExplainNodeStats;
  private final AggregationHeapAccountant heapAccountant;

  private AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup,
      Optional<Sort> sort,
      Optional<Sort> invertedSort,
      int partitionId,
      Optional<MultiValueDocMarker> multiValueDocMarker,
      Optional<LeafReaderContext> leafReaderContext,
      AtomicLong observedBuckets,
      boolean recordExplainNodeStats,
      AggregationHeapAccountant heapAccountant) {
    Check.checkArg(
        sort.isPresent() == invertedSort.isPresent(),
        "sort and invertedSort must be present or absent together");
    this.indexReader = indexReader;
    this.registry = registry;
    this.fieldResolver = fieldResolver;
    this.cachedGlobalValueLookup = cachedGlobalValueLookup;
    this.sort = sort;
    this.invertedSort = invertedSort;
    this.partitionId = partitionId;
    this.multiValueDocMarker = multiValueDocMarker;
    this.leafReaderContext = leafReaderContext;
    this.observedBuckets = observedBuckets;
    this.recordExplainNodeStats = recordExplainNodeStats;
    this.heapAccountant = heapAccountant;
  }

  AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup,
      Optional<Sort> sort,
      Optional<Sort> invertedSort,
      int partitionId,
      boolean recordExplainNodeStats,
      AggregationHeapAccountant heapAccountant) {
    this(
        indexReader,
        registry,
        fieldResolver,
        cachedGlobalValueLookup,
        sort,
        invertedSort,
        partitionId,
        Optional.empty(),
        Optional.empty(),
        new AtomicLong(),
        recordExplainNodeStats,
        heapAccountant);
  }

  @Override
  public AggregationContext withLeafReaderContext(LeafReaderContext context) {
    return new AggregationContextImpl(
        this.indexReader,
        this.registry,
        this.fieldResolver,
        this.cachedGlobalValueLookup,
        this.sort,
        this.invertedSort,
        this.partitionId,
        this.multiValueDocMarker,
        Optional.of(context),
        this.observedBuckets,
        this.recordExplainNodeStats,
        this.heapAccountant);
  }

  @Override
  public AggregationContext withMultiValueDocMarker(MultiValueDocMarker marker) {
    return new AggregationContextImpl(
        this.indexReader,
        this.registry,
        this.fieldResolver,
        this.cachedGlobalValueLookup,
        this.sort,
        this.invertedSort,
        this.partitionId,
        Optional.of(marker),
        this.leafReaderContext,
        this.observedBuckets,
        this.recordExplainNodeStats,
        this.heapAccountant);
  }

  @Override
  public AggregationHeapAccountant getHeapAccountant() {
    return this.heapAccountant;
  }

  @Override
  public int getPartitionId() {
    return this.partitionId;
  }

  @Override
  public Optional<Sort> getSort() {
    return this.sort;
  }

  @Override
  public Optional<Sort> getInvertedSort() {
    return this.invertedSort;
  }

  @Override
  public AggregationRegistry getRegistry() {
    return this.registry;
  }

  @Override
  public Optional<AggregationFieldResolver> getFieldResolver() {
    return this.fieldResolver;
  }

  @Override
  public Optional<MultiValueDocMarker> getMultiValueDocMarker() {
    return this.multiValueDocMarker;
  }

  @Override
  public CachedGlobalValueLookup getCachedGlobalValueLookup() {
    return this.cachedGlobalValueLookup;
  }

  @Override
  public LeafReaderContext getLeafReaderContext() {
    return this.leafReaderContext.orElseThrow(
        () -> new IllegalStateException("not bound to a leaf reader"));
  }

  @Override
  public IndexReader getIndexReader() {
    return this.indexReader;
  }

  @Override
  public void addObservedBuckets(long count) {
    this.observedBuckets.addAndGet(count);
  }

  @Override
  public long observedBuckets() {
    return this.observedBuckets.get();
  }

  @Override
  public boolean recordExplainNodeStats() {
    return this.recordExplainNodeStats;
  }
}
