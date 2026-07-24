package com.xgen.mongot.index.query.aggregation;

import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

final class AggregationContextImpl implements AggregationContext {
  private final IndexReader indexReader;
  private final AggregationRegistry registry;
  private final CachedGlobalValueLookup cachedGlobalValueLookup;
  private final Optional<LeafReaderContext> leafReaderContext;

  private AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      CachedGlobalValueLookup cachedGlobalValueLookup,
      Optional<LeafReaderContext> leafReaderContext) {
    this.indexReader = indexReader;
    this.registry = registry;
    this.cachedGlobalValueLookup = cachedGlobalValueLookup;
    this.leafReaderContext = leafReaderContext;
  }

  AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      CachedGlobalValueLookup cachedGlobalValueLookup) {
    this(indexReader, registry, cachedGlobalValueLookup, Optional.empty());
  }

  @Override
  public AggregationContext withLeafReaderContext(LeafReaderContext context) {
    return new AggregationContextImpl(
        this.indexReader, this.registry, this.cachedGlobalValueLookup, Optional.of(context));
  }

  @Override
  public AggregationRegistry getRegistry() {
    return this.registry;
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
}
