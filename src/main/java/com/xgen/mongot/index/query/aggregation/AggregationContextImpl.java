package com.xgen.mongot.index.query.aggregation;

import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

final class AggregationContextImpl implements AggregationContext {
  private final IndexReader indexReader;
  private final AggregationRegistry registry;
  private final Optional<AggregationFieldResolver> fieldResolver;
  private final CachedGlobalValueLookup cachedGlobalValueLookup;
  private final Optional<MultiValueDocMarker> multiValueDocMarker;
  private final Optional<LeafReaderContext> leafReaderContext;

  private AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup,
      Optional<MultiValueDocMarker> multiValueDocMarker,
      Optional<LeafReaderContext> leafReaderContext) {
    this.indexReader = indexReader;
    this.registry = registry;
    this.fieldResolver = fieldResolver;
    this.cachedGlobalValueLookup = cachedGlobalValueLookup;
    this.multiValueDocMarker = multiValueDocMarker;
    this.leafReaderContext = leafReaderContext;
  }

  AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      Optional<AggregationFieldResolver> fieldResolver,
      CachedGlobalValueLookup cachedGlobalValueLookup) {
    this(
        indexReader,
        registry,
        fieldResolver,
        cachedGlobalValueLookup,
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public AggregationContext withLeafReaderContext(LeafReaderContext context) {
    return new AggregationContextImpl(
        this.indexReader,
        this.registry,
        this.fieldResolver,
        this.cachedGlobalValueLookup,
        this.multiValueDocMarker,
        Optional.of(context));
  }

  @Override
  public AggregationContext withMultiValueDocMarker(MultiValueDocMarker marker) {
    return new AggregationContextImpl(
        this.indexReader,
        this.registry,
        this.fieldResolver,
        this.cachedGlobalValueLookup,
        Optional.of(marker),
        this.leafReaderContext);
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
}
