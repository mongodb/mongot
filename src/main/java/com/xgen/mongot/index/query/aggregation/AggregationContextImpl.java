package com.xgen.mongot.index.query.aggregation;

import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

final class AggregationContextImpl implements AggregationContext {
  private final IndexReader indexReader;
  private final AggregationRegistry registry;
  private final Optional<LeafReaderContext> leafReaderContext;

  private AggregationContextImpl(
      IndexReader indexReader,
      AggregationRegistry registry,
      Optional<LeafReaderContext> leafReaderContext) {
    this.indexReader = indexReader;
    this.registry = registry;
    this.leafReaderContext = leafReaderContext;
  }

  AggregationContextImpl(IndexReader indexReader, AggregationRegistry registry) {
    this(indexReader, registry, Optional.empty());
  }

  @Override
  public AggregationContext withLeafReaderContext(LeafReaderContext context) {
    return new AggregationContextImpl(this.indexReader, this.registry, Optional.of(context));
  }

  @Override
  public AggregationRegistry getRegistry() {
    return this.registry;
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
