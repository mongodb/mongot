package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.junit.Test;

/** Tests that AggregationSpec implementations expose the framework contract correctly. */
public class SpecTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath PRICE = FieldPath.parse("price");

  /**
   * Stub context -- the inline spec/accumulator fixtures store it but never call any method on it,
   * so unsupported-operation throws are fine. SpecTest only checks how specs wire accumulators
   * together; no Lucene scan runs here.
   */
  private static AggregationContext stubContext(Optional<MultiValueDocMarker> multiValueDocMarker) {
    return new AggregationContext() {
      @Override
      public AggregationContext withLeafReaderContext(LeafReaderContext context) {
        throw new UnsupportedOperationException();
      }

      @Override
      public AggregationRegistry getRegistry() {
        return AggregationRegistry.getInstance();
      }

      @Override
      public CachedGlobalValueLookup getCachedGlobalValueLookup() {
        throw new UnsupportedOperationException();
      }

      @Override
      public LeafReaderContext getLeafReaderContext() {
        throw new UnsupportedOperationException();
      }

      @Override
      public IndexReader getIndexReader() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Optional<AggregationFieldResolver> getFieldResolver() {
        return Optional.empty();
      }

      @Override
      public Optional<Sort> getSort() {
        return Optional.empty();
      }

      @Override
      public Optional<Sort> getInvertedSort() {
        return Optional.empty();
      }

      @Override
      public int getPartitionId() {
        return 0;
      }

      @Override
      public Optional<MultiValueDocMarker> getMultiValueDocMarker() {
        return multiValueDocMarker;
      }

      @Override
      public AggregationContext withMultiValueDocMarker(MultiValueDocMarker marker) {
        return stubContext(Optional.of(marker));
      }
    };
  }

  private static final AggregationContext STUB_CONTEXT = stubContext(Optional.empty());

  @Test
  public void operatorSpecExposesMetadata() {
    SumSpec spec = new SumSpec("sum_price", PRICE);
    assertEquals("sum_price", spec.name());
    assertTrue(spec.children().isEmpty());
    assertEquals(PRICE, spec.getPath());
    assertEquals(Operator.SUM, spec.getOperator());
  }

  @Test
  public void groupSpecExposesMetadataAndChildren() {
    AggregationSpec child = new SumSpec("sum_price", PRICE);
    TermsSpec spec = new TermsSpec("by_city", CITY, List.of(child));
    assertEquals("by_city", spec.name());
    assertEquals(CITY, spec.getPath());
    assertEquals(1, spec.children().size());
    assertSame(child, spec.children().get(0));
  }

  @Test
  public void topAggregationSpecHasNoName() {
    TopAggregationSpec top = new TopAggregationSpec(List.of());
    assertThrows(UnsupportedOperationException.class, top::name);
  }

  @Test
  public void operatorSpecBuildsExpectedAccumulator() throws IOException {
    SumSpec spec = new SumSpec("sum_price", PRICE);
    AggregationContext context = STUB_CONTEXT;
    Accumulator parent = new TopAggregationSpec(List.of(spec)).buildAccumulator(context);
    Accumulator childAccumulator = parent.children().get(0);
    assertTrue(childAccumulator instanceof SumAccumulator);
    assertEquals("sum_price", childAccumulator.getName());
    assertSame(parent, childAccumulator.parent().orElseThrow());
  }

  @Test
  public void groupSpecBuildsExpectedAccumulatorWithChildren() throws IOException {
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);
    TermsSpec spec = new TermsSpec("by_city", CITY, List.of(leaf));

    AggregationContext context = STUB_CONTEXT;
    Accumulator topAccumulator = new TopAggregationSpec(List.of(spec)).buildAccumulator(context);
    Accumulator group = topAccumulator.children().get(0);

    assertTrue(group instanceof TermsAccumulator);
    assertEquals("by_city", group.getName());
    assertEquals(1, group.children().size());
    assertEquals("sum_price", group.children().get(0).getName());
  }

  @Test
  public void compositionPreservesTreeStructure() {
    AggregationSpec leaf1 = new SumSpec("sum_price", PRICE);
    AggregationSpec leaf2 = new SumSpec("sum_other", PRICE);
    GroupAggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf1, leaf2));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    assertEquals(1, top.children().size());
    assertEquals(2, top.children().get(0).children().size());

    OperatorAggregationSpec firstLeaf = (OperatorAggregationSpec) group.children().get(0);
    assertEquals("sum_price", firstLeaf.name());
  }

  @Test
  public void multipleNestedGroupChildren_rejected() {
    // Defends the sharded merge planner's assumption of at most one nested grouping child per
    // level; parse-time validation normally prevents this, so a bypass must fail loudly.
    GroupAggregationSpec branch1 = new TermsSpec("by_a", CITY, List.of());
    GroupAggregationSpec branch2 = new TermsSpec("by_b", CITY, List.of());

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new TermsSpec("by_city", CITY, List.of(branch1, branch2)));
    assertTrue(ex.getMessage().contains("at most one nested grouping child"));
  }

  @Test
  public void singleNestedGroupChildWithLeaves_allowed() {
    // One nested grouping child alongside metric leaves is within the invariant.
    GroupAggregationSpec nested = new TermsSpec("by_capacity", CITY, List.of());
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);

    GroupAggregationSpec group = new TermsSpec("by_city", CITY, List.of(nested, leaf));
    assertEquals(2, group.children().size());
  }

  // ===== minimal inline spec/accumulator fixtures =====

  private static final class SumSpec extends OperatorAggregationSpec {
    private final FieldPath path;

    SumSpec(String name, FieldPath path) {
      super(Operator.SUM, name);
      this.path = path;
    }

    public FieldPath getPath() {
      return this.path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      return new SumAccumulator(this.name, context);
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      throw new UnsupportedOperationException("not exercised in SpecTest");
    }
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    public int groupingBucketCount() {
      return 1;
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      return new TermsAccumulator(this.name, childAccumulators, context);
    }
  }

  private static final class SumAccumulator extends Accumulator {
    SumAccumulator(String name, AggregationContext context) {
      super(name, List.of(), context);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class TermsAccumulator extends BucketsAccumulator {
    TermsAccumulator(String name, List<Accumulator> childAccumulators, AggregationContext context) {
      super(name, childAccumulators, context);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(
        AggregationContext context, LeafBucketCollector children) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
      throw new UnsupportedOperationException();
    }
  }
}
