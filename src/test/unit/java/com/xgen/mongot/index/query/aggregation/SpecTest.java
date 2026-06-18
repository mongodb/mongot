package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
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
  private static final AggregationContext STUB_CONTEXT =
      new AggregationContext() {
        @Override
        public AggregationContext withLeafReaderContext(LeafReaderContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public FieldTypeDefinition resolveFieldDefinition(FieldPath path) {
          throw new UnsupportedOperationException();
        }

        @Override
        public AggregationRegistry getRegistry() {
          return AggregationRegistry.INSTANCE;
        }

        @Override
        public LeafReaderContext getLeafReaderContext() {
          throw new UnsupportedOperationException();
        }

        @Override
        public IndexReader getIndexReader() {
          throw new UnsupportedOperationException();
        }
      };

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
  public void operatorSpecBuildsExpectedAccumulator() {
    SumSpec spec = new SumSpec("sum_price", PRICE);
    AggregationContext context = STUB_CONTEXT;
    Accumulator parent =
        new TopAggregationSpec(List.of(spec)).buildAccumulator(context, Optional.empty());
    Accumulator childAccumulator = spec.buildAccumulator(context, Optional.of(parent));
    assertTrue(childAccumulator instanceof SumAccumulator);
    assertEquals("sum_price", childAccumulator.getName());
  }

  @Test
  public void groupSpecBuildsExpectedAccumulatorWithChildren() {
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);
    TermsSpec spec = new TermsSpec("by_city", CITY, List.of(leaf));

    AggregationContext context = STUB_CONTEXT;
    Accumulator topAccumulator =
        new TopAggregationSpec(List.of(spec)).buildAccumulator(context, Optional.empty());
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
    public Accumulator buildAccumulator(AggregationContext context, Optional<Accumulator> parent) {
      return new SumAccumulator(this.name, context, parent);
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
    protected Accumulator createAccumulator(
        AggregationContext context, Optional<Accumulator> parent) {
      return new TermsAccumulator(this.name, this.children, context, parent);
    }
  }

  private static final class SumAccumulator extends Accumulator {
    SumAccumulator(String name, AggregationContext context, Optional<Accumulator> parent) {
      super(name, parent, List.of(), context);
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
    TermsAccumulator(
        String name,
        List<AggregationSpec> childrenSpecs,
        AggregationContext context,
        Optional<Accumulator> parent) {
      super(name, parent, childrenSpecs, context);
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
