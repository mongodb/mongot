package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;

public class AggregationRegistryTest {

  @After
  public void resetGlobalRegistry() {
    // install() mutates the global singleton; restore the empty default so test order
    // cannot leak suppliers into other tests.
    AggregationRegistry.install(Map.of(), Map.of());
  }

  private static final FieldPath PRICE = FieldPath.parse("price");
  private static final FieldPath CITY = FieldPath.parse("city");

  /** Minimal inline operator spec used only to verify supplier wiring. */
  private static final class TestOpSpec extends OperatorAggregationSpec {
    private final FieldPath path;

    TestOpSpec(String name, FieldPath path) {
      super(Operator.SUM, name);
      this.path = path;
    }

    public FieldPath getPath() {
      return this.path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      throw new UnsupportedOperationException();
    }
  }

  /** Minimal inline group spec used only to verify supplier wiring. */
  private static final class TestGrpSpec extends GroupAggregationSpec {
    TestGrpSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void defaultInstanceHasEmptyRegistries() {
    // The framework-only PR ships with no production specs wired up.
    assertThrows(
        IllegalArgumentException.class,
        () -> AggregationRegistry.getInstance().getGroupSpecSupplier("anything"));
    assertThrows(
        IllegalArgumentException.class,
        () -> AggregationRegistry.getInstance().getOperatorSupplier("anything"));
  }

  @Test
  public void installRegistersGroupSupplier() {
    AggregationRegistry.install(Map.of("group", TestGrpSpec::new), Map.of());

    AggregationRegistry.GroupSpecSupplier supplier =
        AggregationRegistry.getInstance().getGroupSpecSupplier("group");
    assertNotNull(supplier);

    GroupAggregationSpec spec = supplier.build("by_city", CITY, List.of());
    assertEquals("by_city", spec.name());
    assertEquals(CITY, spec.getPath());
    assertTrue(spec.children().isEmpty());
  }

  @Test
  public void installRegistersOperatorSupplier() {
    AggregationRegistry.install(Map.of(), Map.of("op", TestOpSpec::new));

    AggregationRegistry.OperatorSupplier supplier =
        AggregationRegistry.getInstance().getOperatorSupplier("op");
    assertNotNull(supplier);

    OperatorAggregationSpec spec = supplier.build("avg_price", PRICE);
    assertEquals("avg_price", spec.name());
    assertEquals(PRICE, ((TestOpSpec) spec).getPath());
    assertEquals(Operator.SUM, spec.getOperator());
  }
}
