package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SearchAggregationBootstrapTest {

  private static final FieldPath PRICE = FieldPath.parse("price");

  // The registry is a global singleton. Clearing it before each test as well as after keeps
  // installRegistryIfEnabled_flagDisabled_leavesRegistryEmpty from depending on whether some other
  // test class left operators installed.
  @Before
  public void clearGlobalRegistry() {
    AggregationRegistry.install(Map.of(), Map.of());
  }

  @After
  public void resetGlobalRegistry() {
    AggregationRegistry.install(Map.of(), Map.of());
  }

  @Test
  public void installRegistryIfEnabled_flagDisabled_leavesRegistryEmpty() {
    SearchAggregationBootstrap.installRegistryIfEnabled(FeatureFlags.getDefault());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.SUM));
  }

  @Test
  public void installRegistryIfEnabled_flagEnabled_registersOperators() {
    SearchAggregationBootstrap.installRegistryIfEnabled(
        FeatureFlags.withDefaults().enable(Feature.SEARCH_AGGREGATION).build());

    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.SUM));
    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.AVG));
    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.FIRST));
    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.LAST));
  }

  @Test
  public void installRegistry_registersSumAndAvgOperators() {
    SearchAggregationBootstrap.installRegistry();

    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.SUM));
    assertNotNull(
        AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.AVG));

    OperatorAggregationSpec sumSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.SUM)
            .build("sum_price", Optional.of(PRICE));
    assertTrue(sumSpec instanceof SumAggregationSpec);
    assertEquals(PRICE, ((SumAggregationSpec) sumSpec).getPath());

    OperatorAggregationSpec avgSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.AVG)
            .build("avg_price", Optional.of(PRICE));
    assertTrue(avgSpec instanceof AvgAggregationSpec);
    assertEquals(PRICE, ((AvgAggregationSpec) avgSpec).getPath());
  }

  @Test
  public void installRegistry_registersMinAndMaxOperators() {
    SearchAggregationBootstrap.installRegistry();

    OperatorAggregationSpec minSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.MIN)
            .build("min_price", Optional.of(PRICE));
    assertTrue(minSpec instanceof MinAggregationSpec);
    assertEquals(PRICE, ((MinAggregationSpec) minSpec).getPath());

    OperatorAggregationSpec maxSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.MAX)
            .build("max_price", Optional.of(PRICE));
    assertTrue(maxSpec instanceof MaxAggregationSpec);
    assertEquals(PRICE, ((MaxAggregationSpec) maxSpec).getPath());
  }

  @Test
  public void installRegistryIfEnabled_flagDisabled_leavesMinMaxUnregistered() {
    SearchAggregationBootstrap.installRegistryIfEnabled(FeatureFlags.getDefault());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.MIN));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance().getOperatorSupplier(SearchAggregationBootstrap.MAX));
  }

  @Test
  public void installRegistry_minOrMaxWithoutPath_throws() {
    SearchAggregationBootstrap.installRegistry();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.MIN)
                .build("min_price", Optional.empty()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.MAX)
                .build("max_price", Optional.empty()));
  }

  @Test
  public void installRegistry_sumWithoutPath_throws() {
    SearchAggregationBootstrap.installRegistry();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.SUM)
                .build("sum_price", Optional.empty()));
  }

  @Test
  public void installRegistry_registersFirstAndLastOperators() {
    SearchAggregationBootstrap.installRegistry();

    OperatorAggregationSpec firstSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.FIRST)
            .build("first_price", Optional.of(PRICE));
    assertTrue(firstSpec instanceof FirstAggregationSpec);
    assertEquals(PRICE, ((FirstAggregationSpec) firstSpec).getPath());

    OperatorAggregationSpec lastSpec =
        AggregationRegistry.getInstance()
            .getOperatorSupplier(SearchAggregationBootstrap.LAST)
            .build("last_price", Optional.of(PRICE));
    assertTrue(lastSpec instanceof LastAggregationSpec);
    assertEquals(PRICE, ((LastAggregationSpec) lastSpec).getPath());
  }

  @Test
  public void installRegistry_firstWithoutPath_throws() {
    SearchAggregationBootstrap.installRegistry();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.FIRST)
                .build("first_price", Optional.empty()));
  }

  @Test
  public void installRegistry_lastWithoutPath_throws() {
    SearchAggregationBootstrap.installRegistry();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.LAST)
                .build("last_price", Optional.empty()));
  }

  /** Sharded merge is still deferred; single-shard accumulation is covered by its own tests. */
  @Test
  public void firstAndLastSpecs_shardMergeDeferred_throw() {
    FirstAggregationSpec first = new FirstAggregationSpec("first_price", PRICE);
    LastAggregationSpec last = new LastAggregationSpec("last_price", PRICE);

    // IllegalStateException so ShardedSearchPlanner wraps it into ShardedSearchPlannerException
    // rather than letting it escape planning as an uncaught internal error.
    ShardMergePipelineBuilder builder =
        ShardMergePipelineBuilder.create(
            ShardMergeAliasPlan.from(new TopAggregationSpec(List.of()), List.of()));
    assertThrows(IllegalStateException.class, () -> first.buildShardMergePipeline(builder));
    assertThrows(IllegalStateException.class, () -> last.buildShardMergePipeline(builder));
  }
}
