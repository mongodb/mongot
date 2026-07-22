package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;

public class SearchAggregationBootstrapTest {

  private static final FieldPath PRICE = FieldPath.parse("price");

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
  public void installRegistry_sumWithoutPath_throws() {
    SearchAggregationBootstrap.installRegistry();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AggregationRegistry.getInstance()
                .getOperatorSupplier(SearchAggregationBootstrap.SUM)
                .build("sum_price", Optional.empty()));
  }
}
