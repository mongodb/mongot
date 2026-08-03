package com.xgen.mongot.index.query.optimizer.rules;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.query.optimizer.RewriteRule;
import com.xgen.mongot.index.query.optimizer.RuleRegistry;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.junit.Test;

/** Wiring tests for {@link DefaultRuleRegistry}. */
public class DefaultRuleRegistryTest {

  /**
   * The per-rule firing counters in {@link IndexMetricsUpdater} are keyed by duplicated rule-name
   * literals (a package cycle keeps that class from referencing the rule constants), and the
   * rewrite engines skip incrementing when the counter lookup returns null — so a missing or
   * drifted entry fails silently at runtime. Every registered rule must have a counter.
   */
  @Test
  public void everyRegisteredRule_hasFiredCounter() {
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    RuleRegistry.Filtered rules = DefaultRuleRegistry.get().forContext(allFlagsEnabled());

    assertFalse("expected the default registry to contain rules", rules.isEmpty());
    Stream<RewriteRule> allRules =
        Stream.concat(rules.clauseRules().stream(), rules.operatorRules().stream());
    allRules.forEach(
        rule ->
            assertNotNull(
                "missing optimizer-rule-fired counter for rule: " + rule.name(),
                metrics.getOptimizerRuleFiredCounter(rule.name())));
  }

  /** Every dynamic feature flag enabled, so no registered rule is filtered out of the view. */
  private static DynamicFeatureFlagRegistry allFlagsEnabled() {
    List<DynamicFeatureFlagConfig> configs = new ArrayList<>();
    for (DynamicFeatureFlags flag : DynamicFeatureFlags.values()) {
      configs.add(
          new DynamicFeatureFlagConfig(
              flag.getName(), Phase.ENABLED, List.of(), List.of(), 0, Scope.GROUP));
    }
    return new DynamicFeatureFlagRegistry(
        Optional.of(configs), Optional.empty(), Optional.of(new ObjectId()), Optional.empty());
  }
}
