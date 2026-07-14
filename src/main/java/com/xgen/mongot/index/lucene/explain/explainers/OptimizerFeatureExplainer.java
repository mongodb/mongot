package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects {@code QueryOptimizer} rule-firing names and surfaces them as {@code
 * optimizerRulesFired} on {@code SearchExplainInformation}.
 */
public class OptimizerFeatureExplainer implements FeatureExplainer {
  private final List<String> firedRules = new ArrayList<>();

  /** Append a rule name. Order is preserved and corresponds to the engine's firing order. */
  public void addRuleFiring(String ruleName) {
    this.firedRules.add(ruleName);
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    builder.optimizerRulesFired(this.firedRules);
  }
}
