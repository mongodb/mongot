package com.xgen.testing.mongot.upgrade;

import com.xgen.testing.mongot.integration.index.serialization.TestSpec;
import java.util.Set;

/** Derives the index names created by an upgrade-test instance. */
public final class UpgradeTestIndexNames {

  private UpgradeTestIndexNames() {}

  public static Set<String> expectedIndexNames(String testName, TestSpec testSpec) {
    if (testSpec.getType() == TestSpec.Type.AGGREGATION) {
      return Set.copyOf(
          testSpec.asAggregationTestSpec().getCollectionsForTestName(testName).keySet());
    }
    return Set.of(testName);
  }
}
