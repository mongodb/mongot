package com.xgen.testing.mongot.upgrade;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.testing.mongot.integration.index.serialization.QueryTestSuite;
import com.xgen.testing.mongot.integration.index.serialization.TestSpec;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class UpgradeTestIndexNamesTest {
  private static final String TEST_SUITE_JSON =
      """
      {
        "tests": [
          {
            "name": "aggregation-test",
            "collections": {
              "coll0": [],
              "coll1": []
            },
            "aggregation": [],
            "result": {
              "valid": true,
              "aggregationResults": []
            }
          },
          {
            "name": "query-test",
            "query": {
              "text": {
                "path": "field",
                "query": "value"
              }
            },
            "result": {
              "valid": true,
              "results": []
            }
          }
        ]
      }
      """;

  @Test
  public void expectedIndexNames_aggregationWithCollections_includesEachCollection()
      throws Exception {
    List<TestSpec> testSpecs = QueryTestSuite.fromJson(TEST_SUITE_JSON).getTests();

    assertThat(
            UpgradeTestIndexNames.expectedIndexNames(
                "aggregation-test_FormatVersion-6_FeatureVersion-4", testSpecs.get(0)))
        .containsExactly(
            "aggregation-test_FormatVersion-6_FeatureVersion-4-coll0",
            "aggregation-test_FormatVersion-6_FeatureVersion-4-coll1");
  }

  @Test
  public void expectedIndexNames_queryTest_usesTestName() throws Exception {
    List<TestSpec> testSpecs = QueryTestSuite.fromJson(TEST_SUITE_JSON).getTests();

    assertThat(
            UpgradeTestIndexNames.expectedIndexNames(
                "query-test_FormatVersion-6_FeatureVersion-4", testSpecs.get(1)))
        .isEqualTo(Set.of("query-test_FormatVersion-6_FeatureVersion-4"));
  }
}
