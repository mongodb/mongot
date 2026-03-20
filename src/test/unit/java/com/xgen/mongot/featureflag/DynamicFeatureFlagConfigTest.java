package com.xgen.mongot.featureflag;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      DynamicFeatureFlagConfigTest.DeserializationTest.class,
      DynamicFeatureFlagConfigTest.SerializationTest.class,
      DynamicFeatureFlagConfigTest.GettersTest.class,
      DynamicFeatureFlagConfigTest.SortersTest.class,
    })
public class DynamicFeatureFlagConfigTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "dynamicFeatureFlagConfigDeserialization";
    private static final BsonDeserializationTestSuite<DynamicFeatureFlagConfig> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/featureflag", SUITE_NAME, DynamicFeatureFlagConfig::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagConfig> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagConfig>>
        data() {
      return TEST_SUITE.withExamples(
          full(),
          minimal(),
          withEmptyList(),
          invalidPhases(),
          invalidScopes(),
          invalidRolloutPercentages());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig> full() {
      ObjectId allowedId1 = new ObjectId("507f1f77bcf86cd799439011");
      ObjectId allowedId2 = new ObjectId("507f1f77bcf86cd799439012");
      ObjectId blockedId = new ObjectId("507f1f77bcf86cd799439013");

      List<ObjectId> allowList = new ArrayList<>();
      allowList.add(allowedId1);
      allowList.add(allowedId2);

      List<ObjectId> blockList = new ArrayList<>();
      blockList.add(blockedId);
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Complete config with all fields",
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              allowList,
              blockList,
              50,
              DynamicFeatureFlagConfig.Scope.GROUP));
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig> minimal() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Minimal config with only name and phase",
          new DynamicFeatureFlagConfig(
              "minimal-feature",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig>
        withEmptyList() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Config with empty lists",
          new DynamicFeatureFlagConfig(
              "empty-lists",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              100,
              DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig>
        invalidPhases() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Invalid phase value",
          new DynamicFeatureFlagConfig(
              "invalid-phase",
              DynamicFeatureFlagConfig.Phase.UNSPECIFIED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig>
        invalidScopes() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Invalid scope value",
          new DynamicFeatureFlagConfig(
              "invalid-scope",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
    }
  }

  private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagConfig>
      invalidRolloutPercentages() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "Invalid rollout percentage (negative)",
        new DynamicFeatureFlagConfig(
            "negative-rollout",
            DynamicFeatureFlagConfig.Phase.CONTROLLED,
            List.of(),
            List.of(),
            -10,
            DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
  }

  public static class GettersTest {
    @Test
    public void sanitizedRolloutPercentage_variousInputs_clampsToValidRange() {
      // Valid percentage
      assertEquals(
          50,
          new DynamicFeatureFlagConfig(
                  "clamps-to-valid-range",
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  50,
                  DynamicFeatureFlagConfig.Scope.UNSPECIFIED)
              .sanitizedRolloutPercentage());

      // Edge cases
      assertEquals(
          0,
          new DynamicFeatureFlagConfig(
                  "clamps-to-valid-range",
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  0,
                  DynamicFeatureFlagConfig.Scope.UNSPECIFIED)
              .sanitizedRolloutPercentage());
      assertEquals(
          100,
          new DynamicFeatureFlagConfig(
                  "clamps-to-valid-range",
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  100,
                  DynamicFeatureFlagConfig.Scope.UNSPECIFIED)
              .sanitizedRolloutPercentage());

      // Out of bounds (should be clamped)
      assertEquals(
          0,
          new DynamicFeatureFlagConfig(
                  "clamps-to-valid-range",
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  -10,
                  DynamicFeatureFlagConfig.Scope.UNSPECIFIED)
              .sanitizedRolloutPercentage());
      assertEquals(
          100,
          new DynamicFeatureFlagConfig(
                  "clamps-to-valid-range",
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  150,
                  DynamicFeatureFlagConfig.Scope.UNSPECIFIED)
              .sanitizedRolloutPercentage());
    }
  }

  public static class SortersTest {
    @Test
    public void testFeatureFlagSorter() {
      int[] sizes = {20, 50, 100, 1000};
      for (int size : sizes) {
        List<DynamicFeatureFlagConfig> configs = generateConfigs(size);

        configs.sort(DynamicFeatureFlagConfig.FEATURE_FLAG_SORTER);

        // Verify that the list is sorted
        for (int i = 0; i < size - 1; i++) {
          String name1 = configs.get(i).featureFlagName();
          String name2 = configs.get(i + 1).featureFlagName();
          assertTrue(
              "List is not sorted correctly. " + name1 + " should come before " + name2,
              name1.compareTo(name2) <= 0);
        }
      }
    }

    private List<DynamicFeatureFlagConfig> generateConfigs(int size) {
      List<DynamicFeatureFlagConfig> configs = new ArrayList<>(size);
      // Create configs in reverse order to ensure sorting is non-trivial
      for (int i = size - 1; i >= 0; i--) {
        configs.add(
            new DynamicFeatureFlagConfig(
                "feature-" + String.format("%04d", i), // Padded for correct lexicographical sort
                DynamicFeatureFlagConfig.Phase.UNSPECIFIED,
                List.of(),
                List.of(),
                0,
                DynamicFeatureFlagConfig.Scope.UNSPECIFIED));
      }
      return configs;
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "dynamicFeatureFlagConfigSerialization";
    private static final BsonSerializationTestSuite<DynamicFeatureFlagConfig> TEST_SUITE =
        fromEncodable("src/test/unit/resources/featureflag", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagConfig> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagConfig>> data() {
      return Collections.singletonList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagConfig> simple() {
      ObjectId allowedId1 = new ObjectId("507f1f77bcf86cd799439011");
      ObjectId allowedId2 = new ObjectId("507f1f77bcf86cd799439012");
      ObjectId blockedId = new ObjectId("507f1f77bcf86cd799439013");

      List<ObjectId> allowList = new ArrayList<>();
      allowList.add(allowedId1);
      allowList.add(allowedId2);

      List<ObjectId> blockList = new ArrayList<>();
      blockList.add(blockedId);

      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              allowList,
              blockList,
              50,
              DynamicFeatureFlagConfig.Scope.GROUP));
    }
  }
}
