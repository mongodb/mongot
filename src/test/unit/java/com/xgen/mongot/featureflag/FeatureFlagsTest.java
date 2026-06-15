package com.xgen.mongot.featureflag;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      FeatureFlagsTest.DeserializationTest.class,
      FeatureFlagsTest.SerializationTest.class,
      FeatureFlagsTest.BuilderTest.class,
      FeatureFlagsTest.StaticTest.class,
    })
public class FeatureFlagsTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "featureFlagsDeserialization";
    private static final BsonDeserializationTestSuite<FeatureFlags> TEST_SUITE =
        fromDocument("src/test/unit/resources/featureflag", SUITE_NAME, FeatureFlags::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<FeatureFlags> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<FeatureFlags> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "deserialize: {0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<FeatureFlags>> data() {
      return TEST_SUITE.withExamples(
          emptyDocumentUsesDefaults(),
          disabledDefaultOverriddenToEnabled(),
          enabledDefaultOverriddenToDisabled(),
          allFeaturesEnabled(),
          allFeaturesDisabled());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<FeatureFlags>
        emptyDocumentUsesDefaults() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Empty document uses defaults", FeatureFlags.getDefault());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FeatureFlags>
        disabledDefaultOverriddenToEnabled() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Disabled default overridden to enabled",
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FeatureFlags>
        enabledDefaultOverriddenToDisabled() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "Enabled default overridden to disabled",
          FeatureFlags.withDefaults().disable(Feature.INITIAL_INDEX_STATUS_UNKNOWN).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FeatureFlags> allFeaturesEnabled() {
      FeatureFlags.Builder builder = FeatureFlags.withDefaults();
      for (Feature feature : Feature.values()) {
        builder.enable(feature);
      }
      return BsonDeserializationTestSuite.TestSpec.valid("All features enabled", builder.build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FeatureFlags> allFeaturesDisabled() {
      FeatureFlags.Builder builder = FeatureFlags.withDefaults();
      for (Feature feature : Feature.values()) {
        builder.disable(feature);
      }
      return BsonDeserializationTestSuite.TestSpec.valid("All features disabled", builder.build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "featureFlagsSerialization";
    private static final BsonSerializationTestSuite<FeatureFlags> TEST_SUITE =
        fromEncodable("src/test/unit/resources/featureflag", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<FeatureFlags> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<FeatureFlags> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "serialize: {0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<FeatureFlags>> data() {
      return List.of(
          defaultConfig(), queryFeaturesEnabled(), allFeaturesEnabled(), allFeaturesDisabled());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<FeatureFlags> defaultConfig() {
      return BsonSerializationTestSuite.TestSpec.create("default", FeatureFlags.getDefault());
    }

    private static BsonSerializationTestSuite.TestSpec<FeatureFlags> queryFeaturesEnabled() {
      return BsonSerializationTestSuite.TestSpec.create(
          "query features enabled", FeatureFlags.withQueryFeaturesEnabled());
    }

    private static BsonSerializationTestSuite.TestSpec<FeatureFlags> allFeaturesEnabled() {
      FeatureFlags.Builder builder = FeatureFlags.withDefaults();
      for (Feature feature : Feature.values()) {
        builder.enable(feature);
      }
      return BsonSerializationTestSuite.TestSpec.create("all features enabled", builder.build());
    }

    private static BsonSerializationTestSuite.TestSpec<FeatureFlags> allFeaturesDisabled() {
      FeatureFlags.Builder builder = FeatureFlags.withDefaults();
      for (Feature feature : Feature.values()) {
        builder.disable(feature);
      }
      return BsonSerializationTestSuite.TestSpec.create("all features disabled", builder.build());
    }
  }

  public static class BuilderTest {

    @Test
    public void getDefault_returnsCachedSingleton() {
      assertSame(FeatureFlags.getDefault(), FeatureFlags.getDefault());
    }

    @Test
    public void withDefaults_build_equalsGetDefault() {
      assertEquals(FeatureFlags.getDefault(), FeatureFlags.withDefaults().build());
    }

    @Test
    public void builder_enable_enablesFeature() {
      FeatureFlags flags =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      assertTrue(flags.isEnabled(Feature.STALE_STATE_TRANSITION));
    }

    @Test
    public void builder_disable_disablesFeature() {
      FeatureFlags flags =
          FeatureFlags.withDefaults().disable(Feature.INITIAL_INDEX_STATUS_UNKNOWN).build();
      assertFalse(flags.isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN));
    }

    @Test
    public void builder_enable_otherFeaturesUnaffected() {
      FeatureFlags flags =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      // Other disabled-by-default features remain disabled
      assertFalse(flags.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS));
      // Other enabled-by-default features remain enabled
      assertTrue(flags.isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN));
    }

    @Test
    public void isEnabled_defaultDisabledFeature_returnsFalse() {
      assertFalse(FeatureFlags.getDefault().isEnabled(Feature.STALE_STATE_TRANSITION));
      assertFalse(FeatureFlags.getDefault().isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS));
    }

    @Test
    public void isEnabled_defaultEnabledFeature_returnsTrue() {
      assertTrue(FeatureFlags.getDefault().isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN));
      assertTrue(
          FeatureFlags.getDefault().isEnabled(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE));
      assertTrue(FeatureFlags.getDefault().isEnabled(Feature.VECTOR_STORED_SOURCE));
      assertTrue(FeatureFlags.getDefault().isEnabled(Feature.NESTED_VECTOR));
    }

    @Test
    public void withQueryFeaturesEnabled_enablesExpectedFeatures() {
      FeatureFlags flags = FeatureFlags.withQueryFeaturesEnabled();
      assertTrue(flags.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS));
      assertTrue(flags.isEnabled(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES));
      assertTrue(flags.isEnabled(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC));
      assertTrue(flags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));
      assertTrue(flags.isEnabled(Feature.SORTED_INDEX));
      assertTrue(flags.isEnabled(Feature.NESTED_VECTOR));
    }

    @Test
    public void withQueryFeaturesEnabled_leavesOtherFeaturesAtDefaults() {
      FeatureFlags flags = FeatureFlags.withQueryFeaturesEnabled();
      // Other disabled-by-default features remain disabled
      assertFalse(flags.isEnabled(Feature.STALE_STATE_TRANSITION));
      // Enabled-by-default features remain enabled
      assertTrue(flags.isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN));
      assertTrue(flags.isEnabled(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE));
      assertTrue(flags.isEnabled(Feature.VECTOR_STORED_SOURCE));
    }

    @Test
    public void communityDefaults_enablesAtlasRolledOutFeatures() {
      FeatureFlags flags = FeatureFlags.communityDefaults();
      assertTrue(flags.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS));
      assertTrue(flags.isEnabled(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES));
      assertTrue(flags.isEnabled(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC));
      assertTrue(flags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));
      assertTrue(flags.isEnabled(Feature.SORTED_INDEX));
      assertTrue(flags.isEnabled(Feature.STALE_STATE_TRANSITION));
      assertTrue(flags.isEnabled(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK));
      assertTrue(flags.isEnabled(Feature.REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION));
      assertTrue(flags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND));
      assertTrue(flags.isEnabled(Feature.FLOOR_SEGMENT_MB));
      assertTrue(flags.isEnabled(Feature.TRUNCATE_AUTOCOMPLETE_TOKENS));
      assertTrue(flags.isEnabled(Feature.FTDC_EXECUTOR_METRICS_TO_PROMETHEUS));
      assertTrue(flags.isEnabled(Feature.INDEX_SIZE_QUANTIZATION_METRICS));
      assertTrue(flags.isEnabled(Feature.CACHE_WARMER));
      assertTrue(flags.isEnabled(Feature.CONCURRENT_INDEX_PARTITION_SEARCH));
      assertTrue(flags.isEnabled(Feature.CANCEL_MERGE));

      // Other disabled-by-default features remain disabled
      assertFalse(flags.isEnabled(Feature.OVERLOAD_RETRY_SIGNAL));
      // Enabled-by-default features remain enabled
      assertTrue(flags.isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN));
      assertTrue(flags.isEnabled(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE));
      assertTrue(flags.isEnabled(Feature.VECTOR_STORED_SOURCE));
      assertTrue(flags.isEnabled(Feature.NESTED_VECTOR));
    }

    @Test
    public void equals_sameStates_areEqual() {
      FeatureFlags flags1 =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      FeatureFlags flags2 =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      assertEquals(flags1, flags2);
    }

    @Test
    public void equals_differentStates_areNotEqual() {
      FeatureFlags withEnabled =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      FeatureFlags withDefault = FeatureFlags.getDefault();
      assertNotEquals(withEnabled, withDefault);
    }

    @Test
    public void hashCode_equalObjects_haveEqualHashCodes() {
      FeatureFlags flags1 =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      FeatureFlags flags2 =
          FeatureFlags.withDefaults().enable(Feature.STALE_STATE_TRANSITION).build();
      assertEquals(flags1.hashCode(), flags2.hashCode());
    }
  }

  public static class StaticTest {
    private FeatureFlags flags;

    @Before
    public void setUp() {
      this.flags = FeatureFlags.withDefaults().build();
    }

    @After
    public void tearDown() {
      FeatureFlags.resetForTest();
    }

    @Test
    public void initializeProcessInstance_called_instanceNotNull() {
      FeatureFlags.initializeProcessInstance(this.flags);
      assertNotNull(FeatureFlags.getStatic());
    }

    @Test
    public void getStatic_noDependencyInjection_returnsEnabledFeature() {
      var custom = FeatureFlags.withDefaults().enable(Feature.SORTED_INDEX).build();
      FeatureFlags.initializeProcessInstance(custom);
      assertTrue(FeatureFlags.getStatic().isEnabled(Feature.SORTED_INDEX));
    }

    @Test
    public void getStatic_noDependencyInjection_returnsDisabledFeature() {
      var custom = FeatureFlags.withDefaults().disable(Feature.SORTED_INDEX).build();
      FeatureFlags.initializeProcessInstance(custom);
      assertFalse(FeatureFlags.getStatic().isEnabled(Feature.SORTED_INDEX));
    }

    @Test
    public void getStatic_beforeInit_throwsIllegalStateException() {
      assertThrows(IllegalStateException.class, FeatureFlags::getStatic);
    }

    @Test
    public void initializeProcessInstance_calledTwice_throwsIllegalStateException() {
      FeatureFlags.initializeProcessInstance(this.flags);
      assertThrows(IllegalStateException.class,
          () -> FeatureFlags.initializeProcessInstance(this.flags));
    }

    @Test
    public void getStatic_afterReset_throwsIllegalStateException() {
      FeatureFlags.initializeProcessInstance(this.flags);
      FeatureFlags.resetForTest();
      assertThrows(IllegalStateException.class, FeatureFlags::getStatic);
    }

    @Test
    public void getStatic_succeeds_afterReset() {
      FeatureFlags.initializeProcessInstance(this.flags);
      FeatureFlags.resetForTest();
      FeatureFlags.initializeProcessInstance(this.flags);
      assertNotNull(FeatureFlags.getStatic());
    }

  }

}
