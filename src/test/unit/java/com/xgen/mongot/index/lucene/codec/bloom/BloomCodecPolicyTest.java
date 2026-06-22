package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.DOES_NOT_EXIST;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.FAILED;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.INITIAL_SYNC;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.NOT_STARTED;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.RECOVERING_TRANSIENT;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.STALE;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.STEADY;
import static com.xgen.mongot.index.status.IndexStatus.StatusCode.UNKNOWN;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BooleanSupplier;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BloomCodecPolicyTest {

  private static final boolean BLOOM_DFF_ENABLED = true;
  private static final boolean BLOOM_DFF_DISABLED = false;
  private static final boolean STEADY_STATE_DFF_ENABLED = true;
  private static final boolean STEADY_STATE_DFF_DISABLED = false;
  private static final boolean EXPECTED_ENABLED = true;
  private static final boolean EXPECTED_DISABLED = false;

  private final boolean bloomDffEnabled;
  private final boolean steadyStateDffEnabled;
  private final IndexStatus indexStatus;
  private final boolean expectedBloomEnabled;

  public BloomCodecPolicyTest(
      boolean bloomDffEnabled,
      boolean steadyStateDffEnabled,
      StatusCode statusCode,
      boolean expectedBloomEnabled) {
    this.bloomDffEnabled = bloomDffEnabled;
    this.steadyStateDffEnabled = steadyStateDffEnabled;
    this.indexStatus = indexStatusFor(statusCode);
    this.expectedBloomEnabled = expectedBloomEnabled;
  }

  @Parameters(name = "bloomDff={0}, steadyStateDff={1}, status={2}, expected={3}")
  public static Collection<Object[]> data() {
    return Arrays.asList(TEST_INPUTS);
  }

  @Test
  public void getBloomFilterEnabledForIdField() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD))
        .thenReturn(this.bloomDffEnabled);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_IN_STEADY_STATE))
        .thenReturn(this.steadyStateDffEnabled);

    BooleanSupplier supplier =
        BloomCodecPolicy.getBloomFilterEnabledForIdField(
            registry, true, MOCK_INDEX_DEFINITION, () -> this.indexStatus);

    assertThat(supplier.getAsBoolean()).isEqualTo(this.expectedBloomEnabled);
  }

  private static final Object[][] TEST_INPUTS = {
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, UNKNOWN, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, DOES_NOT_EXIST, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, NOT_STARTED, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, INITIAL_SYNC, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, STALE, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, RECOVERING_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, RECOVERING_NON_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, STEADY, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_DISABLED, FAILED, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, UNKNOWN, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, DOES_NOT_EXIST, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, NOT_STARTED, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, INITIAL_SYNC, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, STALE, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, RECOVERING_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, RECOVERING_NON_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, STEADY, EXPECTED_DISABLED},
    {BLOOM_DFF_DISABLED, STEADY_STATE_DFF_ENABLED, FAILED, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, UNKNOWN, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, DOES_NOT_EXIST, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, NOT_STARTED, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, INITIAL_SYNC, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, STALE, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, RECOVERING_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, RECOVERING_NON_TRANSIENT, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, STEADY, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_DISABLED, FAILED, EXPECTED_DISABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, UNKNOWN, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, DOES_NOT_EXIST, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, NOT_STARTED, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, INITIAL_SYNC, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, STALE, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, RECOVERING_TRANSIENT, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, RECOVERING_NON_TRANSIENT, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, STEADY, EXPECTED_ENABLED},
    {BLOOM_DFF_ENABLED, STEADY_STATE_DFF_ENABLED, FAILED, EXPECTED_DISABLED},
  };

  private static IndexStatus indexStatusFor(StatusCode statusCode) {
    return switch (statusCode) {
      case UNKNOWN -> IndexStatus.unknown();
      case DOES_NOT_EXIST -> IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED);
      case NOT_STARTED -> IndexStatus.notStarted();
      case INITIAL_SYNC -> IndexStatus.initialSync();
      case STALE -> IndexStatus.stale("stale", new BsonTimestamp(0, 0));
      case RECOVERING_TRANSIENT -> IndexStatus.recoveringTransient("recovering");
      case RECOVERING_NON_TRANSIENT -> IndexStatus.recoveringNonTransient(new BsonTimestamp(0, 0));
      case STEADY -> IndexStatus.steady();
      case FAILED -> IndexStatus.failed("error");
    };
  }
}
