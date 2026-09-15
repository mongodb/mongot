package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.Test;

public class SearchQueryAdmissionControlConfigMapperTest {

  private static final Path TUNING_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");

  @Test
  public void createRegularBlockingRequestSettings_honorsConfiguredValues() throws Exception {
    CommunityConfig config = CommunityConfig.readFromFile(TUNING_CONFIG).config();

    RegularBlockingRequestSettings settings =
        SearchQueryAdmissionControlConfigMapper.createRegularBlockingRequestSettings(
            config.advancedConfigs().flatMap(AdvancedConfigs::regularBlockingRequestConfig));

    assertEquals(3.0, settings.threadPoolSizeMultiplier(), 0.0);
    assertEquals(30.0, settings.queueCapacityMultiplier(), 0.0);
    assertEquals(true, settings.virtualQueueCapacity());
    assertEquals(
        RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE, settings.getMode());
  }

  @Test
  public void createRegularBlockingRequestSettings_appliesDefaultsWhenUnset() {
    RegularBlockingRequestSettings settings =
        SearchQueryAdmissionControlConfigMapper.createRegularBlockingRequestSettings(
            Optional.empty());

    assertEquals(3.0, settings.threadPoolSizeMultiplier(), 0.0);
    assertEquals(30.0, settings.queueCapacityMultiplier(), 0.0);
    assertTrue(settings.virtualQueueCapacity());
    assertEquals(
        RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE, settings.getMode());
  }

  @Test
  public void shouldRetryViaLoadBalancer_configPresentTrue_returnsTrue() {
    boolean result =
        SearchQueryAdmissionControlConfigMapper.shouldRetryViaLoadBalancer(
            Optional.of(new SearchQueryAdmissionControlConfig(3.0, 30.0, true, true)));

    assertTrue(result);
  }

  @Test
  public void shouldRetryViaLoadBalancer_configPresentFalse_returnsFalse() {
    boolean result =
        SearchQueryAdmissionControlConfigMapper.shouldRetryViaLoadBalancer(
            Optional.of(new SearchQueryAdmissionControlConfig(3.0, 30.0, true, false)));

    assertFalse(result);
  }

  @Test
  public void shouldRetryViaLoadBalancer_configAbsent_returnsFalse() {
    boolean result =
        SearchQueryAdmissionControlConfigMapper.shouldRetryViaLoadBalancer(Optional.empty());

    assertFalse(result);
  }

  @Test
  public void createRegularBlockingRequestSettings_nonPositiveMultipliersFallBackToDefaults() {
    RegularBlockingRequestSettings settings =
        SearchQueryAdmissionControlConfigMapper.createRegularBlockingRequestSettings(
            Optional.of(new SearchQueryAdmissionControlConfig(0.0, -1.0, false, false)));

    assertEquals(0.0, settings.threadPoolSizeMultiplier(), 0.0);
    assertEquals(0.0, settings.queueCapacityMultiplier(), 0.0);
    assertEquals(RegularBlockingRequestSettings.Mode.UNBOUNDED_CACHING, settings.getMode());
  }
}
