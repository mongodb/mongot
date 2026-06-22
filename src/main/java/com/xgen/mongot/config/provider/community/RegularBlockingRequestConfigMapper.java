package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import java.util.Optional;

/**
 * Maps the {@code searchQueryAdmissionControl} config block onto the runtime {@link
 * RegularBlockingRequestSettings}.
 */
public final class RegularBlockingRequestConfigMapper {

  private RegularBlockingRequestConfigMapper() {}

  /**
   * Builds {@link RegularBlockingRequestSettings} from the parsed {@code
   * searchQueryAdmissionControl} block. When the block is absent, {@link
   * RegularBlockingRequestSettings#create} falls back to the unbounded-caching defaults.
   */
  public static RegularBlockingRequestSettings createRegularBlockingRequestSettings(
      Optional<RegularBlockingRequestConfig> config) {
    return RegularBlockingRequestSettings.create(
        config.flatMap(RegularBlockingRequestConfig::threadPoolSizeMultiplier),
        config.flatMap(RegularBlockingRequestConfig::queueCapacityMultiplier),
        config.flatMap(RegularBlockingRequestConfig::virtualQueueCapacity));
  }
}
