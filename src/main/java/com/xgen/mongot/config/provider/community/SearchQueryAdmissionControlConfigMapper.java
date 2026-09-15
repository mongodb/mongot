package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import java.util.Optional;

/**
 * Maps the {@code searchQueryAdmissionControl} config block onto the runtime {@link
 * RegularBlockingRequestSettings}.
 */
public final class SearchQueryAdmissionControlConfigMapper {

  private SearchQueryAdmissionControlConfigMapper() {}

  /**
   * Builds {@link RegularBlockingRequestSettings} from the parsed {@code
   * searchQueryAdmissionControl} block. When the block is absent, config defaults are used.
   */
  public static RegularBlockingRequestSettings createRegularBlockingRequestSettings(
      Optional<SearchQueryAdmissionControlConfig> config) {
    SearchQueryAdmissionControlConfig effective =
        config.orElseGet(SearchQueryAdmissionControlConfig::defaults);
    return RegularBlockingRequestSettings.create(
        Optional.of(effective.threadPoolSizeMultiplier()),
        Optional.of(effective.queueCapacityMultiplier()),
        Optional.of(effective.virtualQueueCapacity()));
  }

  /**
   * Resolves whether load-shedding rejections should signal the load balancer to retry on another
   * host. Returns {@code false} when the {@code searchQueryAdmissionControl} block is absent.
   */
  public static boolean shouldRetryViaLoadBalancer(
      Optional<SearchQueryAdmissionControlConfig> config) {
    return config
        .map(SearchQueryAdmissionControlConfig::shouldRetryViaLoadBalancer)
        .orElse(SearchQueryAdmissionControlConfig.defaults().shouldRetryViaLoadBalancer());
  }
}
