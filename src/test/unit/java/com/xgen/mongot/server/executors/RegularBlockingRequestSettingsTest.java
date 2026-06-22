package com.xgen.mongot.server.executors;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;

/** Unit tests for {@link RegularBlockingRequestSettings}. */
public class RegularBlockingRequestSettingsTest {

  @Test
  public void defaults_noExplicitValues_allZero() {
    RegularBlockingRequestSettings settings = RegularBlockingRequestSettings.defaults();

    assertThat(settings.threadPoolSizeMultiplier()).isWithin(0.001).of(0.0d);
    assertThat(settings.queueCapacityMultiplier()).isWithin(0.001).of(0.0d);
    assertThat(settings.virtualQueueCapacity()).isFalse();
  }

  @Test
  public void create_withExplicitValues_storesAllValues() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(3.0), Optional.of(4.0), Optional.of(true));

    assertThat(settings.threadPoolSizeMultiplier()).isWithin(0.001).of(3.0d);
    assertThat(settings.queueCapacityMultiplier()).isWithin(0.001).of(4.0d);
    assertThat(settings.virtualQueueCapacity()).isTrue();
  }

  @Test
  public void create_withNonPositiveValues_filtersOutToZero() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(-2.0), Optional.of(0.0), Optional.empty());

    assertThat(settings.threadPoolSizeMultiplier()).isWithin(0.001).of(0.0d);
    assertThat(settings.queueCapacityMultiplier()).isWithin(0.001).of(0.0d);
    assertThat(settings.virtualQueueCapacity()).isFalse();
  }

  @Test
  public void resolvedPoolSize_withMultiplier_computesFromMultiplierTimesCpus() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(3.0), Optional.empty(), Optional.empty());

    assertThat(settings.resolvedPoolSize(4)).isEqualTo(12); // 3.0 * 4 = 12
    assertThat(settings.resolvedPoolSize(8)).isEqualTo(24); // 3.0 * 8 = 24
    assertThat(settings.resolvedPoolSize(1)).isEqualTo(3); // 3.0 * 1 = 3
  }

  @Test
  public void resolvedPoolSize_withZeroMultiplier_returnsOne() {
    RegularBlockingRequestSettings settings = RegularBlockingRequestSettings.defaults();

    assertThat(settings.resolvedPoolSize(4)).isEqualTo(1);
    assertThat(settings.resolvedPoolSize(8)).isEqualTo(1);
  }

  @Test
  public void resolvedPoolSize_withZeroOrNegativeCpuCount_treatsAsOneCpu() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(2.0), Optional.empty(), Optional.empty());

    assertThat(settings.resolvedPoolSize(0)).isEqualTo(2); // 2.0 * 1 = 2
    assertThat(settings.resolvedPoolSize(-1)).isEqualTo(2); // 2.0 * 1 = 2
  }

  @Test
  public void resolvedPoolSize_withSmallMultiplier_returnsMinimumOfOne() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(0.1), Optional.empty(), Optional.empty());

    assertThat(settings.resolvedPoolSize(1)).isEqualTo(1); // ceil(0.1 * 1) = 1
  }

  @Test
  public void resolvedPoolSize_withFractionalMultiplier_ceilsResult() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(2.5), Optional.empty(), Optional.empty());

    assertThat(settings.resolvedPoolSize(4)).isEqualTo(10); // 2.5 * 4 = 10
    assertThat(settings.resolvedPoolSize(3)).isEqualTo(8); // ceil(2.5 * 3) = ceil(7.5) = 8
  }

  @Test
  public void resolvedQueueCapacity_withMultiplier_computesFromMultiplierTimesCpus() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.empty(), Optional.of(4.0), Optional.empty());

    assertThat(settings.resolvedQueueCapacity(4)).isEqualTo(16); // 4.0 * 4 = 16
    assertThat(settings.resolvedQueueCapacity(8)).isEqualTo(32); // 4.0 * 8 = 32
  }

  @Test
  public void resolvedQueueCapacity_withZeroMultiplier_returnsOne() {
    RegularBlockingRequestSettings settings = RegularBlockingRequestSettings.defaults();

    assertThat(settings.resolvedQueueCapacity(4)).isEqualTo(1);
    assertThat(settings.resolvedQueueCapacity(8)).isEqualTo(1);
  }

  @Test
  public void resolvedQueueCapacity_withFractionalMultiplier_ceilsResult() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.empty(), Optional.of(1.5), Optional.empty());

    assertThat(settings.resolvedQueueCapacity(4)).isEqualTo(6); // 1.5 * 4 = 6
    assertThat(settings.resolvedQueueCapacity(3)).isEqualTo(5); // ceil(1.5 * 3) = ceil(4.5) = 5
  }

  @Test
  public void maybeResolvedQueueCapacity_withZeroMultiplier_returnsEmpty() {
    RegularBlockingRequestSettings settings = new RegularBlockingRequestSettings(2.0, 0.0, false);

    OptionalInt result = settings.maybeResolvedQueueCapacity(4);

    assertThat(result).isEmpty();
  }

  @Test
  public void maybeResolvedQueueCapacity_withPositiveMultiplier_returnsComputedValue() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.empty(), Optional.of(3.0), Optional.empty());

    OptionalInt result = settings.maybeResolvedQueueCapacity(4);

    assertThat(result).hasValue(12); // 3.0 * 4 = 12
  }

  @Test
  public void getMode_withNoPoolOrQueue_returnsUnboundedCaching() {
    RegularBlockingRequestSettings settings = RegularBlockingRequestSettings.defaults();

    assertThat(settings.getMode()).isEqualTo(RegularBlockingRequestSettings.Mode.UNBOUNDED_CACHING);
  }

  @Test
  public void getMode_withPoolOnly_returnsUnboundedQueue() {
    // Pool configured but no queue -> Fixed Pool with Unbounded Queue (no wouldHaveRejected)
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(4.0), Optional.empty(), Optional.empty());

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE);
  }

  @Test
  public void getMode_withQueueOnly_returnsUnboundedCaching() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.empty(), Optional.of(8.0), Optional.empty());

    assertThat(settings.getMode()).isEqualTo(RegularBlockingRequestSettings.Mode.UNBOUNDED_CACHING);
  }

  @Test
  public void getMode_withPoolAndQueue_virtualFalse_returnsBoundedQueue() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.of(8.0), Optional.of(false));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_BOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolAndQueue_virtualTrue_returnsUnboundedQueue() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.of(8.0), Optional.of(true));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolAndQueue_virtualNotSet_defaultsToBoundedQueue() {
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(Optional.of(4.0), Optional.of(8.0), Optional.empty());

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_BOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolMultiplierAndQueueMultiplier_virtualTrue_returnsUnboundedQueue() {
    // Per-CPU pool + per-CPU queue + virtualQueueCapacity=true -> FIXED_POOL_UNBOUNDED_QUEUE
    // (records wouldHaveRejected)
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.of(8.0), Optional.of(true));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolMultiplierAndQueueMultiplier_virtualFalse_returnsBoundedQueue() {
    // Per-CPU pool + per-CPU queue + virtualQueueCapacity=false -> FIXED_POOL_BOUNDED_QUEUE
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.of(8.0), Optional.of(false));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_BOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolOnly_virtualTrue_returnsUnboundedQueue() {
    // Pool + virtualQueueCapacity=true (no queue config) -> FIXED_POOL_UNBOUNDED_QUEUE
    // Queue config is optional for unbounded queue mode
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.empty(), Optional.of(true));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE);
  }

  @Test
  public void getMode_withPoolAndQueueMultiplier_virtualTrue_returnsUnboundedQueue() {
    // Pool + queue multiplier + virtualQueueCapacity=true -> FIXED_POOL_UNBOUNDED_QUEUE
    // (wouldHaveRejected will be recorded based on queue capacity)
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(4.0), Optional.of(8.0), Optional.of(true));

    assertThat(settings.getMode())
        .isEqualTo(RegularBlockingRequestSettings.Mode.FIXED_POOL_UNBOUNDED_QUEUE);
  }
}
