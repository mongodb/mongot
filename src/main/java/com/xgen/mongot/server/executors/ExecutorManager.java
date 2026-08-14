package com.xgen.mongot.server.executors;

import com.google.common.collect.HashBasedTable;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.server.util.NettyUtil;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.Crash;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.EventLoopGroup;
import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the executors that are shared between different servers.
 *
 * <p>This class is thread-unsafe.
 */
public class ExecutorManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorManager.class);
  private static final int WORKER_EVENT_LOOP_GROUP_NUM_THREADS = 32;

  public final BulkheadCommandExecutor commandExecutor;

  public enum EventLoopGroupType {
    BOSS,
    WORKER
  }

  private final HashBasedTable<NettyUtil.SocketType, EventLoopGroupType, EventLoopGroup>
      eventLoopGroups;

  public ExecutorManager(MeterRegistry meterRegistry) {
    this(
        meterRegistry,
        RegularBlockingRequestSettings.defaults(),
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        Optional.empty());
  }

  public ExecutorManager(
      MeterRegistry meterRegistry, RegularBlockingRequestSettings regularBlockingRequestSettings) {
    this(
        meterRegistry,
        regularBlockingRequestSettings,
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        Optional.empty());
  }

  public ExecutorManager(
      MeterRegistry meterRegistry,
      RegularBlockingRequestSettings regularBlockingRequestSettings,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      Optional<ThreadPoolResourceMetrics.Bindings> threadPoolResourceBindings) {
    this.commandExecutor =
        new BulkheadCommandExecutor(
            meterRegistry,
            regularBlockingRequestSettings,
            featureFlags,
            dynamicFeatureFlagRegistry,
            threadPoolResourceBindings);
    this.eventLoopGroups = HashBasedTable.create();
  }

  public EventLoopGroup getEventLoopGroup(
      NettyUtil.SocketType socketType, EventLoopGroupType eventLoopGroupType) {
    if (this.eventLoopGroups.contains(socketType, eventLoopGroupType)) {
      return this.eventLoopGroups.get(socketType, eventLoopGroupType);
    }
    var eventLoopGroup = createEventLoopGroup(socketType, eventLoopGroupType);
    this.eventLoopGroups.put(socketType, eventLoopGroupType, eventLoopGroup);
    return eventLoopGroup;
  }

  private EventLoopGroup createEventLoopGroup(
      NettyUtil.SocketType socketType, EventLoopGroupType eventLoopGroupType) {
    return switch (eventLoopGroupType) {
      case BOSS -> NettyUtil.createEventLoopGroup(socketType);
      case WORKER ->
          NettyUtil.createEventLoopGroup(socketType, WORKER_EVENT_LOOP_GROUP_NUM_THREADS);
    };
  }

  void shutdown() throws InterruptedException {
    this.commandExecutor.close();
    shutdownEventLoopGroupsInParallel(
        this.eventLoopGroups.column(EventLoopGroupType.WORKER).values());
    shutdownEventLoopGroupsInParallel(
        this.eventLoopGroups.column(EventLoopGroupType.BOSS).values());
  }

  private static void shutdownEventLoopGroupsInParallel(Collection<EventLoopGroup> eventLoopGroups)
      throws InterruptedException {
    CheckedStream.from(eventLoopGroups.stream().map(EventLoopGroup::shutdownGracefully))
        .forEachChecked(future -> future.sync());
  }

  @Override
  public void close() {
    LOG.info("Shutting down server executors.");
    Crash.because("failed to shut down server executors").ifThrows(this::shutdown);
  }
}
