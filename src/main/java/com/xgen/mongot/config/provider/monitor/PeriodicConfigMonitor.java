package com.xgen.mongot.config.provider.monitor;

import com.xgen.mongot.config.updater.ConfigUpdater;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PeriodicConfigMonitor owns a ConfigUpdater, periodically calling it to update. */
public class PeriodicConfigMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicConfigMonitor.class);

  private final NamedScheduledExecutorService executorService;
  private final ConfigUpdater configUpdater;
  private final Duration period;

  private PeriodicConfigMonitor(
      NamedScheduledExecutorService executorService, ConfigUpdater configUpdater, Duration period) {
    this.executorService = executorService;
    this.configUpdater = configUpdater;
    this.period = period;
  }

  public static PeriodicConfigMonitor create(
      ConfigUpdater configUpdater, Duration period, MeterRegistry meterRegistry) {
    return new PeriodicConfigMonitor(
        Executors.singleThreadScheduledExecutor(
            "config-monitor", Thread.MAX_PRIORITY, meterRegistry),
        configUpdater,
        period);
  }

  /** Initializes the ConfigUpdater and begins the periodic calling of ConfigUpdater::update. */
  public void start() {
    LOG.info("Beginning periodic config monitoring");

    this.executorService.scheduleWithFixedDelay(
        () ->
            Crash.because("failed to update config")
                .ifThrowsExceptionOrError(this.configUpdater::update),
        0,
        this.period.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  /** Ceases the periodic calling of ConfigUpdater::update and closes the ConfigUpdater. */
  public void stop() {
    LOG.info("Stopping periodic config monitoring.");

    Executors.shutdownOrFail(this.executorService);
    this.configUpdater.close();
  }
}
