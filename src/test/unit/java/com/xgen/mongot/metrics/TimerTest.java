package com.xgen.mongot.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class TimerTest {
  @Test
  public void testSupplier() throws Exception {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    Timer timer = meterRegistry.timer("testSupplier");

    CheckedSupplier<Void, Exception> supplier =
        () -> {
          Thread.sleep(200);
          return null;
        };

    Timed.supplier(timer, supplier);
    Timed.supplier(timer, supplier);
    Duration meanDuration = Duration.ofMillis((long) timer.mean(TimeUnit.MILLISECONDS));

    Assert.assertEquals(2, timer.count());
    Assert.assertEquals(200L, meanDuration.toMillis(), 75);
  }

  @Test
  public void testRecordSampleIgnoringMetricsFailure_recordsSample() {
    Timer timer = new SimpleMeterRegistry().timer("testRecordSampleIgnoringMetricsFailure");

    Timed.recordSampleIgnoringMetricsFailure(timer, Duration.ofMillis(5));

    Assert.assertEquals(1, timer.count());
    Assert.assertEquals(5L, (long) timer.totalTime(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRecordSampleIgnoringMetricsFailure_dropsSampleOnMetricsFailure() {
    Timer timer = mock(Timer.class);
    when(timer.getId()).thenReturn(new SimpleMeterRegistry().timer("failing").getId());

    // The HdrHistogram reset/record race (CLOUDP-433931) must not reach the caller: these samples
    // are recorded on the change stream thread, where an escaping failure stales the index.
    doThrow(new ArrayIndexOutOfBoundsException("Index -16 out of bounds for length 112"))
        .when(timer)
        .record(any(Duration.class));

    Timed.recordSampleIgnoringMetricsFailure(timer, Duration.ofMillis(1));

    // No metrics failure is worth an index, so the guard is not limited to the known histogram bug.
    doThrow(new IllegalStateException("metrics failure")).when(timer).record(any(Duration.class));

    Timed.recordSampleIgnoringMetricsFailure(timer, Duration.ofMillis(1));
  }
}
