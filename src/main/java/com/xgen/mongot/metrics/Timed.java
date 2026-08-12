package com.xgen.mongot.metrics;

import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier2;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Timed {

  private static final Logger LOG = LoggerFactory.getLogger(Timed.class);

  private Timed() {}

  /**
   * Records one timing sample, dropping the sample instead of propagating a failure from the
   * metrics layer (CLOUDP-433931).
   *
   * <p>Use this for samples taken on a thread where a metrics failure costs more than a data point.
   * Timers that publish percentiles are backed by HdrHistogram's {@code ConcurrentHistogram}, whose
   * time-window reset is not safe against a concurrent record and can compute a negative array
   * index. The resulting error if escaped into change stream batch error handling, can stall
   * indexes.
   *
   * <p>Every failure drops the sample, but the log level distinguishes them: WARN for that known
   * race, which needs no operator action, and ERROR for anything else.
   */
  public static void recordSampleIgnoringMetricsFailure(Timer timer, Duration sample) {
    try {
      timer.record(sample);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.warn("Dropped timing sample for metric {}.", timer.getId().getName(), e);
    } catch (RuntimeException e) {
      LOG.error(
          "Dropped timing sample for metric {} after an unexpected metrics failure.",
          timer.getId().getName(),
          e);
    }
  }

  /** Runs the given supplier, timing its execution with the given timer. */
  public static <T, E extends Exception> T supplier(Timer timer, CheckedSupplier<T, E> supplier)
      throws E {
    Timer.Sample sample = Timer.start();
    T val = supplier.get();
    sample.stop(timer);
    return val;
  }

  public static <T, E1 extends Exception, E2 extends Exception> T supplier2(
      Timer timer, CheckedSupplier2<T, E1, E2> supplier) throws E1, E2 {
    Timer.Sample sample = Timer.start();
    T val = supplier.get();
    sample.stop(timer);
    return val;
  }

  /** Runs the given runnable, timing its execution with the given timer. */
  public static <E extends Exception> void runnable(Timer timer, CheckedRunnable<E> runnable)
      throws E {
    Timer.Sample sample = Timer.start();
    runnable.run();
    sample.stop(timer);
  }
}
