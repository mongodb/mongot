package com.xgen.mongot.monitor;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;

public class SizeBasedDiskMonitorTest {

  private static final long MAX_INDEX_MB = 10;
  private static final long MAX_INDEX_BYTES = MAX_INDEX_MB * 1024 * 1024;
  private static final long DISK_SIZE_BYTES = 100L * 1024 * 1024;

  private final AtomicReference<OptionalLong> sample = new AtomicReference<>(OptionalLong.of(0L));
  private final AtomicLong diskSizeBytes = new AtomicLong(DISK_SIZE_BYTES);
  private final AtomicReference<IOException> failure = new AtomicReference<>();
  private final AtomicReference<RuntimeException> unexpectedFailure = new AtomicReference<>();
  private final AtomicReference<RuntimeException> diskSizeFailure = new AtomicReference<>();

  private SizeBasedDiskMonitor monitor;

  private SizeBasedDiskMonitor newMonitor(long maxIndexMb, long diskSizeBytes) {
    this.diskSizeBytes.set(diskSizeBytes);
    return SizeBasedDiskMonitor.create(
        this::sampleOrThrow, this::diskSizeBytesOrThrow, maxIndexMb, new SimpleMeterRegistry());
  }

  private long diskSizeBytesOrThrow() {
    RuntimeException thrown = this.diskSizeFailure.get();
    if (thrown != null) {
      throw thrown;
    }
    return this.diskSizeBytes.get();
  }

  private OptionalLong sampleOrThrow() throws IOException {
    RuntimeException unexpected = this.unexpectedFailure.get();
    if (unexpected != null) {
      throw unexpected;
    }
    IOException thrown = this.failure.get();
    if (thrown != null) {
      throw thrown;
    }
    return this.sample.get();
  }

  @After
  public void after() {
    if (this.monitor != null) {
      this.monitor.stop();
    }
  }

  @Test
  public void create_nonPositiveCeiling_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SizeBasedDiskMonitor.create(
                this::sampleOrThrow, () -> DISK_SIZE_BYTES, 0, new SimpleMeterRegistry()));
  }

  @Test
  public void create_samplesBeforeReturning() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));

    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_belowCeiling_reportsFractionAgainstMinOfCapAndDisk() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 4));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);

    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.25);
  }

  @Test
  public void update_diskSmallerThanCap_usesDiskSizeAsDenominator() {
    long diskSizeBytes = MAX_INDEX_BYTES / 2;
    this.sample.set(OptionalLong.of(diskSizeBytes / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, diskSizeBytes);

    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_unreadableDiskSize_usesCeilingAsDenominator() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, 0);

    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_diskGrows_widensDenominator() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 4));
    this.monitor = newMonitor(MAX_INDEX_MB, MAX_INDEX_BYTES / 2);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.diskSizeBytes.set(DISK_SIZE_BYTES);
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.25);
  }

  @Test
  public void update_diskShrinks_narrowsDenominator() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 4));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.25);

    this.diskSizeBytes.set(MAX_INDEX_BYTES / 2);
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_diskSizeUnreadableAfterCreate_usesCeilingAsDenominator() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 4));
    this.monitor = newMonitor(MAX_INDEX_MB, MAX_INDEX_BYTES / 2);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.diskSizeBytes.set(0);
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.25);
  }

  @Test
  public void update_diskSizeSamplerThrows_keepsLastReading() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.diskSizeFailure.set(new IllegalStateException("file store unavailable"));
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_aboveCeiling_clampsToOne() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES * 3));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);

    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(1.0);
  }

  @Test
  public void update_sampleUnavailable_keepsLastReading() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.sample.set(OptionalLong.empty());
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void update_ioException_keepsLastReading() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.failure.set(new IOException("walk failed"));
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void create_failedFirstSample_doesNotReportZeroUsage() {
    this.failure.set(new IOException("walk failed"));

    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(1.0);
  }

  @Test
  public void update_runtimeException_keepsLastReading() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);

    this.unexpectedFailure.set(new IllegalStateException("sampler failed"));
    this.monitor.update();

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(0.5);
  }

  @Test
  public void create_runtimeExceptionFirstSample_doesNotReportZeroUsage() {
    this.unexpectedFailure.set(new IllegalStateException("sampler failed"));

    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);

    assertThat(this.monitor.getIndexSizeUtilization()).isEqualTo(1.0);
  }

  @Test
  public void register_pushesCurrentUtilization() {
    this.sample.set(OptionalLong.of(MAX_INDEX_BYTES / 2));
    this.monitor = newMonitor(MAX_INDEX_MB, DISK_SIZE_BYTES);
    var seen = new AtomicReference<Double>();

    this.monitor.register(new RecordingGate(seen));

    assertThat(seen.get()).isEqualTo(0.5);
  }

  private static final class RecordingGate implements Gate {
    private final AtomicReference<Double> seen;

    RecordingGate(AtomicReference<Double> seen) {
      this.seen = seen;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void awaitOpen() {}

    @Override
    public void awaitClose() {}

    @Override
    public void update(double value) {
      this.seen.set(value);
    }

    @Override
    public Instant lastChanged() {
      return Instant.EPOCH;
    }
  }
}
