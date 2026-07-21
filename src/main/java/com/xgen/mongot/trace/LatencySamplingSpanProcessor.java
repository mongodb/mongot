package com.xgen.mongot.trace;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.errorprone.annotations.Var;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class LatencySamplingSpanProcessor implements SpanProcessor {

  public enum ExportStrategy {
    TOP_N,
    PERCENTILE
  }

  private final SpanExporter spanExporter;
  private final int topN;
  private final double percentile;
  private final ExportStrategy exportStrategy;
  // Maximum number of traces exported per window. Derived from windowSize / minExportInterval so
  // that both TOP_N and PERCENTILE honor the same export budget (1 trace per 30s).
  private final int maxExportsPerWindow;
  // Upper bound on completed-but-not-yet-drained traces retained for the PERCENTILE pool, so a
  // burst between window drains cannot grow the pool (and the spans it pins) unbounded.
  private final int maxRetainedTraces;
  private final Cache<String, List<ReadableSpan>> tracesInProgress;
  private final Cache<String, Boolean> completedTraces;
  private final ConcurrentSkipListSet<TraceRecord> topTraceRecords;
  // Root-span durations for every trace completed in window, used to compute percentile threshold
  private final ConcurrentLinkedQueue<Long> windowDurationsNanos;
  private final ScheduledExecutorService scheduler;

  private LatencySamplingSpanProcessor(Builder builder) {
    this.spanExporter = Objects.requireNonNull(builder.spanExporter, "spanExporter cannot be null");
    this.topN = builder.topN;
    this.percentile = builder.percentile;
    this.exportStrategy = builder.exportStrategy;
    this.maxRetainedTraces = builder.maxRetainedTraces;
    this.topTraceRecords = new ConcurrentSkipListSet<>();
    this.windowDurationsNanos = new ConcurrentLinkedQueue<>();
    this.tracesInProgress =
        CacheBuilder.newBuilder()
            .expireAfterWrite(builder.traceTimeout)
            .maximumSize(builder.maxInProgressTraces)
            .build();
    this.completedTraces =
        CacheBuilder.newBuilder()
            .expireAfterWrite(builder.traceTimeout)
            .maximumSize(builder.maxInProgressTraces)
            .build();
    long windowSecs = Math.max(1, builder.windowSize.toSeconds());
    this.maxExportsPerWindow =
        (int) Math.max(1, windowSecs / Math.max(1, builder.minExportIntervalSeconds));
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.scheduler.scheduleAtFixedRate(this::drainWindow, windowSecs, windowSecs, TimeUnit.SECONDS);
  }

  /**
   * Creates a new builder for configuring the processor.
   *
   * @param exporter The {@link SpanExporter} to which the sampled traces will be exported.
   * @return A new {@link Builder}.
   */
  public static Builder builder(SpanExporter exporter) {
    return new Builder(exporter);
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {}

  @Override
  public void onEnd(ReadableSpan span) {
    String traceId = span.getSpanContext().getTraceId();

    // The root span already ended for this trace; this is a late-arriving child. Drop it rather
    // than resurrecting a trace entry that would leak until expiry (and whose spans would never be
    // exported anyway, since the sampling decision was already made).
    if (this.completedTraces.getIfPresent(traceId) != null) {
      return;
    }

    List<ReadableSpan> spansForTrace =
        this.tracesInProgress.asMap().computeIfAbsent(traceId, k -> new CopyOnWriteArrayList<>());
    spansForTrace.add(span);

    // If the root span completes between the check above and computeIfAbsent, re-check and remove
    // any recreated entry
    if (this.completedTraces.getIfPresent(traceId) != null) {
      this.tracesInProgress.asMap().remove(traceId, spansForTrace);
      return;
    }

    // A root span has no local parent: its parent context is either invalid (a true root) or
    // remote (a local root continuing a distributed trace). Either way, its end signifies the trace
    // is complete and is the trigger for our sampling decision.
    SpanContext parentCtx = span.getParentSpanContext();
    if (!parentCtx.isValid() || parentCtx.isRemote()) {
      this.completedTraces.put(traceId, Boolean.TRUE);
      this.tracesInProgress.invalidate(traceId);
      processCompletedTrace(spansForTrace, span);
    }
  }

  private void processCompletedTrace(List<ReadableSpan> traceSpans, ReadableSpan rootSpan) {
    long rootDurationNanos = rootSpan.getLatencyNanos();
    if (this.exportStrategy == ExportStrategy.PERCENTILE) {
      this.windowDurationsNanos.add(rootDurationNanos);
    }
    TraceRecord currentTrace =
        new TraceRecord(traceSpans, rootDurationNanos, rootSpan.getSpanContext().getTraceId());
    this.topTraceRecords.add(currentTrace);

    // Bound the retained pool so a burst between window drains cannot grow unbounded.
    int cap = this.exportStrategy == ExportStrategy.TOP_N ? this.topN : this.maxRetainedTraces;
    while (this.topTraceRecords.size() > cap) {
      this.topTraceRecords.pollFirst();
    }
  }

  private CompletableResultCode drainWindow() {
    return switch (this.exportStrategy) {
      case TOP_N -> drainTopN();
      case PERCENTILE -> drainPercentile();
    };
  }

  private CompletableResultCode drainTopN() {
    // topN value is validated/bounded on the MMS config side, so a full drain here is already
    // export-budgeted.
    List<CompletableResultCode> results = new ArrayList<>();
    @Var TraceRecord traceRecord;
    while ((traceRecord = this.topTraceRecords.pollLast()) != null) {
      results.add(exportTrace(traceRecord));
    }
    return CompletableResultCode.ofAll(results);
  }

  private CompletableResultCode drainPercentile() {
    // Drain the full window's durations (uncapped) to compute the threshold.
    List<Long> allDurations = new ArrayList<>();
    @Var Long duration;
    while ((duration = this.windowDurationsNanos.poll()) != null) {
      allDurations.add(duration);
    }
    if (allDurations.isEmpty()) {
      return CompletableResultCode.ofSuccess();
    }
    allDurations.sort(Comparator.naturalOrder());
    // Compute the index for the configured percentile (e.g. 0.99 → p99).
    int thresholdIdx = Math.max(0, (int) Math.ceil(this.percentile * allDurations.size()) - 1);
    long thresholdNanos = allDurations.get(thresholdIdx);

    // Atomically drain the (capped) retention pool via pollLast (slowest first) so records added
    // concurrently during the drain are simply carried into the next window rather than lost.
    List<TraceRecord> drained = new ArrayList<>();
    @Var TraceRecord record;
    while ((record = this.topTraceRecords.pollLast()) != null) {
      drained.add(record);
    }
    // Export the slowest retained traces at or above the threshold, but no more than the
    // per-window budget, so we honor the token-bucket export rate.
    List<CompletableResultCode> results = new ArrayList<>();
    @Var int exported = 0;
    for (TraceRecord candidate : drained) {
      if (exported >= this.maxExportsPerWindow) {
        break;
      }
      if (candidate.rootSpanDurationNanos() >= thresholdNanos) {
        results.add(exportTrace(candidate));
        exported++;
      }
    }
    return CompletableResultCode.ofAll(results);
  }

  private CompletableResultCode exportTrace(TraceRecord traceRecord) {
    List<SpanData> spanData =
        traceRecord.spans().stream().map(ReadableSpan::toSpanData).collect(Collectors.toList());
    return this.spanExporter.export(spanData);
  }

  // Delegate lifecycle methods
  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }

  @Override
  public CompletableResultCode shutdown() {
    this.scheduler.shutdown();
    try {
      if (!this.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        this.scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      this.scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
    this.tracesInProgress.invalidateAll();
    this.completedTraces.invalidateAll();
    this.topTraceRecords.clear();
    this.windowDurationsNanos.clear();
    return this.spanExporter.shutdown();
  }

  @Override
  public CompletableResultCode forceFlush() {
    CompletableResultCode drainResult = drainWindow();
    return CompletableResultCode.ofAll(List.of(drainResult, this.spanExporter.flush()));
  }

  /**
   * Holds a completed trace and its root span's duration. It implements Comparable to allow sorting
   * based on the root span's latency.
   */
  private record TraceRecord(List<ReadableSpan> spans, long rootSpanDurationNanos, String traceId)
      implements Comparable<TraceRecord> {
    @Override
    public int compareTo(TraceRecord other) {
      int durationCompare = Long.compare(this.rootSpanDurationNanos, other.rootSpanDurationNanos);
      if (durationCompare != 0) {
        return durationCompare;
      }
      return this.traceId.compareTo(other.traceId); // Tie-breaker for uniqueness
    }
  }

  /** A fluent builder for creating {@link LatencySamplingSpanProcessor} instances. */
  public static class Builder {
    private final SpanExporter spanExporter;
    private int topN = 10;
    private double percentile = 0.99;
    private ExportStrategy exportStrategy = ExportStrategy.TOP_N;
    private Duration traceTimeout = Duration.ofMinutes(30);
    private Duration windowSize = Duration.ofSeconds(300);
    private long maxInProgressTraces = 10_000;
    // Minimum steady-state interval between exported traces. windowSize / this yields the number of
    // traces exported per drain (the token-bucket budget). Defaults to one trace per 30s.
    private long minExportIntervalSeconds = 30;
    private int maxRetainedTraces = 10_000;

    Builder(SpanExporter spanExporter) {
      this.spanExporter = spanExporter;
    }

    public Builder setTopN(int topN) {
      if (topN <= 0) {
        throw new IllegalArgumentException("topN must be positive.");
      }
      this.topN = topN;
      this.exportStrategy = ExportStrategy.TOP_N;
      return this;
    }

    public Builder setPercentile(double percentile) {
      if (percentile <= 0.0 || percentile > 1.0) {
        throw new IllegalArgumentException("percentile must be in (0, 1].");
      }
      this.percentile = percentile;
      this.exportStrategy = ExportStrategy.PERCENTILE;
      return this;
    }

    public Builder setWindowSize(Duration windowSize) {
      this.windowSize = Objects.requireNonNull(windowSize, "windowSize cannot be null");
      return this;
    }

    public Builder setTraceTimeout(Duration timeout) {
      this.traceTimeout = Objects.requireNonNull(timeout, "timeout cannot be null");
      return this;
    }

    public Builder setMaxInProgressTraces(long max) {
      this.maxInProgressTraces = max;
      return this;
    }

    public Builder setMinExportIntervalSeconds(long minExportIntervalSeconds) {
      if (minExportIntervalSeconds <= 0) {
        throw new IllegalArgumentException("minExportIntervalSeconds must be positive.");
      }
      this.minExportIntervalSeconds = minExportIntervalSeconds;
      return this;
    }

    public Builder setMaxRetainedTraces(int maxRetainedTraces) {
      if (maxRetainedTraces <= 0) {
        throw new IllegalArgumentException("maxRetainedTraces must be positive.");
      }
      this.maxRetainedTraces = maxRetainedTraces;
      return this;
    }

    public LatencySamplingSpanProcessor build() {
      return new LatencySamplingSpanProcessor(this);
    }
  }
}
