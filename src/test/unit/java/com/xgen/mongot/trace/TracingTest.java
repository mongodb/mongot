package com.xgen.mongot.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.errorprone.annotations.MustBeClosed;
import com.xgen.testing.TestUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;

/**
 * Note: many unit tests in {@code TracingTest} do not utilize try-with-resources blocks where they
 * can, instead opting to manually close created {@link SpanGuard}s with a "finally" block. This is
 * because {@link MustBeClosed} detects try-finally blocks, but does not detect try-with-resources
 * blocks with resources created outside the frame of the try-finally block.
 */
public class TracingTest {

  private static final Logger tracingLogger = TestUtils.getClassLogger(Tracing.class);

  private static final double originalSamplingRate = Tracing.getDefaultSamplingRate();

  @After
  public void resetInitialSamplingRate() {
    Tracing.updateSamplingRate(originalSamplingRate);
  }

  @After
  public void resetStrategyConfig() {
    Tracing.updateStrategyConfig(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  public void testToggleOffTrace() {
    try (SpanGuard unsampledSpan = Tracing.simpleSpanGuard("unsampled")) {
      assertFalse(unsampledSpan.getSpan().isRecording());
    }
  }

  @Test
  public void testToggleOnTrace() {
    SpanGuard sampledSpan = Tracing.simpleSpanGuard("sampled", Tracing.TOGGLE_ON);
    try {
      assertEquals(sampledSpan.getSpan(), Span.current());
      assertTrue(sampledSpan.getSpan().isRecording());
    } finally {
      sampledSpan.close();
    }

    assertEquals(Span.getInvalid(), Span.current()); // check scope is closed and inactive
    assertFalse(sampledSpan.getSpan().isRecording()); // check span has ended
  }

  @Test
  public void testClosesAfterCaughtException() {
    SpanGuard guard = Tracing.simpleSpanGuard("exceptionThrown", Tracing.TOGGLE_ON);
    try {
      throw new ArrayIndexOutOfBoundsException();
    } catch (ArrayIndexOutOfBoundsException e) {
      guard.getSpan().recordException(e);
    } finally {
      guard.close();
    }

    assertEquals(Span.getInvalid(), Span.current());
    assertFalse(guard.getSpan().isRecording());
  }

  @Test
  @SuppressWarnings("MustBeClosedChecker")
  public void testClosesAfterUncaughtException() {
    SpanGuard guard = Tracing.simpleSpanGuard("exceptionThrown", Tracing.TOGGLE_ON);
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          try (guard) {
            throw new ArrayIndexOutOfBoundsException();
          }
        });

    assertEquals(Span.getInvalid(), Span.current()); // checks that the scope is closed
    assertFalse(guard.getSpan().isRecording());
  }

  @Test
  @SuppressWarnings("MustBeClosedChecker")
  public void testParentGuardClosesAfterException() {
    Span parentSpan = Tracing.unguardedSpan("parent", Tracing.TOGGLE_ON);
    ParentScopeGuard parentGuard =
        Tracing.childUnderActivatedParent(parentSpan, "child", Tracing.TOGGLE_ON);
    try {
      assertThrows(
          ArrayIndexOutOfBoundsException.class,
          () -> {
            try (parentGuard) {
              dummyFunction();
              throw new ArrayIndexOutOfBoundsException();
            }
          });

      assertEquals(
          Span.getInvalid(), Span.current()); // both parent and child scope should be closed
      assertFalse(parentGuard.getChildSpanGuard().getSpan().isRecording()); // child span ended
      assertTrue(parentGuard.getParentSpan().isRecording()); // parent span still on
    } finally {
      parentSpan.end();
    }

    assertFalse(parentGuard.getParentSpan().isRecording()); // parent span ended
  }

  @Test
  @SuppressWarnings("MustBeClosedChecker")
  public void testClosingParentBeforeChild() {
    Span parentSpan = Tracing.unguardedSpan("parentEarly", Tracing.TOGGLE_ON);

    ParentScopeGuard parentGuard =
        Tracing.childUnderActivatedParent(parentSpan, "childEarly", Tracing.TOGGLE_ON);
    try {
      try (parentGuard) {
        try (Scope parentScope =
            parentSpan.makeCurrent()) { // should "re-open" parentSpan, then close it early.
          dummyFunction();
        }
      }
    } finally {
      parentSpan.end();
    }

    assertEquals(Span.getInvalid(), Span.current()); // both parent and child scope should be closed
    assertFalse(parentSpan.isRecording()); // parent span ended
    assertFalse(parentGuard.getChildSpanGuard().getSpan().isRecording());
  }

  /** A simple test to ensure that if a sibling is created, it is linked to the correct traceId. */
  @Test
  public void testWithTraceId() {
    String sampleTraceId = Tracing.ID_GENERATOR.generateTraceId();
    try (SpanGuard guard = Tracing.withTraceId("sibling", sampleTraceId, TraceFlags.getDefault())) {
      assertTrue(guard.getSpan().getSpanContext().isValid());
      assertEquals(sampleTraceId, guard.getSpan().getSpanContext().getTraceId());
    }
  }

  /**
   * Tests to ensure that when {@link Tracing#withTraceId(String, String, TraceFlags)} is called,
   * the created sibling span will have the correct sampling decision, based on the attributes and
   * configurations of the sibling span itself.
   */
  @Test
  public void testWithTraceIdRespectsSamplingDecision() {
    String sampleTraceId = Tracing.ID_GENERATOR.generateTraceId();
    SpanGuard siblingGuard = Tracing.withTraceId("sibling", sampleTraceId, TraceFlags.getSampled());
    try {
      assertTrue(siblingGuard.getSpan().getSpanContext().isSampled());
    } finally {
      siblingGuard.close();
    }

    assertFalse(siblingGuard.getSpan().isRecording());

    try (SpanGuard siblingGuard2 =
        Tracing.withTraceId("sibling", sampleTraceId, TraceFlags.getDefault())) {
      assertFalse(siblingGuard2.getSpan().getSpanContext().isSampled());
      assertFalse(siblingGuard2.getSpan().isRecording());
    }
  }

  @Test
  public void testUpdatingSamplingRate() {
    Tracing.updateSamplingRate(1.0);

    try (var guard = Tracing.simpleSpanGuard("possiblyTracedFunction")) {
      assertTrue(guard.getSpan().isRecording());
    }
  }

  @Test
  public void testUpdatingWithInvalidRate() {
    List<ILoggingEvent> list = TestUtils.getLogEvents(tracingLogger);

    Tracing.updateSamplingRate(1.5);
    Tracing.updateSamplingRate(-0.5);
    String expectedLogFragment = "Tried to update OpenTelemetry tracing sampling-rate";
    long count =
        list.stream()
            .map(l -> l.getMessage())
            .filter(str -> str.contains(expectedLogFragment))
            .count();

    assertEquals(2, count);
  }

  @Test
  public void testVectorSearchSpanGuardHeadStrategySamplesAtRateOne() {
    Tracing.updateStrategyConfig(
        Optional.empty(), Optional.of("head"), Optional.empty(), Optional.of(1.0));

    try (var guard = Tracing.vectorSearchSpanGuard("vectorSearch")) {
      assertTrue(guard.getSpan().isRecording());
    }
  }

  @Test
  public void testVectorSearchSpanGuardHeadStrategyDoesNotSampleAtRateZero() {
    Tracing.updateStrategyConfig(
        Optional.empty(), Optional.of("head"), Optional.empty(), Optional.of(0.0));

    try (var guard = Tracing.vectorSearchSpanGuard("vectorSearch")) {
      assertFalse(guard.getSpan().isRecording());
    }
  }

  @Test
  public void testVectorSearchSpanGuardTailStrategyAlwaysSamples() {
    // TAIL sampling always tags the span TOGGLE_ON; the actual keep/drop decision happens
    // downstream, so the configured rate should have no bearing here.
    Tracing.updateStrategyConfig(
        Optional.empty(), Optional.of("tail"), Optional.empty(), Optional.of(0.0));

    try (var guard = Tracing.vectorSearchSpanGuard("vectorSearch")) {
      assertTrue(guard.getSpan().isRecording());
    }
  }

  @Test
  public void testSearchAndVectorSearchStrategiesAreConfiguredIndependently() {
    Tracing.updateStrategyConfig(
        Optional.of("tail"), Optional.of("head"), Optional.empty(), Optional.of(0.0));

    try (var searchGuard = Tracing.searchSpanGuard("search")) {
      assertTrue(searchGuard.getSpan().isRecording()); // tail strategy always samples
    }
    try (var vectorGuard = Tracing.vectorSearchSpanGuard("vectorSearch")) {
      assertFalse(vectorGuard.getSpan().isRecording()); // head strategy at rate 0.0
    }
  }

  private static int dummyFunction() {
    return 1 + 1;
  }
}
