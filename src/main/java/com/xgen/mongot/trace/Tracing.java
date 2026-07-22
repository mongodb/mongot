package com.xgen.mongot.trace;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.concurrent.LazyInit;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanLimits;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the OpenTelemetry Tracer. Helper methods are provided to simplify implementation of
 * tracing for methods of interest. Many methods return guards, or wrapper classes for spans, which
 * undertake the responsibility of closing / ending spans for you.
 */
public class Tracing {
  private static final Logger LOG = LoggerFactory.getLogger(Tracing.class);

  /**
   * When using {@link ToggleSampler}, an {@link AttributeKey} to toggle sampling on or off.:
   *
   * <pre>{@code
   * Attributes atr = Attributes.of(TOGGLE_TRACE, true);
   *
   * }</pre>
   *
   * <p>While {@code TOGGLE_TRACE} is more verbose than using {@link Tracing#TOGGLE_ON} or {@link
   * Tracing#TOGGLE_OFF}, it allows for adding other attributes at initialization:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo",
   *    Attributes.of(TOGGLE_TRACE, true, AttributeKey.doubleKey("myNum"), 2.0));
   *
   * }</pre>
   */
  public static final AttributeKey<Boolean> TOGGLE_TRACE = ToggleSampler.TOGGLE_TRACE;

  /**
   * When using {@link ToggleSampler}, an {@link Attributes} object to toggle sampling on:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo", TOGGLE_ON);
   * }</pre>
   *
   * <p>Note that while {@code TOGGLE_ON} is more succinct than using {@link Tracing#TOGGLE_TRACE},
   * it does not allow you to add any additional attributes to a new span at initialization.
   */
  public static final Attributes TOGGLE_ON = Attributes.of(ToggleSampler.TOGGLE_TRACE, true);

  /**
   * When using {@link ToggleSampler}, an {@link Attributes} object to toggle sampling off:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo", TOGGLE_OFF);
   *
   * }</pre>
   *
   * <p>Note that while {@code TOGGLE_OFF} is more succinct than using {@link Tracing#TOGGLE_TRACE},
   * it does not allow you to add any additional attributes to a new span at initialization.
   */
  public static final Attributes TOGGLE_OFF = Attributes.of(ToggleSampler.TOGGLE_TRACE, false);

  /**
   * An OpenTelemetry random {@link IdGenerator}. Use {@link IdGenerator#generateTraceId()} and
   * {@link IdGenerator#generateSpanId()} to create random TraceIds and SpanIds.
   */
  @VisibleForTesting static final IdGenerator ID_GENERATOR = IdGenerator.random();

  private enum SamplingStrategy {
    HEAD,
    TAIL;

    static SamplingStrategy parse(String value) {
      return "tail".equalsIgnoreCase(value) ? TAIL : HEAD;
    }
  }

  private static volatile @Nullable SamplingStrategy searchStrategy = null;
  private static volatile @Nullable SamplingStrategy vectorStrategy = null;
  private static volatile double searchSamplingRate = 0.0;
  private static volatile double vectorSamplingRate = 0.0;

  private static final String DEFAULT_REMOTE_SPAN_ID = ID_GENERATOR.generateSpanId();
  private static final ToggleSampler sampler = new ToggleSampler();

  private static final Lock LOCK = new ReentrantLock();

  private static final AttributeKey<String> MMS_GROUP_ID = AttributeKey.stringKey("mms.group_id");

  private static class TracingHolder {

    private static volatile @LazyInit SdkTracerProvider tracerProvider;
    private static volatile @LazyInit Tracer tracer;

    private static SdkTracerProvider currentProvider() {
      ensureInitialized();
      return tracerProvider;
    }

    // Lazily initializes tracing once in a thread-safe way.
    private static void ensureInitialized() {
      if (tracer == null) {
        LOCK.lock();
        try {
          if (tracer == null) {
            configure(defaultProcessor(), Optional.empty());
          }
        } finally {
          LOCK.unlock();
        }
      }
    }

    // Ensures spans are still logged locally even if bootstrap doesn't wire explicit tracing
    // config before something calls getTracer().
    private static SpanProcessor defaultProcessor() {
      return LatencySamplingSpanProcessor.builder(Slf4jExporter.create()).setTopN(10).build();
    }

    private static void configure(SpanProcessor processor, Optional<ObjectId> groupId) {
      Resource baseResource =
          Resource.getDefault()
              .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "mongot")));
      Resource resource =
          groupId
              .map(
                  id ->
                      baseResource.merge(
                          Resource.create(Attributes.of(MMS_GROUP_ID, id.toString()))))
              .orElse(baseResource);

      SdkTracerProvider newProvider =
          SdkTracerProvider.builder()
              .addSpanProcessor(processor)
              .setSpanLimits(
                  SpanLimits.getDefault().toBuilder().setMaxAttributeValueLength(1000).build())
              .setSampler(Sampler.parentBased(sampler))
              .setResource(resource)
              .build();

      SdkTracerProvider previousProvider = tracerProvider;
      tracerProvider = newProvider;
      tracer = newProvider.get("mongot-tracing", "0.0.1");

      if (previousProvider != null) {
        previousProvider.shutdown();
      }
    }
  }

  public static void initializeTracer(Optional<ObjectId> groupId, SpanProcessor processor) {
    LOCK.lock();
    try {
      TracingHolder.configure(processor, groupId);
    } finally {
      LOCK.unlock();
    }
  }

  public static Tracer getTracer() {
    TracingHolder.ensureInitialized();
    return TracingHolder.tracer;
  }

  /**
   * Sets mongot's global ToggleSampler instance to have a new default sampling-rate. Should only be
   * used by {@code MmsMongotBootstrapper.java} to update the default sampling-rate, when mongot
   * receives an MmsConfig file at startup.
   *
   * <p>Note: assumes the use of {@link ToggleSampler} as mongot's global {@link Sampler} instance
   * for tracing.
   *
   * @param newRate the new default sampling-rate. Must be in the range [0,1]. If {@code newRate} is
   *     not in this range, the sampling-rate will not be updated.
   */
  public static void updateSamplingRate(double newRate) {
    double oldRate = sampler.getDefaultRatio();
    if (newRate < 0.0 || newRate > 1.0) { // not a valid rate
      LOG.atInfo()
          .addKeyValue("oldRate", oldRate)
          .addKeyValue("newRate", newRate)
          .log(
              "Tried to update OpenTelemetry tracing sampling-rate, but the new rate was invalid.");
    } else {
      sampler.updateDefaultSampleRate(newRate);
      LOG.atInfo()
          .addKeyValue("oldRate", oldRate)
          .addKeyValue("newRate", newRate)
          .log("OpenTelemetry tracing sampling-rate updated.");
    }
  }

  /**
   * Note: assumes the use of {@link ToggleSampler} as mongot's global {@link Sampler} instance for
   * tracing.
   *
   * @return the default sampling-rate of the tracer
   */
  public static double getDefaultSamplingRate() {
    return sampler.getDefaultRatio();
  }

  /**
   * Creates a {@link SpanGuard}, that wraps around a newly created {@link Span} and opens a new
   * {@link Scope} around the span. When called within a try-with-resources block, the SpanGuard
   * will auto-close the span and its associated scope.
   *
   * @param name The name of the span
   * @return The created SpanGuard
   */
  @MustBeClosed
  public static SpanGuard simpleSpanGuard(String name) {
    return simpleSpanGuard(name, Attributes.empty());
  }

  /**
   * Creates a {@link SpanGuard}, that wraps around a newly created {@link Span} and opens a new
   * {@link Scope} around the span. When called within a try-with-resources block, the SpanGuard
   * will auto-close the span and its associated scope.
   *
   * @param name the name of the span
   * @param attributes any initial {@link Attributes} of the span
   * @return the created SpanGuard
   */
  @MustBeClosed
  public static SpanGuard simpleSpanGuard(String name, Attributes attributes) {
    Span span = unguardedSpan(name, attributes);
    return SpanGuard.fromSpan(span);
  }

  /**
   * Creates a new span with the given {@code name}, without any initial attributes. The method
   * starts the {@link Span}, but is not responsible with ending the span, or opening its scope.
   *
   * @param name the name of the span
   * @return the newly created Span
   */
  public static Span unguardedSpan(String name) {
    return unguardedSpan(name, Attributes.empty());
  }

  /**
   * Creates a new span with the given {@code name} and initialized with the given {@code
   * attributes}. The method starts the {@link Span}, but is not responsible with ending the span,
   * or opening its scope.
   *
   * @param name the name of the span
   * @param attributes any initial {@link Attributes} of the span
   * @return the newly created Span
   */
  public static Span unguardedSpan(String name, Attributes attributes) {
    return getTracer().spanBuilder(name).setAllAttributes(attributes).startSpan();
  }

  /**
   * Creates a new child {@link Span} which has its parentSpanId pointing to {@code parent}. Is
   * responsible for returning opening and closing the child span and {@link Scope}, and
   * activating/closing the parent scope. It is NOT responsible for ending the parent span, nor
   * creating the parent span.
   *
   * <p>Note that since the method opens the parent's scope, any other spans created during the
   * lifespan of the returned {@link ParentScopeGuard} will be placed as a child underneath the
   * {@code parent}.
   *
   * @param parent the parent Span of the child Span
   * @param childName the name of the new child span
   * @return a {@link ParentScopeGuard}, which wraps the parent scope and the child {@link
   *     SpanGuard}
   */
  @MustBeClosed
  public static ParentScopeGuard childUnderActivatedParent(Span parent, String childName) {
    return childUnderActivatedParent(parent, childName, Attributes.empty());
  }

  /**
   * Creates a new child {@link Span} which has its parentSpanId pointing to {@code parent}. Is
   * responsible for returning opening and closing the child span and {@link Scope}, and
   * activating/closing the parent scope. It is NOT responsible for ending the parent span, nor
   * creating the parent span.
   *
   * <p>Note that since the method opens the parent's scope, any other spans created during the
   * lifespan of the returned {@link ParentScopeGuard} will be placed as a child underneath the
   * {@code parent}.
   *
   * @param parent the parent Span of the child Span
   * @param childName the name of the new child span
   * @param childAttributes initial {@link Attributes} for the child span
   * @return a {@link ParentScopeGuard}, which wraps the parent scope and the child {@link
   *     SpanGuard}
   */
  @MustBeClosed
  public static ParentScopeGuard childUnderActivatedParent(
      Span parent, String childName, Attributes childAttributes) {
    return ParentScopeGuard.withChild(parent, childName, childAttributes);
  }

  /**
   * Creates a {@link Span} underneath a parent context with custom {@link TraceId} of {@code
   * traceId} and custom {@link TraceFlags} of {@code traceFlags}. This means:
   *
   * <ul>
   *   <li>the created span has the same traceId as its parent context.
   *   <li>the created span has the same sampling decision as its parent context. This is because by
   *       default, the global sampler will propagate a sampling decision of a context to all
   *       children created underneath that context.
   *       <ul>
   *         <li>The parent context's sampling decision is held within and determined by the
   *             parameter {@code traceFlags} (i.e. one of the bits of {@code traceFlags} is a
   *             sampling bit).
   *       </ul>
   * </ul>
   *
   * <p>Note that the {@code traceId} and {@code traceFlags} are associated with a parent context,
   * but the parent context is not linked to a real parent span:
   *
   * <ul>
   *   <li>there's no parent span that starts or ends.
   *   <li>there's no parent span that has an active scope open, and you never have to worry about
   *       closing a scope.
   *   <li>there's no parent span that is sampled.
   *   <li>there's no parent span actively kept in memory.
   * </ul>
   *
   * <p>The parent context is to create a common ancestor for multiple sibling spans. Note how this
   * method differs from {@link Tracing#childUnderActivatedParent(Span, String)}, as here, the
   * "parent" span is not real.
   *
   * <p>Note: the new span does not accept initial attributes, i.e. it should not be manually
   * sampled by a {@link Tracing#TOGGLE_ON} attribute. This is to ensure that children created under
   * the parent context have the same sampling decision, i.e. the parent context's sampling
   * decision.
   *
   * <p>Sample usage:
   *
   * <pre>{@code
   * String traceId;
   * TraceFlags cachedFlags;
   * try (SpanGuard sibling1 = Tracing.simpleSpanGuard("sibling1")) {
   *   traceId = sibling1.getSpan().getSpanContext().getTraceId();
   *   cachedFlags = sibling1.getSpan().getSpanContext().getTraceFlags();
   * }
   * try (SpanGuard sibling2 = Tracing.withTraceId("sibling2", traceId, cachedFlags) {
   *   // do something
   * }
   * }</pre>
   *
   * @param name the name of the created child span
   * @param traceId the traceId of the parent context under which you place the child span. The
   *     child span will have the same traceId.
   * @param traceFlags the traceFlags of the parent context under which you place the child span.
   *     The child span will have the same traceFlags (includes sampling decision).
   * @return the created SpanGuard wrapping the child span
   */
  @MustBeClosed
  public static SpanGuard withTraceId(String name, String traceId, TraceFlags traceFlags) {
    SpanContext parentContext =
        SpanContext.createFromRemoteParent(
            traceId, DEFAULT_REMOTE_SPAN_ID, traceFlags, TraceState.getDefault());

    Span span =
        getTracer()
            .spanBuilder(name)
            .setAllAttributes(Attributes.empty())
            .setParent(Context.current().with(Span.wrap(parentContext)))
            .startSpan();

    return SpanGuard.fromSpan(span);
  }

  public static void updateStrategyConfig(
      Optional<String> searchSamplingStrategy,
      Optional<String> vectorSamplingStrategy,
      Optional<Double> searchTracingSamplerRate,
      Optional<Double> vectorTracingSamplerRate) {
    searchStrategy = searchSamplingStrategy.map(SamplingStrategy::parse).orElse(null);
    vectorStrategy = vectorSamplingStrategy.map(SamplingStrategy::parse).orElse(null);
    searchSamplingRate = searchTracingSamplerRate.orElse(0.0);
    vectorSamplingRate = vectorTracingSamplerRate.orElse(0.0);
  }

  /**
   * Creates a root {@link SpanGuard} for a {@code $search} operation, applying the configured
   * sampling strategy. If no strategy is set, falls back to {@link #simpleSpanGuard(String)}.
   */
  @MustBeClosed
  public static SpanGuard searchSpanGuard(String name) {
    return spanGuardWithStrategy(name, searchStrategy, searchSamplingRate);
  }

  /**
   * Creates a root {@link SpanGuard} for a {@code $vectorSearch} operation, applying the configured
   * sampling strategy. If no strategy is set, falls back to {@link #simpleSpanGuard(String)}.
   */
  @MustBeClosed
  public static SpanGuard vectorSearchSpanGuard(String name) {
    return spanGuardWithStrategy(name, vectorStrategy, vectorSamplingRate);
  }

  @MustBeClosed
  private static SpanGuard spanGuardWithStrategy(
      String name, @Nullable SamplingStrategy strategy, double rate) {
    if (strategy == null) {
      return simpleSpanGuard(name);
    }

    Attributes attrs =
        switch (strategy) {
          case HEAD -> ThreadLocalRandom.current().nextDouble() < rate ? TOGGLE_ON : TOGGLE_OFF;
          case TAIL -> TOGGLE_ON;
        };
    return simpleSpanGuard(name, attrs);
  }

  /**
   * This method force flushes any spans waiting in a batch, ready to be exported. This is
   * <b>not</b> recommended to be used, other than for diagnostic/testing purposes.
   *
   * @return the {@link CompletableResultCode} which OpenTelemetry tracers provide when they are
   *     force flushed
   */
  @VisibleForTesting
  static CompletableResultCode forceFlush() {
    return TracingHolder.currentProvider().forceFlush();
  }
}
