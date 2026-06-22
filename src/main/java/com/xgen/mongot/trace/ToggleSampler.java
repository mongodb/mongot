package com.xgen.mongot.trace;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/**
 * A {@link Sampler} which can be toggled. It chooses to sample based on 2 factors, in order of
 * preference:
 *
 * <ol>
 *   <li>if the span has a boolean {@link AttributeKey} "toggleTrace," it will always/never sample.
 *   <li>if the attribute does not exist, the sampler's default behavior will be to utilize {@link
 *       Sampler#traceIdRatioBased(double)}, using a {@code defaultRatio} given at construction
 *       time.
 *       <ul>
 *         <li>Note that {@code defaultRatio} can be updated, but should only be done once,
 *             specifically when mongot receives the MmsConfig file at startup, which may include a
 *             custom default-ratio.
 *       </ul>
 * </ol>
 */
class ToggleSampler implements Sampler {

  private static final SamplingResult POSITIVE_SAMPLING_RESULT = SamplingResult.recordAndSample();
  private static final SamplingResult NEGATIVE_SAMPLING_RESULT = SamplingResult.drop();

  /**
   * A preset span boolean attribute which a ToggleSampler may detect, and correspondingly always
   * sample or never sample.
   */
  public static final AttributeKey<Boolean> TOGGLE_TRACE = AttributeKey.booleanKey("toggleTrace");

  private volatile Sampler defaultRatioSampler;
  private volatile double defaultRatio;

  /**
   * Default constructor for ToggleSampler. Sets the {@code defaultRatio} to 0, i.e. no sampling.
   */
  ToggleSampler() {
    this.defaultRatio = 0.0;
    this.defaultRatioSampler = Sampler.alwaysOff();
  }

  ToggleSampler(double rate) {
    this.defaultRatio = rate;
    this.defaultRatioSampler = Sampler.traceIdRatioBased(this.defaultRatio);
  }

  /**
   * Should only be used when mongot receives an MmsConfig file at startup, which may have a custom
   * default sampling-rate.
   *
   * @param rate The new default sampling-rate.
   */
  void updateDefaultSampleRate(double rate) {
    this.defaultRatio = rate;
    this.defaultRatioSampler = Sampler.traceIdRatioBased(this.defaultRatio);
  }

  public double getDefaultRatio() {
    return this.defaultRatio;
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {

    Boolean isToggled = attributes.get(TOGGLE_TRACE);
    if (isToggled != null) {
      return isToggled ? POSITIVE_SAMPLING_RESULT : NEGATIVE_SAMPLING_RESULT;
    }

    // use defaultRatio
    return this.defaultRatioSampler.shouldSample(
        parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return "ToggleSampler{" + String.format("%.6f", this.defaultRatio) + "}";
  }

  @Override
  public int hashCode() {
    return Double.valueOf(this.defaultRatio).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ToggleSampler that = (ToggleSampler) o;
    return Double.compare(that.getDefaultRatio(), getDefaultRatio()) == 0;
  }

  @Override
  public String toString() {
    return getDescription();
  }
}
