package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

class JaegerSpan {
  @SuppressWarnings("unused")
  @JsonProperty(value = "traceID")
  private final String traceId;

  @SuppressWarnings("unused")
  @JsonProperty(value = "spanID")
  private final String spanId;

  @SuppressWarnings("unused")
  @JsonProperty(value = "operationName")
  private final String operationName;

  @SuppressWarnings("unused")
  @JsonProperty(value = "parentSpanId")
  private final String parentSpanId;

  @SuppressWarnings("unused")
  @JsonProperty(value = "references")
  private final List<Reference> references;

  @SuppressWarnings("unused")
  @JsonProperty(value = "startTime")
  private final long startTime;

  @SuppressWarnings("unused")
  @JsonProperty(value = "duration")
  private final long duration;

  @SuppressWarnings("unused")
  @JsonProperty(value = "tags")
  private final List<Tag> tags;

  @SuppressWarnings("unused")
  @JsonProperty(value = "processID")
  private final String processId;

  JaegerSpan(
      String traceId,
      String spanId,
      String operationName,
      String parentSpanId,
      List<Reference> references,
      long startTime,
      long duration,
      List<Tag> tags,
      String processId) {
    this.traceId = traceId;
    this.spanId = spanId;
    this.operationName = operationName;
    this.parentSpanId = parentSpanId;
    this.references = references;
    this.startTime = startTime;
    this.duration = duration;
    this.tags = tags;
    this.processId = processId;
  }
}
