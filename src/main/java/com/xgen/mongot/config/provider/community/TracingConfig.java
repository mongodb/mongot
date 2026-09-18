package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

/**
 * The self-managed {@code tracing} block of the Community mongot config file. Controls enablement
 * and sampling for mongot-local OpenTelemetry query tracing; traces are written through the
 * existing structured JSON log pipeline.
 *
 * <p>The block is optional and disabled by default. Only enablement and sampling are configurable:
 * latency-sampling parameters such as the export window and top-N count use the processor's
 * internal defaults. The field names match the existing mongot tracing configuration vocabulary.
 *
 * <p>{@code enabled} is read once at startup and there is no runtime kill switch: to stop tracing,
 * set it to {@code false} and restart mongot. Runtime enable/disable is deliberately out of scope
 * for this Community opt-in; tracing is off by default and only writes to the existing log
 * pipeline.
 */
public record TracingConfig(
    boolean enabled,
    SamplingStrategy searchSamplingStrategy,
    SamplingStrategy vectorSamplingStrategy,
    double searchTracingSamplerRate,
    double vectorTracingSamplerRate)
    implements DocumentEncodable {

  /** Sampling mode for query traces. */
  public enum SamplingStrategy {
    HEAD,
    TAIL
  }

  private static final boolean DEFAULT_ENABLED = false;
  private static final SamplingStrategy DEFAULT_SEARCH_SAMPLING_STRATEGY = SamplingStrategy.TAIL;
  private static final SamplingStrategy DEFAULT_VECTOR_SAMPLING_STRATEGY = SamplingStrategy.HEAD;
  private static final double DEFAULT_SEARCH_TRACING_SAMPLER_RATE = 0.01;
  private static final double DEFAULT_VECTOR_TRACING_SAMPLER_RATE = 0.01;

  private static class Fields {
    public static final Field.WithDefault<Boolean> ENABLED =
        Field.builder("enabled").booleanField().optional().withDefault(DEFAULT_ENABLED);

    public static final Field.WithDefault<SamplingStrategy> SEARCH_SAMPLING_STRATEGY =
        Field.builder("searchSamplingStrategy")
            .enumField(SamplingStrategy.class)
            .asCamelCase()
            .optional()
            .withDefault(DEFAULT_SEARCH_SAMPLING_STRATEGY);

    public static final Field.WithDefault<SamplingStrategy> VECTOR_SAMPLING_STRATEGY =
        Field.builder("vectorSamplingStrategy")
            .enumField(SamplingStrategy.class)
            .asCamelCase()
            .optional()
            .withDefault(DEFAULT_VECTOR_SAMPLING_STRATEGY);

    public static final Field.WithDefault<Double> SEARCH_TRACING_SAMPLER_RATE =
        Field.builder("searchTracingSamplerRate")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .optional()
            .withDefault(DEFAULT_SEARCH_TRACING_SAMPLER_RATE);

    public static final Field.WithDefault<Double> VECTOR_TRACING_SAMPLER_RATE =
        Field.builder("vectorTracingSamplerRate")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .optional()
            .withDefault(DEFAULT_VECTOR_TRACING_SAMPLER_RATE);
  }

  public static TracingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new TracingConfig(
        parser.getField(Fields.ENABLED).unwrap(),
        parser.getField(Fields.SEARCH_SAMPLING_STRATEGY).unwrap(),
        parser.getField(Fields.VECTOR_SAMPLING_STRATEGY).unwrap(),
        parser.getField(Fields.SEARCH_TRACING_SAMPLER_RATE).unwrap(),
        parser.getField(Fields.VECTOR_TRACING_SAMPLER_RATE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLED, this.enabled)
        .field(Fields.SEARCH_SAMPLING_STRATEGY, this.searchSamplingStrategy)
        .field(Fields.VECTOR_SAMPLING_STRATEGY, this.vectorSamplingStrategy)
        .field(Fields.SEARCH_TRACING_SAMPLER_RATE, this.searchTracingSamplerRate)
        .field(Fields.VECTOR_TRACING_SAMPLER_RATE, this.vectorTracingSamplerRate)
        .build();
  }
}
