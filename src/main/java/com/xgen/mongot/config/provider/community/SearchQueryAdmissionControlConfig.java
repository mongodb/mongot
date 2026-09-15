package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/**
 * The {@code searchQueryAdmissionControl} block of the Community mongot config file. So the query
 * load-shedding knobs can be tuned in community.
 */
public record SearchQueryAdmissionControlConfig(
    double threadPoolSizeMultiplier,
    double queueCapacityMultiplier,
    boolean virtualQueueCapacity,
    boolean shouldRetryViaLoadBalancer)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.WithDefault<Double> THREAD_POOL_SIZE_MULTIPLIER =
        Field.builder("threadPoolSizeMultiplier")
            .doubleField()
            .mustBeNonNegative()
            .optional()
            .withDefault(3.0);

    public static final Field.WithDefault<Double> QUEUE_CAPACITY_MULTIPLIER =
        Field.builder("queueCapacityMultiplier")
            .doubleField()
            .mustBeNonNegative()
            .optional()
            .withDefault(30.0);

    public static final Field.WithDefault<Boolean> VIRTUAL_QUEUE_CAPACITY =
        Field.builder("virtualQueueCapacity").booleanField().optional().withDefault(Boolean.TRUE);

    public static final Field.WithDefault<Boolean> SHOULD_RETRY_VIA_LOAD_BALANCER =
        Field.builder("shouldRetryViaLoadBalancer")
            .booleanField()
            .optional()
            .withDefault(Boolean.FALSE);
  }

  public static SearchQueryAdmissionControlConfig defaults() {
    return new SearchQueryAdmissionControlConfig(
        Fields.THREAD_POOL_SIZE_MULTIPLIER.getDefaultValue(),
        Fields.QUEUE_CAPACITY_MULTIPLIER.getDefaultValue(),
        Fields.VIRTUAL_QUEUE_CAPACITY.getDefaultValue(),
        Fields.SHOULD_RETRY_VIA_LOAD_BALANCER.getDefaultValue());
  }

  public static SearchQueryAdmissionControlConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SearchQueryAdmissionControlConfig(
        parser.getField(Fields.THREAD_POOL_SIZE_MULTIPLIER).unwrap(),
        parser.getField(Fields.QUEUE_CAPACITY_MULTIPLIER).unwrap(),
        parser.getField(Fields.VIRTUAL_QUEUE_CAPACITY).unwrap(),
        parser.getField(Fields.SHOULD_RETRY_VIA_LOAD_BALANCER).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.THREAD_POOL_SIZE_MULTIPLIER, this.threadPoolSizeMultiplier)
        .field(Fields.QUEUE_CAPACITY_MULTIPLIER, this.queueCapacityMultiplier)
        .field(Fields.VIRTUAL_QUEUE_CAPACITY, this.virtualQueueCapacity)
        .field(Fields.SHOULD_RETRY_VIA_LOAD_BALANCER, this.shouldRetryViaLoadBalancer)
        .build();
  }
}
