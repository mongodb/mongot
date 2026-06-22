package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * The {@code searchQueryAdmissionControl} block of the Community mongot config file. So the query
 * load-shedding knobs can be tuned in community.
 */
public record RegularBlockingRequestConfig(
    Optional<Double> threadPoolSizeMultiplier,
    Optional<Double> queueCapacityMultiplier,
    Optional<Boolean> virtualQueueCapacity)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<Double> THREAD_POOL_SIZE_MULTIPLIER =
        Field.builder("threadPoolSizeMultiplier")
            .doubleField()
            .mustBeNonNegative()
            .optional()
            .noDefault();

    public static final Field.Optional<Double> QUEUE_CAPACITY_MULTIPLIER =
        Field.builder("queueCapacityMultiplier")
            .doubleField()
            .mustBeNonNegative()
            .optional()
            .noDefault();

    public static final Field.Optional<Boolean> VIRTUAL_QUEUE_CAPACITY =
        Field.builder("virtualQueueCapacity").booleanField().optional().noDefault();
  }

  public static RegularBlockingRequestConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new RegularBlockingRequestConfig(
        parser.getField(Fields.THREAD_POOL_SIZE_MULTIPLIER).unwrap(),
        parser.getField(Fields.QUEUE_CAPACITY_MULTIPLIER).unwrap(),
        parser.getField(Fields.VIRTUAL_QUEUE_CAPACITY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.THREAD_POOL_SIZE_MULTIPLIER, this.threadPoolSizeMultiplier)
        .field(Fields.QUEUE_CAPACITY_MULTIPLIER, this.queueCapacityMultiplier)
        .field(Fields.VIRTUAL_QUEUE_CAPACITY, this.virtualQueueCapacity)
        .build();
  }
}
