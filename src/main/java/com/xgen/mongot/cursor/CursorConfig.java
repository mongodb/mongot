package com.xgen.mongot.cursor;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Duration;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public class CursorConfig implements DocumentEncodable {

  private static final Duration DEFAULT_IDLE_CURSOR_HANDLING_RATE = Duration.ofHours(1);
  private static final Duration DEFAULT_CURSOR_IDLE_TIMEOUT = Duration.ofHours(2);

  /** https://github.com/mongodb/mongo/blob/r5.0.0-rc3/src/mongo/rpc/message.h#L45 */
  public static final Bytes DEFAULT_MESSAGE_SIZE_LIMIT = Bytes.ofBytes(48000000);

  /** https://github.com/mongodb/mongo/blob/r5.0.0-rc3/src/mongo/bson/util/builder.h#L69 */
  public static final Bytes DEFAULT_BSON_SIZE_SOFT_LIMIT = Bytes.ofBytes(16777216);

  /**
   * https://github.com/mongodb/mongo/blob/r5.0.0-rc3/jstests/noPassthrough/query_knobs_validation.js#L180
   */
  private static final Bytes DEFAULT_BSON_SIZE_HARD_LIMIT = Bytes.ofBytes(16793600);

  static final Range<Long> DEFAULT_CURSOR_ID_RANGE = Range.of(1L, Long.MAX_VALUE);

  private static class IdRange implements DocumentEncodable {

    private final long min;
    private final long max;

    private static class Fields {
      private static final Field.Required<Long> MIN = Field.builder("min").longField().required();
      private static final Field.Required<Long> MAX = Field.builder("max").longField().required();
    }

    IdRange(long min, long max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MIN, this.min)
          .field(Fields.MAX, this.max)
          .build();
    }
  }

  private static class Fields {
    private static final Field.Optional<Integer> IDLE_CURSOR_HANDLING_RATE_MS =
        Field.builder("idleCursorHandlingRateMs")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();
    private static final Field.Optional<Integer> CURSOR_IDLE_TIMEOUT_MS =
        Field.builder("cursorIdleTimeMs").intField().mustBePositive().optional().noDefault();
    private static final Field.Optional<Integer> MESSAGE_SIZE_LIMIT_BYTES =
        Field.builder("messageSizeLimitBytes").intField().mustBePositive().optional().noDefault();
    private static final Field.Optional<Integer> BSON_SIZE_SOFT_LIMIT_BYTES =
        Field.builder("bsonSizeSoftLimitBytes").intField().mustBePositive().optional().noDefault();
    private static final Field.Optional<Integer> BSON_SIZE_HARD_LIMIT_BYTES =
        Field.builder("bsonSizeHardLimitBytes").intField().mustBePositive().optional().noDefault();

    private static final Field.Optional<IdRange> ID_RANGE =
        Field.builder("idRange")
            .classField(
                doc -> Check.unreachable("Deserialization not supported for IdRange"),
                IdRange::toBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  /** An interval to check and kill idle cursors. */
  final Duration idleCursorHandlingRate;

  /** Inactivity duration after which we consider a cursor as idle. */
  final Duration cursorIdleTime;

  /** MongoDB maximum message size. */
  public final Bytes messageSizeLimit;

  /** BSON document soft size limit, which MongoDB enforces for all stored documents. */
  public final Bytes bsonSizeSoftLimit;

  /**
   * BSON document hard size limit, which MongoDB enforces for processing. Includes 16KB overhead on
   * the BSON envelope.
   */
  final Bytes bsonSizeHardLimit;

  final Range<Long> cursorIdRange;

  private CursorConfig(
      Optional<Duration> idleCursorHandlingRate,
      Optional<Duration> cursorIdleTime,
      Optional<Bytes> messageSizeLimit,
      Optional<Bytes> bsonSizeSoftLimit,
      Optional<Bytes> bsonSizeHardLimit,
      Optional<Range<Long>> cursorIdRange) {
    this.idleCursorHandlingRate = idleCursorHandlingRate.orElse(DEFAULT_IDLE_CURSOR_HANDLING_RATE);
    this.cursorIdleTime = cursorIdleTime.orElse(DEFAULT_CURSOR_IDLE_TIMEOUT);
    this.messageSizeLimit = messageSizeLimit.orElse(DEFAULT_MESSAGE_SIZE_LIMIT);
    this.bsonSizeSoftLimit = bsonSizeSoftLimit.orElse(DEFAULT_BSON_SIZE_SOFT_LIMIT);
    this.bsonSizeHardLimit = bsonSizeHardLimit.orElse(DEFAULT_BSON_SIZE_HARD_LIMIT);
    this.cursorIdRange = cursorIdRange.orElse(DEFAULT_CURSOR_ID_RANGE);
  }

  public static CursorConfig create(
      Optional<Duration> idleCursorHandlingRate,
      Optional<Duration> cursorIdleTime,
      Optional<Bytes> messageSizeLimit,
      Optional<Bytes> bsonSizeSoftLimit,
      Optional<Bytes> bsonSizeHardLimit,
      Optional<Range<Long>> cursorIdRange) {
    return new CursorConfig(
        idleCursorHandlingRate,
        cursorIdleTime,
        messageSizeLimit,
        bsonSizeSoftLimit,
        bsonSizeHardLimit,
        cursorIdRange);
  }

  public static CursorConfig getDefault() {
    return new CursorConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(
            Fields.IDLE_CURSOR_HANDLING_RATE_MS,
            Optional.of(Math.toIntExact(this.idleCursorHandlingRate.toMillis())))
        .field(
            Fields.CURSOR_IDLE_TIMEOUT_MS,
            Optional.of(Math.toIntExact(this.cursorIdleTime.toMillis())))
        .field(
            Fields.MESSAGE_SIZE_LIMIT_BYTES,
            Optional.of(Math.toIntExact(this.messageSizeLimit.toBytes())))
        .field(
            Fields.BSON_SIZE_SOFT_LIMIT_BYTES,
            Optional.of(Math.toIntExact(this.bsonSizeSoftLimit.toBytes())))
        .field(
            Fields.BSON_SIZE_HARD_LIMIT_BYTES,
            Optional.of(Math.toIntExact(this.bsonSizeHardLimit.toBytes())))
        .field(
            Fields.ID_RANGE,
            Optional.of(
                new IdRange(this.cursorIdRange.getMinimum(), this.cursorIdRange.getMaximum())))
        .build();
  }
}
