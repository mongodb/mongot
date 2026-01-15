package com.xgen.mongot.replication.mongodb.initialsync.config;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Duration;
import java.util.Optional;
import org.bson.BsonDocument;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InitialSyncConfig
 *
 * @param collectionScanTime The amount of time to spend on the collection scan phase.
 * @param changeStreamCatchupTimeout The amount of time to spend trying to catch up to the change
 *     stream.
 * @param changeStreamLagTime The amount of time to lag behind the end of the change stream.
 */
public record InitialSyncConfig(
    Duration collectionScanTime,
    Duration changeStreamCatchupTimeout,
    Duration changeStreamLagTime,
    boolean useAggregateCommand,
    boolean enableNaturalOrderScan,
    boolean avoidNaturalOrderScanSyncSourceChangeResync)
    implements DocumentEncodable {

  /** Parses InitialSyncConfig from the provided DocumentParser. */
  public static InitialSyncConfig fromOuterBson(DocumentParser parser) throws BsonParseException {
    return parser.getField(Fields.BUFFERLESS_CONFIG).unwrap();
  }

  /**
   * These field definitions are used for ser/des tests, on production values from MMS are parsed in
   * MmsConfig.InitialSyncConfig and passed via the constructor below.
   */
  static class Fields {
    public static final Field.Required<Long> COLLECTION_SCAN_TIME_MS =
        Field.builder("collectionScanTimeMs").longField().mustBePositive().required();

    public static final Field.Required<Long> CHANGE_STREAM_CATCHUP_TIMEOUT_MS =
        Field.builder("changeStreamCatchupTimeoutMs").longField().mustBePositive().required();

    public static final Field.Required<Long> CHANGE_STREAM_LAG_TIME_MS =
        Field.builder("changeStreamLagTimeMs").longField().mustBePositive().required();

    public static final Field.Required<Boolean> USE_AGGREGATE_COMMAND =
        Field.builder("useAggregateCommand").booleanField().required();

    public static final Field.WithDefault<Boolean> ENABLE_NATURAL_ORDER_SCAN =
        Field.builder("enableNaturalOrderScan").booleanField().optional().withDefault(false);

    public static final Field.WithDefault<Boolean>
        AVOID_NATURAL_ORDER_SCAN_SYNC_SOURCE_CHANGE_RESYNC =
            Field.builder("avoidNaturalOrderScanSyncSourceChangeResync")
                .booleanField()
                .optional()
                .withDefault(false);

    public static final Field.Required<InitialSyncConfig> BUFFERLESS_CONFIG =
        Field.builder("bufferless")
            .classField(InitialSyncConfig::fromBson, InitialSyncConfig::initialSyncConfigToBson)
            .disallowUnknownFields()
            .required();
  }

  private static final Logger LOG = LoggerFactory.getLogger(InitialSyncConfig.class);

  /** Default collection scan time of 5 minutes. */
  private static final Duration DEFAULT_COLLECTION_SCAN_TIME = Duration.ofMinutes(5);

  /** Default change stream catchup time of 5 minutes. */
  private static final Duration DEFAULT_CHANGE_STREAM_CATCHUP_TIMEOUT = Duration.ofMinutes(5);

  /** Default change stream event max lag time of 1 minute. */
  private static final Duration DEFAULT_CHANGE_STREAM_LAG_TIME = Duration.ofMinutes(1);

  public static InitialSyncConfig fromBson(DocumentParser parser) throws BsonParseException {
    Duration collectionScanTime =
        Duration.ofMillis(parser.getField(Fields.COLLECTION_SCAN_TIME_MS).unwrap());
    Duration changeStreamCatchupTimeout =
        Duration.ofMillis(parser.getField(Fields.CHANGE_STREAM_CATCHUP_TIMEOUT_MS).unwrap());
    Duration changeStreamLagTime =
        Duration.ofMillis(parser.getField(Fields.CHANGE_STREAM_LAG_TIME_MS).unwrap());
    Boolean useAggregateCommand = parser.getField(Fields.USE_AGGREGATE_COMMAND).unwrap();
    Boolean enableNaturalOrderScan = parser.getField(Fields.ENABLE_NATURAL_ORDER_SCAN).unwrap();
    Boolean avoidNaturalOrderScanSyncSourceChangeResync =
        parser.getField(Fields.AVOID_NATURAL_ORDER_SCAN_SYNC_SOURCE_CHANGE_RESYNC).unwrap();

    return new InitialSyncConfig(
        Optional.of(collectionScanTime),
        Optional.of(changeStreamCatchupTimeout),
        Optional.of(changeStreamLagTime),
        Optional.of(useAggregateCommand),
        Optional.of(enableNaturalOrderScan),
        Optional.of(avoidNaturalOrderScanSyncSourceChangeResync));
  }

  public InitialSyncConfig() {
    this(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @TestOnly
  public InitialSyncConfig(boolean avoidNaturalOrderScanSyncSourceChangeResync) {
    this(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(avoidNaturalOrderScanSyncSourceChangeResync));
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public InitialSyncConfig(
      Optional<Duration> collectionScanTime,
      Optional<Duration> changeStreamCatchupTimeout,
      Optional<Duration> changeStreamLagTime,
      Optional<Boolean> useAggregateCommand,
      Optional<Boolean> enableNaturalOrderScan,
      Optional<Boolean> avoidNaturalOrderScanSyncSourceChangeResync) {
    this(
        collectionScanTime.orElse(setDefault("collectionScanTimeMs", DEFAULT_COLLECTION_SCAN_TIME)),
        changeStreamCatchupTimeout.orElse(
            setDefault("changeStreamCatchupTimeoutMs", DEFAULT_CHANGE_STREAM_CATCHUP_TIMEOUT)),
        changeStreamLagTime.orElse(
            setDefault("changeStreamLagTimeMs", DEFAULT_CHANGE_STREAM_LAG_TIME)),
        useAggregateCommand.orElse(true),
        enableNaturalOrderScan.orElse(false),
        avoidNaturalOrderScanSyncSourceChangeResync.orElse(false));

    useAggregateCommand.ifPresent(
        value ->
            LOG.atInfo()
                .addKeyValue("useAggregateCommand", value)
                .log(
                    "useAggregateCommand was set to but will be "
                        + "ignored as the option is now always enabled"));

    LOG.info("enableNaturalOrderScan is {}", enableNaturalOrderScan.orElse(false));
    LOG.info(
        "avoidNaturalOrderScanSyncSourceChangeResync is {}",
        avoidNaturalOrderScanSyncSourceChangeResync.orElse(false));
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.BUFFERLESS_CONFIG, this).build();
  }

  public BsonDocument initialSyncConfigToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COLLECTION_SCAN_TIME_MS, this.collectionScanTime.toMillis())
        .field(Fields.CHANGE_STREAM_CATCHUP_TIMEOUT_MS, this.changeStreamCatchupTimeout.toMillis())
        .field(Fields.CHANGE_STREAM_LAG_TIME_MS, this.changeStreamLagTime.toMillis())
        .field(Fields.USE_AGGREGATE_COMMAND, this.useAggregateCommand)
        .field(Fields.ENABLE_NATURAL_ORDER_SCAN, this.enableNaturalOrderScan)
        .field(
            Fields.AVOID_NATURAL_ORDER_SCAN_SYNC_SOURCE_CHANGE_RESYNC,
            this.avoidNaturalOrderScanSyncSourceChangeResync)
        .build();
  }

  private static Duration setDefault(String field, Duration duration) {
    LOG.atInfo()
        .addKeyValue("field", field)
        .addKeyValue("defaultMs", duration.toMillis())
        .addKeyValue("defaultMinutes", duration.toMinutes())
        .log("Bufferless initial sync config field not provided, using default.");
    return duration;
  }
}
