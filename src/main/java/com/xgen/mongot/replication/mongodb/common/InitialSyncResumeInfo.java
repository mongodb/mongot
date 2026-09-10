package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * InitialSyncResumeInfo holds information regarding how to resume an in-progress initial sync. It
 * can be either a buffered initial sync resuming from a collection scan or change stream
 * application, or a bufferless initial sync (which will always resume as a collection scan phase).
 *
 * <p>For a bufferless initial sync, it would be BufferlessInitialSyncIdOrderResumeInfo if _id scan
 * is used, BufferlessNaturalOrderInitialSyncResumeInfo if natural order scan is used, and
 * BufferlessParallelNaturalOrderInitialSyncResumeInfo if that natural order scan was split into
 * partitions. It would be empty when there is no resumeInfo commited yet
 */
public abstract class InitialSyncResumeInfo implements DocumentEncodable {
  protected static class Fields {

    @Deprecated
    private static final Field.Optional<BsonValue> COLLECTION_SCAN_RESUME_INFO =
        Field.builder("collectionScan").unparsedValueField().optional().noDefault();

    @Deprecated
    private static final Field.Optional<BsonValue> CHANGE_STREAM_APPLICATION_RESUME_INFO =
        Field.builder("changeStreamApplication").unparsedValueField().optional().noDefault();

    /**
     * BufferlessIdOrderInitialSyncResumeInfo stores _id scan resume info, field name
     * "bufferlessInitialSync" is unchanged to avoid a data migration
     */
    protected static final Field.Optional<BufferlessIdOrderInitialSyncResumeInfo>
        BUFFERLESS_RESUME_INFO =
            Field.builder("bufferlessInitialSync")
                .classField(
                    BufferlessIdOrderInitialSyncResumeInfo::deserialize,
                    InitialSyncResumeInfo::resumeInfoToBson)
                .disallowUnknownFields()
                .optional()
                .noDefault();

    /** BufferlessNaturalOrderInitialSyncResumeInfo stores natural order scan resume info */
    protected static final Field.Optional<BufferlessNaturalOrderInitialSyncResumeInfo>
        BUFFERLESS_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO =
            Field.builder("bufferlessNaturalOrderInitialSync")
                .classField(
                    BufferlessNaturalOrderInitialSyncResumeInfo::deserialize,
                    InitialSyncResumeInfo::resumeInfoToBson)
                .disallowUnknownFields()
                .optional()
                .noDefault();

    /**
     * BufferlessParallelNaturalOrderInitialSyncResumeInfo stores K-cursor parallel natural-order
     * scan resume info.
     */
    protected static final Field.Optional<BufferlessParallelNaturalOrderInitialSyncResumeInfo>
        BUFFERLESS_PARALLEL_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO =
            Field.builder("bufferlessParallelNaturalOrderInitialSync")
                .classField(
                    BufferlessParallelNaturalOrderInitialSyncResumeInfo::deserialize,
                    InitialSyncResumeInfo::resumeInfoToBson)
                .disallowUnknownFields()
                .optional()
                .noDefault();
  }

  /** Parses InitialSyncResumeInfo from the provided document. */
  public static Optional<InitialSyncResumeInfo> fromBson(BsonDocument document)
      throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  /** Parses InitialSyncResumeInfo from the provided DocumentParser. */
  public static Optional<InitialSyncResumeInfo> fromBson(DocumentParser parser)
      throws BsonParseException {

    if (parser.getField(Fields.COLLECTION_SCAN_RESUME_INFO).unwrap().isPresent()
        || parser.getField(Fields.CHANGE_STREAM_APPLICATION_RESUME_INFO).unwrap().isPresent()) {
      // we ignore buffered resume info as it's not supported anymore but could still
      // be encountered if mongot was using buffered implementation before
      return Optional.empty();
    }

    if (parser.getField(Fields.BUFFERLESS_RESUME_INFO).unwrap().isPresent()) {
      return parser.getField(Fields.BUFFERLESS_RESUME_INFO).unwrap().map(Function.identity());
    }

    if (parser
        .getField(Fields.BUFFERLESS_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO)
        .unwrap()
        .isPresent()) {
      return parser
          .getField(Fields.BUFFERLESS_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO)
          .unwrap()
          .map(Function.identity());
    }

    if (parser
        .getField(Fields.BUFFERLESS_PARALLEL_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO)
        .unwrap()
        .isPresent()) {
      return parser
          .getField(Fields.BUFFERLESS_PARALLEL_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO)
          .unwrap()
          .map(Function.identity());
    }

    return Optional.empty();
  }

  /** Turns this InitialSyncResumeInfo into JSON. */
  public String toJson() {
    return toBson()
        .asDocument()
        .toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
  }

  public static InitialSyncResumeInfo create(
      boolean naturalOrderScan,
      BsonTimestamp highWaterMark,
      BsonValue resumeToken,
      Optional<String> syncSourceHost) {
    if (naturalOrderScan) {
      return new BufferlessNaturalOrderInitialSyncResumeInfo(
          highWaterMark, resumeToken.asDocument(), syncSourceHost);
    }
    return new BufferlessIdOrderInitialSyncResumeInfo(highWaterMark, resumeToken);
  }

  /**
   * Turns this InitialSyncResumeInfo into BSON. Subclasses should override {@code
   * toBsonDocumentBuilder} instead of this method.
   */
  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().build();
  }

  /** Turns this InitialSyncResumeInfo into BSON, based on the subclass's BsonDocumentBuilder. */
  protected abstract BsonValue resumeInfoToBson();

  public boolean isBufferlessIdOrderInitialSyncResumeInfo() {
    return this instanceof BufferlessIdOrderInitialSyncResumeInfo;
  }

  public boolean isBufferlessNaturalOrderInitialSyncResumeInfo() {
    return this instanceof BufferlessNaturalOrderInitialSyncResumeInfo;
  }

  public boolean isBufferlessParallelNaturalOrderInitialSyncResumeInfo() {
    return this instanceof BufferlessParallelNaturalOrderInitialSyncResumeInfo;
  }

  /** Returns the opTime of the change stream high watermark. */
  public abstract BsonTimestamp getResumeOperationTime();

  public abstract BsonValue getResumeToken();

  public Optional<String> getSyncSourceHost() {
    return Optional.empty();
  }
}
