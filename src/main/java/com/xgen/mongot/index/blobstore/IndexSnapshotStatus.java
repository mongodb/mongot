package com.xgen.mongot.index.blobstore;

import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DateUtil;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Summary of the latest snapshot status used for index status reporting.
 *
 * @param optime resumable optime captured for the uploaded snapshot
 * @param lastUploadedTime snapshot last upload completion time
 * @param statusCode index status captured when the snapshot was uploaded
 */
public record IndexSnapshotStatus(
    BsonTimestamp optime, Instant lastUploadedTime, IndexStatus.StatusCode statusCode)
    implements DocumentEncodable {
  static class Fields {
    // Stored as int64 containing packed BsonTimestamp#getValue().
    public static final Field.Required<Long> OPTIME =
        Field.builder("optime").longField().required();
    public static final Field.Required<String> LAST_UPLOADED_TIME =
        Field.builder("lastUploadedTime").stringField().required();
    public static final Field.Required<IndexStatus.StatusCode> STATUS_CODE =
        Field.builder("statusCode")
            .enumField(IndexStatus.StatusCode.class)
            .asUpperUnderscore()
            .required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OPTIME, this.optime.getValue())
        .field(
            Fields.LAST_UPLOADED_TIME,
            DateUtil.GMT_DATETIME_SECONDS_FORMATTER.format(this.lastUploadedTime))
        .field(Fields.STATUS_CODE, this.statusCode)
        .build();
  }

  public static IndexSnapshotStatus fromBson(DocumentParser parser) throws BsonParseException {
    BsonTimestamp optime = new BsonTimestamp(parser.getField(Fields.OPTIME).unwrap());
    IndexStatus.StatusCode statusCode = parser.getField(Fields.STATUS_CODE).unwrap();
    String lastUploadedTimeRaw = parser.getField(Fields.LAST_UPLOADED_TIME).unwrap();
    Instant lastUploadedTime;
    try {
      lastUploadedTime =
          Instant.from(DateUtil.GMT_DATETIME_SECONDS_FORMATTER.parse(lastUploadedTimeRaw));
    } catch (DateTimeParseException e) {
      throw new BsonParseException(e);
    }
    return new IndexSnapshotStatus(optime, lastUploadedTime, statusCode);
  }
}
