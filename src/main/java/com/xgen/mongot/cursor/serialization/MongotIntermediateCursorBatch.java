package com.xgen.mongot.cursor.serialization;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents an initial response to an intermediate SearchCommand. Contains a {@link
 * MongotCursorBatch} of results for each of two cursors, the search result cursor and the meta
 * result cursor.
 *
 * <pre>
 *   {
 *     "ok: 1,
 *     "cursors": [
 *       {
 *         "ok": 1,
 *         "cursor": {...}
 *       },
 *       {
 *         "ok": 1,
 *         "cursor": {...}
 *       }
 *     ]
 *   }
 * </pre>
 */
public record MongotIntermediateCursorBatch(int ok, List<MongotCursorBatch> cursors)
    implements DocumentEncodable {

  private static final String MAX_NAMESPACE = new String(new char[255]);

  private static final int OK_RESPONSE = 1;

  private static class Fields {

    private static final Field.Required<Integer> OK =
        Field.builder("ok").intField().mustBeWithinBounds(Range.of(0, 1)).required();

    private static final Field.Required<List<MongotCursorBatch>> CURSORS =
        Field.builder("cursors")
            .classField(MongotCursorBatch::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  /**
   * Constructs a successful new MongotIntermediateCursorBatch with the given meta and search
   * MongotCursorBatches.
   */
  public MongotIntermediateCursorBatch(
      MongotCursorBatch metaCursor, MongotCursorBatch searchCursor, int ok) {
    this(ok, List.of(metaCursor, searchCursor));

    checkState(
        checkCursorType(metaCursor, MongotCursorResult.Type.META),
        "The first cursor must be of type META.");
    checkState(
        checkCursorType(searchCursor, MongotCursorResult.Type.RESULTS),
        "The second cursor must be of type RESULTS.");
  }

  public MongotIntermediateCursorBatch(
      MongotCursorBatch searchCursor, MongotCursorBatch metaCursor) {
    this(searchCursor, metaCursor, OK_RESPONSE);
  }

  @VisibleForTesting
  public static MongotIntermediateCursorBatch fromBson(BsonDocument document)
      throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  static MongotIntermediateCursorBatch fromBson(DocumentParser parser) throws BsonParseException {
    List<MongotCursorBatch> cursors = parser.getField(Fields.CURSORS).unwrap();
    return new MongotIntermediateCursorBatch(
        cursors.get(0), cursors.get(1), parser.getField(Fields.OK).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CURSORS, this.cursors)
        .field(Fields.OK, this.ok)
        .build();
  }

  /**
   * Calculates the size of a IntermediateMongotCursorBatch with two cursors that have the maximum
   * allowable namespace, any long value as an ID, but no batch. The first cursor will be of {@link
   * MongotCursorResult.Type} META and the second will be of type RESULTS.
   */
  public static Bytes calculateEmptyBatchSize() {
    return Bytes.ofBytes(
        new MongotIntermediateCursorBatch(
                getEmptyCursorBatch(Optional.of(MongotCursorResult.Type.META)),
                getEmptyCursorBatch(Optional.of(MongotCursorResult.Type.RESULTS)))
            .toRawBson()
            .getByteBuffer()
            .remaining());
  }

  private static MongotCursorBatch getEmptyCursorBatch(Optional<MongotCursorResult.Type> type) {
    return new MongotCursorBatch(
        new MongotCursorResult(Long.MAX_VALUE, new BsonArray(), MAX_NAMESPACE, type),
        Optional.empty());
  }

  private static boolean checkCursorType(
      MongotCursorBatch batch, MongotCursorResult.Type desiredType) {
    if (batch.cursor().isEmpty()) {
      return true; // cannot check type if cursor isn't present
    }

    return batch.cursor().get().getType().isPresent()
        && batch.cursor().get().getType().get() == desiredType;
  }
}
