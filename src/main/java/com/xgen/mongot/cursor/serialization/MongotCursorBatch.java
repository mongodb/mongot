package com.xgen.mongot.cursor.serialization;

import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents a simple batch of results in response to a GetMoreCommand or a non-intermediate
 * SearchCommand. For a SearchCommand, the batch will also return Variables containing the
 * MetaResults.
 *
 * <pre>
 *   {
 *     "ok: 1,
 *     "cursor": {...},
 *     "explain": {...}
 *     "vars": {...}
 *   }
 * </pre>
 */
public record MongotCursorBatch(
    Optional<MongotCursorResult> cursor,
    Optional<SearchExplainInformation> explain,
    Optional<BsonValue> variables,
    int ok)
    implements DocumentEncodable {

  private static final String MAX_NAMESPACE = new String(new char[255]);

  private static final int OK_RESPONSE = 1;

  private static class Fields {

    private static final Field.Required<Integer> OK =
        Field.builder("ok").intField().mustBeWithinBounds(Range.of(0, 1)).required();

    private static final Field.Optional<MongotCursorResult> CURSOR =
        Field.builder("cursor")
            .classField(MongotCursorResult::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<SearchExplainInformation> EXPLAIN =
        Field.builder("explain")
            .classField(SearchExplainInformation::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<BsonValue> VARIABLES =
        Field.builder("vars").unparsedValueField().optional().noDefault();
  }

  public MongotCursorBatch(
      Optional<MongotCursorResult> cursor,
      Optional<SearchExplainInformation> explain,
      Optional<BsonValue> variables) {
    this(cursor, explain, variables, OK_RESPONSE);
  }

  /** Constructor used to calculate empty cursor batch size and for the GetMoreCommand. */
  public MongotCursorBatch(
      MongotCursorResult cursor,
      Optional<SearchExplainInformation> explain,
      Optional<BsonValue> variables) {
    this(Optional.of(cursor), explain, variables, OK_RESPONSE);
  }

  public MongotCursorBatch(MongotCursorResult cursor, Optional<BsonValue> variables) {
    this(cursor, Optional.empty(), variables);
  }

  public static MongotCursorBatch fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  public static MongotCursorBatch fromBson(DocumentParser parser) throws BsonParseException {
    return new MongotCursorBatch(
        parser.getField(Fields.CURSOR).unwrap(),
        parser.getField(Fields.EXPLAIN).unwrap(),
        parser.getField(Fields.VARIABLES).unwrap(),
        parser.getField(Fields.OK).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CURSOR, this.cursor)
        .field(Fields.VARIABLES, this.variables)
        .field(Fields.EXPLAIN, this.explain)
        .field(Fields.OK, this.ok)
        .build();
  }

  public MongotCursorResult getCursorExpected() {
    return Check.isPresent(this.cursor, "cursor");
  }

  /**
   * Calculates the size of a MongotCursorBatch with the given variables, explain result,
   * resultType, the maximum allowable namespace, any long value, the explain if present, and an
   * empty batch array.
   *
   * <p>The batch is represented as an empty {@link BsonArray} rather than {@link BsonNull} so the
   * array's own container overhead (length prefix + terminating byte) is included in the estimate
   * -- a real batch is always an array, and a null placeholder would understate the envelope size
   * by that overhead.
   */
  public static Bytes calculateEmptyBatchSize(
      Optional<BsonValue> variables, Optional<MongotCursorResult.Type> resultType) {
    MongotCursorResult emptyResult =
        new MongotCursorResult(Long.MAX_VALUE, new BsonArray(), MAX_NAMESPACE, resultType);
    return Bytes.ofBytes(
        new MongotCursorBatch(emptyResult, variables).toRawBson().getByteBuffer().remaining());
  }
}
