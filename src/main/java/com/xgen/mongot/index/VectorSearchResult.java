package com.xgen.mongot.index;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonType;
import org.bson.BsonValue;

/** VectorSearchResult is a successful result from a vector search query. */
public record VectorSearchResult(
    BsonValue id, float vectorSearchScore, Optional<BsonDocument> storedSource)
    implements DocumentEncodable {
  /**
   * The vector search results are first compared by the scores in descending order. Then id is used
   * for tie-breaking.
   */
  private static final Comparator<BsonValue> BSON_VALUE_COMPARATOR =
      VectorSearchResult::compareBsonValues;

  public static final Comparator<VectorSearchResult> VECTOR_SEARCH_RESULT_COMPARATOR =
      Comparator.comparingDouble((VectorSearchResult v) -> -v.vectorSearchScore())
          .thenComparing(v -> v.id(), BSON_VALUE_COMPARATOR);

  private static int compareBsonValues(BsonValue first, BsonValue second) {
    BsonType firstType = first.getBsonType();
    BsonType secondType = second.getBsonType();

    int bracketCompare =
        Integer.compare(
            SortUtil.getBracketPriority(firstType, NullEmptySortPosition.LOWEST),
            SortUtil.getBracketPriority(secondType, NullEmptySortPosition.LOWEST));
    if (bracketCompare != 0) {
      return bracketCompare;
    }

    return switch (firstType) {
      case DOUBLE, INT32, INT64, DECIMAL128 ->
          compareNumbers((BsonNumber) first, (BsonNumber) second);

      case STRING -> first.asString().getValue().compareTo(second.asString().getValue());

      case BINARY -> SortUtil.compare(first.asBinary(), second.asBinary());

      case OBJECT_ID -> first.asObjectId().compareTo(second.asObjectId());

      case BOOLEAN -> first.asBoolean().compareTo(second.asBoolean());

      case DATE_TIME -> Long.compare(first.asDateTime().getValue(), second.asDateTime().getValue());

      case TIMESTAMP -> first.asTimestamp().compareTo(second.asTimestamp());

      case NULL, MIN_KEY, MAX_KEY -> 0;

      case JAVASCRIPT -> first.asJavaScript().getCode().compareTo(second.asJavaScript().getCode());

      case DOCUMENT -> compareDocuments(first.asDocument(), second.asDocument());

      default ->
          throw new UnsupportedOperationException(
              String.format("Unsupported _id BSON type for comparison: %s", firstType));
    };
  }

  private static int compareNumbers(BsonNumber first, BsonNumber second) {
    BsonType firstType = first.getBsonType();
    BsonType secondType = second.getBsonType();

    if (firstType == BsonType.DOUBLE || secondType == BsonType.DOUBLE) {
      return SortUtil.mqlDoubleCompare(first.doubleValue(), second.doubleValue());
    }

    if (firstType == BsonType.DECIMAL128 || secondType == BsonType.DECIMAL128) {
      return first
          .decimal128Value()
          .bigDecimalValue()
          .compareTo(second.decimal128Value().bigDecimalValue());
    }

    if (firstType == BsonType.INT64 || secondType == BsonType.INT64) {
      return Long.compare(first.longValue(), second.longValue());
    }

    return Integer.compare(first.intValue(), second.intValue());
  }

  private static int compareDocuments(BsonDocument first, BsonDocument second) {
    var firstIt = first.entrySet().iterator();
    var secondIt = second.entrySet().iterator();

    while (firstIt.hasNext() && secondIt.hasNext()) {
      var firstEntry = firstIt.next();
      var secondEntry = secondIt.next();

      int typeCompare =
          Integer.compare(
              SortUtil.getBracketPriority(
                  firstEntry.getValue().getBsonType(), NullEmptySortPosition.LOWEST),
              SortUtil.getBracketPriority(
                  secondEntry.getValue().getBsonType(), NullEmptySortPosition.LOWEST));
      if (typeCompare != 0) {
        return typeCompare;
      }

      int keyCompare = firstEntry.getKey().compareTo(secondEntry.getKey());
      if (keyCompare != 0) {
        return keyCompare;
      }

      int valueCompare = compareBsonValues(firstEntry.getValue(), secondEntry.getValue());
      if (valueCompare != 0) {
        return valueCompare;
      }
    }

    return Boolean.compare(firstIt.hasNext(), secondIt.hasNext());
  }

  private static class Fields {
    private static final Field.Required<BsonValue> ID =
        Field.builder("_id").unparsedValueField().required();

    private static final Field.Required<Float> VECTOR_SEARCH_SCORE =
        Field.builder("$vectorSearchScore").floatField().required();

    private static final Field.Optional<BsonDocument> STORED_SOURCE =
        Field.builder("storedSource").documentField().optional().noDefault();
  }

  public VectorSearchResult {
    if (Float.isNaN(vectorSearchScore)) {
      // should never happen
      throw new IllegalArgumentException("vectorSearchScore is NaN");
    }
  }

  public static VectorSearchResult fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  public static VectorSearchResult fromBson(DocumentParser parser) throws BsonParseException {
    return new VectorSearchResult(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.VECTOR_SEARCH_SCORE).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap());
  }

  public static DocumentEncodable create(
      Optional<BsonValue> id,
      float score,
      Optional<List<SearchHighlight>> searchHighlights,
      Optional<ScoreDetails> scoreDetails,
      Optional<BsonDocument> storedSource,
      Optional<SearchSortValues> searchSortValues,
      Optional<SequenceToken> sequenceToken,
      Optional<BsonValue> searchRootDocumentId) {
    Check.isEmpty(searchHighlights, "searchHighlights");
    Check.isEmpty(scoreDetails, "scoreDetails");
    Check.isEmpty(searchSortValues, "searchSortValues");
    Check.isEmpty(sequenceToken, "sequenceToken");
    Check.isEmpty(searchRootDocumentId, "searchRootDocumentId");
    return new VectorSearchResult(id.get(), score, storedSource);
  }

  @Override
  public BsonDocument toBson() {
    @Var var builder = BsonDocumentBuilder.builder();
    builder = builder.field(Fields.ID, this.id);
    builder = builder.field(Fields.VECTOR_SEARCH_SCORE, this.vectorSearchScore);
    if (this.storedSource.isPresent()) {
      builder = builder.field(Fields.STORED_SOURCE, this.storedSource);
    }
    return builder.build();
  }
}
