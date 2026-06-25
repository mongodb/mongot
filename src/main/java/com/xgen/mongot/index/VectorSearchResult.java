package com.xgen.mongot.index;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
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
      MqlComparator.MIN_SELECTING_COMPARATOR;

  public static final Comparator<VectorSearchResult> VECTOR_SEARCH_RESULT_COMPARATOR =
      Comparator.comparingDouble((VectorSearchResult v) -> -v.vectorSearchScore())
          .thenComparing(v -> v.id(), BSON_VALUE_COMPARATOR);

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
