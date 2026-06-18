package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Represents the input for auto-embedding vector search queries. Can be either a simple text string
 * (backward compatible) or a structured document for multi-modal queries.
 *
 * <p>The frontend enforces mutual exclusivity between {@code query} and {@code queryVector} fields,
 * so they will never coexist in a request. When {@code query} is present, it indicates an
 * auto-embedding search where mongot will generate the vector from the text.
 *
 * <p>Downstream behavior (see {@code VectorSearchCommand.embedQueryText}):
 *
 * <ul>
 *   <li>{@code {query: "some text"}} → Auto-embedding with the provided text
 *   <li>{@code {query: {text: "some text"}}} → Auto-embedding with the provided text
 *   <li>{@code {query: ""}} or {@code {query: {text: ""}}} → Error (empty text not allowed for
 *       auto-embedding)
 * </ul>
 */
public sealed interface VectorSearchQueryInput extends DocumentEncodable
    permits VectorSearchQueryInput.Text, VectorSearchQueryInput.MultiModal {

  /** Returns the model name to use for auto-embedding. If not specified, uses the index's model. */
  Optional<String> getModel();

  class Fields {
    static final Field.Optional<String> MODEL =
        Field.builder("model").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<BsonValue> QUERY =
        Field.builder("query").unparsedValueField().optional().noDefault();
  }

  /** Simple text-only query input. Used for backward compatibility with {query: "text"} format. */
  record Text(String text, Optional<String> model) implements VectorSearchQueryInput {

    public Text(String text) {
      this(text, Optional.empty());
    }

    @Override
    public Optional<String> getModel() {
      return this.model;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.QUERY, Optional.of(new BsonString(this.text)))
          .field(Fields.MODEL, this.model)
          .build();
    }
  }

  /**
   * Multi-modal query input supporting text and future modalities. Used for the new document
   * format: {query: {text: "..."}}. Additional modalities (e.g., image) can be added in the future.
   */
  record MultiModal(Optional<String> text, Optional<String> model)
      implements VectorSearchQueryInput {

    public MultiModal(Optional<String> text) {
      this(text, Optional.empty());
    }

    private static class Fields {
      static final Field.Optional<String> TEXT =
          Field.builder("text").stringField().optional().noDefault();
    }

    static MultiModal fromBson(
        BsonParseContext context, BsonDocument document, Optional<String> model)
        throws BsonParseException {

      try (var documentParser =
          BsonDocumentParser.withContext(context, document).allowUnknownFields(false).build()) {
        var text = documentParser.getField(Fields.TEXT).unwrap();
        return new MultiModal(text, model);
      }
    }

    @Override
    public Optional<String> getModel() {
      return this.model;
    }

    @Override
    public BsonDocument toBson() {
      BsonDocument queryDocument =
          BsonDocumentBuilder.builder().field(Fields.TEXT, this.text).build();
      return BsonDocumentBuilder.builder()
          .field(VectorSearchQueryInput.Fields.QUERY, Optional.of(queryDocument))
          .field(VectorSearchQueryInput.Fields.MODEL, this.model)
          .build();
    }
  }

  /**
   * Parse VectorSearchQueryInput from BSON, supporting both string and document formats.
   *
   * <ul>
   *   <li>String format: {@code {query: "search text"}}
   *   <li>Document format: {@code {query: {text: "search text"}}}
   * </ul>
   *
   * @return Optional containing the parsed query input, or empty if no query field is present
   */
  static Optional<VectorSearchQueryInput> fromBson(DocumentParser parser)
      throws BsonParseException {
    BsonParseContext context = parser.getContext();

    Optional<BsonValue> maybeQueryValue = parser.getField(Fields.QUERY).unwrap();
    Optional<String> maybeModel = parser.getField(Fields.MODEL).unwrap();

    if (maybeModel.isPresent() && maybeQueryValue.isEmpty()) {
      context.handleSemanticError(
          "model can only be specified when using 'query' for auto-embedding, "
              + "not with 'queryVector'");
    }

    if (maybeQueryValue.isEmpty()) {
      return Optional.empty();
    }
    BsonValue queryValue = maybeQueryValue.get();

    VectorSearchQueryInput queryInput =
        switch (queryValue.getBsonType()) {
          case STRING -> new Text(queryValue.asString().getValue(), maybeModel);
          case DOCUMENT -> MultiModal.fromBson(context, queryValue.asDocument(), maybeModel);
          default ->
              context.handleSemanticError(
                  "query must be a string or document, but got " + queryValue.getBsonType());
        };
    return Optional.of(queryInput);
  }

  /**
   * Get the text component for embedding. Returns Optional.empty() if the text is null or empty.
   */
  default Optional<String> getText() {
    return switch (this) {
      case Text t -> Optional.ofNullable(t.text()).filter(s -> !s.isEmpty());
      case MultiModal m -> m.text().filter(s -> !s.isEmpty());
    };
  }
}
