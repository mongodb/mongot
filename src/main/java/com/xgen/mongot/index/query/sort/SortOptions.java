package com.xgen.mongot.index.query.sort;

import static com.xgen.mongot.index.query.sort.UserFieldSortOptions.DEFAULT_ASC;
import static com.xgen.mongot.index.query.sort.UserFieldSortOptions.DEFAULT_DESC;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * This class encapsulates any internal options that may be configured for a Lucene sort on a
 * particular field. Some of these options may be configurable by the user while others may be
 * hardcoded in this class.
 */
public sealed interface SortOptions extends DocumentEncodable
    permits MetaSortOptions, UserFieldSortOptions {

  class Fields {
    static final Field.Optional<MetaSortField> META =
        Field.builder("$meta").enumField(MetaSortField.class).asCamelCase().optional().noDefault();

    static final Field.Optional<SortOrder> ORDER =
        Field.builder("order")
            .classField(SortOrder::fromBson)
            .optional()
            .noDefault(); // Default depends on $meta's value

    static final Field.Optional<NullEmptySortPosition> NO_DATA =
        Field.builder("noData")
            .enumField(NullEmptySortPosition.class)
            .asCamelCase()
            .optional()
            .noDefault();

    static final Field.Optional<EmbeddedSortScope> EMBEDDED_DOCUMENT =
        Field.builder("embeddedDocument")
            .classField(EmbeddedSortScope::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  SortOptions invert();

  /** Returns the direction of the sort. */
  SortOrder order();

  /**
   * Returns true if the order is the reverse of the natural ordering. Shorthand for {@code
   * this.getOrder().isReverse()}
   */
  default boolean isReverse() {
    return this.order().isReverse();
  }

  /** Strategy that specifies how to select a representative element from an array field. */
  SortSelector selector();

  static SortOptions fromBson(BsonParseContext context, BsonValue value) throws BsonParseException {
    if (value.isNumber()) {
      return switch (SortOrder.fromBson(context, value)) {
        case ASC -> DEFAULT_ASC;
        case DESC -> DEFAULT_DESC;
      };
    }

    if (!value.isDocument()) {
      return context.handleUnexpectedType("Expected number or document", value.getBsonType());
    }

    return parseDocumentSortOptions(context, value.asDocument());
  }

  private static SortOptions parseDocumentSortOptions(
      BsonParseContext context, BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.withContext(context, document).build()) {
      Optional<SortOrder> sortOrder = parser.getField(Fields.ORDER).unwrap();

      var wrappedMeta = parser.getField(Fields.META);
      Optional<MetaSortField> meta = wrappedMeta.unwrap();

      // Sort order is required if it's not a meta sort
      if (sortOrder.isEmpty() && meta.isEmpty()) {
        context.handleSemanticError("\"order\" is required");
      }

      var wrappedNoData = parser.getField(Fields.NO_DATA);
      var wrappedEmbeddedDocument = parser.getField(Fields.EMBEDDED_DOCUMENT);

      parser.getGroup().atMostOneOf(wrappedMeta, wrappedNoData);
      // An embedded-document scope is a user-field concept; it cannot combine with a $meta sort.
      parser.getGroup().atMostOneOf(wrappedMeta, wrappedEmbeddedDocument);

      if (meta.isPresent()) {
        return new MetaSortOptions(sortOrder.orElse(meta.get().getDefaultSortOrder()), meta.get());
      }

      SortOrder order = sortOrder.get();
      return new UserFieldSortOptions(
          order,
          order == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX,
          wrappedNoData.unwrap().orElse(NullEmptySortPosition.LOWEST),
          wrappedEmbeddedDocument.unwrap());
    }
  }
}
