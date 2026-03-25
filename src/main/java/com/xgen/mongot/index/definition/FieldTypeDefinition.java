package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public sealed interface FieldTypeDefinition extends DocumentEncodable
    permits AutocompleteFieldDefinition,
        BooleanFieldDefinition,
        DateFieldDefinition,
        DateFacetFieldDefinition,
        DocumentFieldDefinition,
        EmbeddedDocumentsFieldDefinition,
        GeoFieldDefinition,
        KnnVectorFieldDefinition,
        NumberFieldDefinition,
        NumberFacetFieldDefinition,
        ObjectIdFieldDefinition,
        SearchAutoEmbedFieldDefinition,
        SearchIndexVectorFieldDefinition,
        SortableDateBetaV1FieldDefinition,
        SortableNumberBetaV1FieldDefinition,
        SortableStringBetaV1FieldDefinition,
        StringFieldDefinition,
        StringFacetFieldDefinition,
        TokenFieldDefinition,
        UuidFieldDefinition {

  class Fields {
    static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  enum Type {
    AUTOCOMPLETE,
    AUTO_EMBED_VECTOR,
    BOOLEAN,
    DATE,
    @Deprecated
    DATE_FACET,
    DOCUMENT,
    EMBEDDED_DOCUMENTS,
    GEO,
    VECTOR,
    KNN_VECTOR,
    NUMBER,
    @Deprecated
    NUMBER_FACET,
    OBJECT_ID,
    @Deprecated
    SORTABLE_DATE_BETA_V1,
    @Deprecated
    SORTABLE_NUMBER_BETA_V1,
    @Deprecated
    SORTABLE_STRING_BETA_V1,
    STRING,
    @Deprecated
    STRING_FACET,
    TOKEN,
    UUID
  }

  Type getType();

  /**
   * Concrete classes should implement this instead of overriding FieldTypeDefinition::toBson.
   * FieldTypeDefinition::toBson will add the proper type field for the concrete type to a
   * BsonDocument, then delegate to fieldTypeToBson to encode the field definition document.
   */
  BsonDocument fieldTypeToBson();

  /**
   * The equals() and hashCode() methods must be implemented in all classes that extend
   * FieldTypeDefinition as they are used when comparing IndexDefinitions to determine whether to
   * stage a new index build. Both the equals() and hashCode() methods must incorporate all the
   * instance variables present in the class.
   */
  @Override
  boolean equals(Object o);

  @Override
  int hashCode();

  static FieldTypeDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case AUTOCOMPLETE -> AutocompleteFieldDefinition.fromBson(parser);
      case AUTO_EMBED_VECTOR -> SearchAutoEmbedFieldDefinition.fromBson(parser);
      case BOOLEAN -> BooleanFieldDefinition.fromBson(parser);
      case DATE -> DateFieldDefinition.fromBson(parser);
      case DATE_FACET -> DateFacetFieldDefinition.fromBson(parser);
      case DOCUMENT -> DocumentFieldDefinition.fromBson(parser);
      case EMBEDDED_DOCUMENTS -> EmbeddedDocumentsFieldDefinition.fromBson(parser);
      case GEO -> GeoFieldDefinition.fromBson(parser);
      case VECTOR -> SearchIndexVectorFieldDefinition.fromBson(parser);
      case KNN_VECTOR -> KnnVectorFieldDefinition.fromBson(parser);
      case NUMBER -> NumberFieldDefinition.fromBson(parser);
      case NUMBER_FACET -> NumberFacetFieldDefinition.fromBson(parser);
      case OBJECT_ID -> ObjectIdFieldDefinition.fromBson(parser);
      case SORTABLE_DATE_BETA_V1 -> SortableDateBetaV1FieldDefinition.fromBson(parser);
      case SORTABLE_NUMBER_BETA_V1 -> SortableNumberBetaV1FieldDefinition.fromBson(parser);
      case SORTABLE_STRING_BETA_V1 -> SortableStringBetaV1FieldDefinition.fromBson(parser);
      case STRING -> StringFieldDefinition.fromBson(parser);
      case STRING_FACET -> StringFacetFieldDefinition.fromBson(parser);
      case TOKEN -> TokenFieldDefinition.fromBson(parser);
      case UUID -> UuidFieldDefinition.fromBson(parser);
    };
  }

  @Override
  default BsonDocument toBson() {
    BsonDocument doc = BsonDocumentBuilder.builder().field(Fields.TYPE, getType()).build();
    doc.putAll(fieldTypeToBson());
    return doc;
  }
}
