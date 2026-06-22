package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public class LuceneIndexedBinaryField extends LuceneIndexedFieldSpec {

  static class Fields {
    static final Field.Required<String> VALUE = Field.builder("binary").stringField().required();

    static final Field.WithDefault<Boolean> STORED =
        Field.builder("stored").booleanField().optional().withDefault(true);

    static final Field.WithDefault<Boolean> OMIT_NORMS =
        Field.builder("omitNorms").booleanField().optional().withDefault(false);
    static final Field.WithDefault<Boolean> TOKENIZED =
        Field.builder("tokenized").booleanField().optional().withDefault(true);
    static final Field.WithDefault<DocValuesType> DOC_VALUES_TYPE =
        Field.builder("docValuesType")
            .enumField(DocValuesType.class)
            .asCamelCase()
            .optional()
            .withDefault(DocValuesType.NONE);

    static final Field.WithDefault<IndexOptions> INDEX_OPTIONS =
        Field.builder("indexOptions")
            .enumField(IndexOptions.class)
            .asCamelCase()
            .optional()
            .withDefault(IndexOptions.NONE);
  }

  private final String value;
  private final boolean stored;
  private final boolean omitNorms;
  private final boolean tokenized;
  private final DocValuesType docValuesType;
  private final IndexOptions indexOptions;

  public LuceneIndexedBinaryField(
      String value,
      boolean stored,
      boolean omitNorms,
      boolean tokenized,
      DocValuesType docValuesType,
      IndexOptions indexOptions) {
    this.value = value;
    this.stored = stored;
    this.omitNorms = omitNorms;
    this.tokenized = tokenized;
    this.docValuesType = docValuesType;
    this.indexOptions = indexOptions;
  }

  static LuceneIndexedBinaryField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedBinaryField(
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.STORED).unwrap(),
        parser.getField(Fields.OMIT_NORMS).unwrap(),
        parser.getField(Fields.TOKENIZED).unwrap(),
        parser.getField(Fields.DOC_VALUES_TYPE).unwrap(),
        parser.getField(Fields.INDEX_OPTIONS).unwrap());
  }

  private static String render(TypeField type, BytesRef value) {
    return switch (type) {
      case UUID -> UUID.fromString(value.utf8ToString()).toString();
      case OBJECT_ID -> new ObjectId(value.bytes).toString();
      case BOOLEAN, TOKEN, STRING, AUTOCOMPLETE, NULL -> value.utf8ToString();
      default -> base64(value);
    };
  }

  private static String base64(BytesRef value) {
    return Base64.getEncoder().withoutPadding().encodeToString(value.bytes);
  }

  public static LuceneIndexedFieldSpec fromField(IndexableField field) {
    BytesRef bytesRef = field.binaryValue();
    var fieldType = field.fieldType();
    String stringValue =
        TypeField.getTypeOf(field.name())
            .map(t -> render(t, bytesRef))
            .orElseGet(() -> base64(bytesRef));

    return new LuceneIndexedBinaryField(
        stringValue,
        fieldType.stored(),
        fieldType.omitNorms(),
        fieldType.tokenized(),
        fieldType.docValuesType(),
        fieldType.indexOptions());
  }

  @Override
  BsonDocument fieldToBson() {
    var builder =
        BsonDocumentBuilder.builder()
            .field(Fields.VALUE, this.value)
            .field(Fields.STORED, this.stored)
            .field(Fields.INDEX_OPTIONS, this.indexOptions);

    // Tokenization and norms are irrelevant if not indexed
    if (this.indexOptions != IndexOptions.NONE) {
      builder.field(Fields.OMIT_NORMS, this.omitNorms).field(Fields.TOKENIZED, this.tokenized);
    }

    return builder.fieldOmitDefaultValue(Fields.DOC_VALUES_TYPE, this.docValuesType).build();
  }

  @Override
  Type getType() {
    return Type.BINARY;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof LuceneIndexedBinaryField that)) {
      return false;
    }

    if (this.indexOptions != IndexOptions.NONE) {
      if (this.omitNorms != that.omitNorms || this.tokenized != that.tokenized) {
        return false;
      }
    }

    return this.stored == that.stored
        && this.value.equals(that.value)
        && this.docValuesType == that.docValuesType
        && this.indexOptions == that.indexOptions;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value, this.stored, this.docValuesType, this.indexOptions);
  }
}
