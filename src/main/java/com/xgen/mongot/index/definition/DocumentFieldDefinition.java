package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * DocumentFieldDefinition defines how mongot should index a field if its value in a document is an
 * object.
 */
public record DocumentFieldDefinition(
    DynamicDefinition dynamic,
    ImmutableMap<String, FieldDefinition> fields,
    FieldHierarchyContext fieldHierarchyContext)
    implements FieldTypeDefinition, HierarchicalFieldDefinition {

  public static class Fields {
    public static final Field.WithDefault<DynamicDefinition> DYNAMIC =
        Field.builder("dynamic")
            .classField(DynamicDefinition::fromBson)
            .optional()
            .withDefault(DynamicDefinition.DISABLED);

    static final Field.WithDefault<Map<String, FieldDefinition>> FIELDS =
        Field.builder("fields")
            .classField(FieldDefinition::fromBson)
            .asMap()
            .optional()
            .withDefault(Collections.emptyMap());
  }

  public DocumentFieldDefinition(
      DynamicDefinition dynamic,
      Map<String, FieldDefinition> fields,
      FieldHierarchyContext fieldHierarchyContext) {
    this(dynamic, ImmutableMap.copyOf(fields), fieldHierarchyContext);
  }

  /**
   * Method for instantiating a {@link DocumentFieldDefinition} from dynamic and fields. Assumes
   * {@code fields} has no two {@link EmbeddedDocumentsFieldDefinition} fields defined for any given
   * path.
   *
   * <p>Throws a {@link IllegalEmbeddedFieldException} if {@code fields} are not legally specified
   * as described here.
   */
  public static DocumentFieldDefinition create(
      DynamicDefinition dynamic, Map<String, FieldDefinition> fields)
      throws IllegalEmbeddedFieldException {
    return new DocumentFieldDefinition(
        dynamic, fields, FieldHierarchyContext.createForDocumentsField(fields));
  }

  public static DocumentFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    try {
      return create(
          parser.getField(Fields.DYNAMIC).unwrap(), parser.getField(Fields.FIELDS).unwrap());
    } catch (IllegalEmbeddedFieldException e) {
      return parser.getContext().handleSemanticError(e.getMessage());
    }
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DYNAMIC, this.dynamic)
        .field(Fields.FIELDS, this.fields)
        .build();
  }

  @Override
  public Type getType() {
    return Type.DOCUMENT;
  }

  /**
   * Returns the FieldDefinition for the field if it exists, or empty if the field is not in the
   * definition.
   */
  @Override
  public Optional<FieldDefinition> getField(String fieldName) {
    return Optional.ofNullable(this.fields.get(fieldName));
  }

  /**
   * True if this document declares an auto-embed field anywhere in its {@code document} subtree.
   * {@code embeddedDocuments} subtrees are not searched: auto-embed fields inside them are rejected
   * at validation.
   */
  public boolean containsSearchAutoEmbedField() {
    return this.fields.values().stream()
        .anyMatch(
            fieldDef ->
                fieldDef.searchAutoEmbedFieldDefinition().isPresent()
                    || fieldDef
                        .documentFieldDefinition()
                        .map(DocumentFieldDefinition::containsSearchAutoEmbedField)
                        .orElse(false));
  }
}
