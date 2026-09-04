package com.xgen.mongot.index.definition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.BsonDocument;

public record EmbeddedDocumentsFieldDefinition(
    DynamicDefinition dynamic,
    ImmutableMap<String, FieldDefinition> fields,
    Optional<StoredSourceDefinition> storedSourceDefinition,
    FieldHierarchyContext fieldHierarchyContext)
    implements FieldTypeDefinition, HierarchicalFieldDefinition {

  /**
   * {@link Type}s that are not allowed to be specified as child fields of an {@link
   * EmbeddedDocumentsFieldDefinition}.
   */
  private static final EnumSet<Type> ILLEGAL_SUBFIELD_TYPES =
      EnumSet.of(
          Type.SORTABLE_DATE_BETA_V1,
          Type.SORTABLE_NUMBER_BETA_V1,
          Type.SORTABLE_STRING_BETA_V1,
          Type.KNN_VECTOR,
          Type.AUTO_EMBED);

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

    static final Field.Optional<StoredSourceDefinition> STORED_SOURCE =
        Field.builder("storedSource")
            .classField(StoredSourceDefinition::fromBson)
            .optional()
            .noDefault();
  }

  public EmbeddedDocumentsFieldDefinition(
      DynamicDefinition dynamic,
      Map<String, FieldDefinition> fields,
      Optional<StoredSourceDefinition> storedSourceDefinition,
      FieldHierarchyContext fieldHierarchyContext) {
    this(dynamic, ImmutableMap.copyOf(fields), storedSourceDefinition, fieldHierarchyContext);
  }

  /**
   * Instantiating an {@link EmbeddedDocumentsFieldDefinition} from dynamic and fields. Assumes
   * {@code fields} has no two {@link EmbeddedDocumentsFieldDefinition} fields defined for any given
   * path, and that all child {@link FieldTypeDefinition}s are allowed to be specified in an {@link
   * EmbeddedDocumentsFieldDefinition} (no child fields are a type marked as illegal in {@link
   * EmbeddedDocumentsFieldDefinition#ILLEGAL_SUBFIELD_TYPES}.
   *
   * <p>Throws a {@link IllegalEmbeddedFieldException} if {@code fields} are not legally specified
   * as described here.
   */
  public static EmbeddedDocumentsFieldDefinition create(
      DynamicDefinition dynamic,
      Map<String, FieldDefinition> fields,
      Optional<StoredSourceDefinition> storedSourceDefinition)
      throws IllegalEmbeddedFieldException {
    Set<Type> illegalTypes =
        fields.values().stream()
            .map(EmbeddedDocumentsFieldDefinition::getIllegalTypes)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    if (!illegalTypes.isEmpty()) {
      throw IllegalEmbeddedFieldException.withIllegalFieldTypes(illegalTypes);
    }

    return new EmbeddedDocumentsFieldDefinition(
        dynamic,
        fields,
        storedSourceDefinition,
        FieldHierarchyContext.createForEmbeddedDocumentsField(fields));
  }

  @VisibleForTesting
  static Set<Type> getIllegalTypes(FieldDefinition fieldDefinition) {
    Set<Type> illegalTypes = EnumSet.noneOf(Type.class);

    for (FieldTypeDefinition fieldTypeDefinition :
        fieldDefinition
            .getAllDefinitions()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList()) {

      if (ILLEGAL_SUBFIELD_TYPES.contains(fieldTypeDefinition.getType())) {
        illegalTypes.add(fieldTypeDefinition.getType());
      }

      // Only need to descend into document-type fields, because any embeddedDocuments fields
      // have already checked that their children are the correct types.
      if (fieldTypeDefinition instanceof DocumentFieldDefinition documentFieldDefinition) {
        documentFieldDefinition.fields().values().stream()
            .map(EmbeddedDocumentsFieldDefinition::getIllegalTypes)
            .forEach(illegalTypes::addAll);
      }
    }

    return illegalTypes;
  }

  public static EmbeddedDocumentsFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    try {
      return create(
          parser.getField(Fields.DYNAMIC).unwrap(),
          parser.getField(Fields.FIELDS).unwrap(),
          parser.getField(Fields.STORED_SOURCE).unwrap());
    } catch (IllegalEmbeddedFieldException e) {
      return parser.getContext().handleSemanticError(e.getMessage());
    }
  }

  @Override
  public Type getType() {
    return Type.EMBEDDED_DOCUMENTS;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DYNAMIC, this.dynamic)
        .field(Fields.FIELDS, this.fields)
        .field(Fields.STORED_SOURCE, this.storedSourceDefinition)
        .build();
  }

  @Override
  public Optional<FieldDefinition> getField(String fieldName) {
    return Optional.ofNullable(this.fields.get(fieldName));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof EmbeddedDocumentsFieldDefinition that)) {
      return false;
    }

    return Objects.equals(this.dynamic, that.dynamic)
        && Objects.equals(this.fieldHierarchyContext, that.fieldHierarchyContext)
        && Objects.equals(this.fields, that.fields)
        && Objects.equals(this.storedSourceDefinition, that.storedSourceDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.dynamic, this.fields, this.fieldHierarchyContext, this.storedSourceDefinition);
  }
}
