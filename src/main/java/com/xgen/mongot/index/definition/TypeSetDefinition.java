package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.BsonDocument;

public record TypeSetDefinition(String name, List<FieldTypeDefinition> types)
    implements DocumentEncodable {
  static class Fields {
    static final Field.Required<String> NAME =
        Field.builder("name").stringField().mustNotBeBlank().required();

    static final Field.Required<List<FieldTypeDefinition>> TYPES =
        Field.builder("types")
            .listOf(
                Value.builder()
                    .classValue(FieldTypeDefinition::fromBson)
                    .disallowUnknownFields()
                    .required())
            .mustHaveUniqueAttribute("type", FieldTypeDefinition::getType)
            .validate(TypeSetDefinition::ensureValidFieldTypes)
            .mustNotBeEmpty()
            .required();
  }

  public static TypeSetDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new TypeSetDefinition(
        parser.getField(Fields.NAME).unwrap(), parser.getField(Fields.TYPES).unwrap());
  }

  /**
   * Returns a constructed {@link FieldDefinition} based on the <code>types</code> present plus an
   * implicit {@link DocumentFieldDefinition} configured with a reference to the current <code>
   * typeSet</code>.
   */
  public FieldDefinition getFieldDefinition() {
    return FieldDefinition.fromValidatedFieldTypeDefinitions(
        Stream.concat(
                this.types.stream(),
                Stream.of(
                    FieldDefinition.createOrAssert(
                        new DynamicDefinition.Document(this.name), Collections.emptyMap())))
            .toList());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.TYPES, this.types)
        .build();
  }

  /**
   * Field types that a type set may not contain. Single source of truth for the type-set
   * restriction.
   */
  public static final Set<FieldTypeDefinition.Type> INVALID_TYPES =
      Set.of(
          FieldTypeDefinition.Type.AUTO_EMBED,
          FieldTypeDefinition.Type.DATE_FACET,
          FieldTypeDefinition.Type.DOCUMENT,
          FieldTypeDefinition.Type.EMBEDDED_DOCUMENTS,
          FieldTypeDefinition.Type.VECTOR,
          FieldTypeDefinition.Type.KNN_VECTOR,
          FieldTypeDefinition.Type.NUMBER_FACET,
          FieldTypeDefinition.Type.SORTABLE_DATE_BETA_V1,
          FieldTypeDefinition.Type.SORTABLE_NUMBER_BETA_V1,
          FieldTypeDefinition.Type.SORTABLE_STRING_BETA_V1,
          FieldTypeDefinition.Type.STRING_FACET);

  static Optional<String> ensureValidFieldTypes(List<FieldTypeDefinition> typeDefinitions) {
    Set<FieldTypeDefinition.Type> invalidFieldTypes = getInvalidFieldTypes();
    return typeDefinitions.stream()
            .map(FieldTypeDefinition::getType)
            .anyMatch(invalidFieldTypes::contains)
        ? Optional.of(
            String.format(
                "The following field type definitions are disallowed in a typeSet definition: %s",
                invalidFieldTypes))
        : Optional.empty();
  }

  private static Set<FieldTypeDefinition.Type> getInvalidFieldTypes() {
    return INVALID_TYPES;
  }
}
