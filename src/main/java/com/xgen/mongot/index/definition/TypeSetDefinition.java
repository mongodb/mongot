package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
    return Arrays.stream(FieldTypeDefinition.Type.values())
        .filter(
            type ->
                switch (type) {
                  case AUTO_EMBED_VECTOR,
                          DATE_FACET,
                          DOCUMENT,
                          EMBEDDED_DOCUMENTS,
                          VECTOR,
                          KNN_VECTOR,
                          NUMBER_FACET,
                          SORTABLE_DATE_BETA_V1,
                          SORTABLE_NUMBER_BETA_V1,
                          SORTABLE_STRING_BETA_V1,
                          STRING_FACET ->
                      true;

                  case AUTOCOMPLETE, BOOLEAN, DATE, GEO, NUMBER, OBJECT_ID, STRING, TOKEN, UUID ->
                      false;
                })
        .collect(Collectors.toSet());
  }
}
