package com.xgen.proto.plugin;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import static com.xgen.proto.plugin.CodeGeneratorUtils.generateReadOneValue;
import static com.xgen.proto.plugin.CodeGeneratorUtils.generateWriteOneValue;
import static com.xgen.proto.plugin.CodeGeneratorUtils.getFieldOptions;
import static com.xgen.proto.plugin.CodeGeneratorUtils.getJavaFullName;
import static com.xgen.proto.plugin.CodeGeneratorUtils.getMessageType;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.protobuf.Descriptors.OneofDescriptor;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonType;
import org.bson.protobuf.MessageType;

/**
 * Generate code to code an arbitrary protobuf message into BSON.
 *
 * <p>Almost all generated message code runs through this class; only well-known types that map
 * directly to BSON types use another implementation.
 *
 * <p>This allows almost all message forms in proto3 syntax with the following exceptions:
 *
 * <ul>
 *   <li>repeated ProtoArray is not allowed because it is difficult to parse.
 *   <li>map fields are unimplemented. They may be implemented in whole or in part in the future.
 * </ul>
 */
public class GenericMessageCodeGenerator implements CodeGenerator {
  private static final Converter<String, String> FIELD_NAME_CONVERTER =
      CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.UPPER_CAMEL);

  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();
  private final Optional<OneofDescriptor> discriminatedUnion;

  /**
   * CodeGenerator that produces output for a single message type.
   *
   * @throws IllegalArgumentException if we cannot generate sound code for this message because it
   *     contains "repeated ProtoArray" or has a map with a non-string type.
   */
  public GenericMessageCodeGenerator(Descriptor messageDescriptor) {
    this.descriptor = messageDescriptor;

    if (getMessageType(this.descriptor) == MessageType.DISCRIMINATED_UNION) {
      var nonSyntheticOneofs =
          this.descriptor.getOneofs().stream()
              .filter(o -> o.getFields().size() != 1 || !o.getField(0).isOptional())
              .collect(Collectors.toList());
      if (nonSyntheticOneofs.size() != 1) {
        throw new IllegalArgumentException(
            String.format(
                "Expected exactly one oneof field for discriminated union message %s; got %d",
                this.descriptor.getFullName(), nonSyntheticOneofs.size()));
      }
      OneofDescriptor unionType = nonSyntheticOneofs.get(0);
      validateDiscriminatedUnion(unionType);
      this.discriminatedUnion = Optional.of(unionType);
    } else {
      this.discriminatedUnion = Optional.empty();
    }

    for (var field : this.descriptor.getFields()) {
      validateField(field);
    }
  }

  private void validateField(FieldDescriptor field) {
    if (getFieldOptions(field).getAllowSingleValue()) {
      if (!field.isRepeated()) {
        throw new IllegalArgumentException(
            String.format(
                "allow_single_value may not be set on %s as the field is not repeated.",
                field.getFullName()));
      }

      if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          && GenericValueMessageCodeGenerator.getMessageValueTypes(field.getMessageType())
              .contains(BsonType.ARRAY)) {
        throw new IllegalArgumentException(
            String.format(
                "allow_single_value may not be set on %s as the value type may contain another"
                    + " array.",
                field.getFullName()));
      }
    }

    if (field.isMapField()
        && field.getMessageType().findFieldByName("key").getJavaType()
            != FieldDescriptor.JavaType.STRING) {
      throw new IllegalArgumentException("map fields only support string keys");
    }

    if (getFieldOptions(field).getAllowUnknownFields()) {
      if (field.isMapField() && !isDocumentField(field.getMessageType().findFieldByName("value"))) {
        throw new IllegalArgumentException(
            String.format(
                "allow_unknown_fields may not be set on %s as this only applies to maps with a"
                    + " document value",
                field.getFullName()));
      } else if (!isDocumentField(field)) {
        throw new IllegalArgumentException(
            String.format(
                "allow_unknown_fields may not be set on %s as this only applies to document"
                    + " fields.",
                field.getFullName()));
      }
    }
  }

  private static boolean isDocumentField(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.MESSAGE
        && GenericValueMessageCodeGenerator.getMessageValueTypes(field.getMessageType())
            .contains(BsonType.DOCUMENT);
  }

  private void validateDiscriminatedUnion(OneofDescriptor unionType) {
    // Collect the names all of non-union fields in the message along with the discriminator name.
    // We use this to ensure that none of the union messages contain fields with the same name.
    // None of the variant messages are allowed to be discriminated unions as that would require us
    // to recursively flatten the message in a way that is difficult to understand.
    Set<String> baseFieldNames =
        Stream.concat(
                Stream.of(unionType.getName()),
                this.descriptor.getFields().stream()
                    .filter(f -> f.getContainingOneof() != null)
                    .map(f -> f.getName()))
            .collect(toImmutableSet());
    for (var field : unionType.getFields()) {
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s in discrimination union %s is not a message-typed field.",
                field.getName(), unionType.getFullName()));
      }
      Descriptor messageType = field.getMessageType();
      if (CodeGeneratorUtils.getMessageType(messageType) == MessageType.DISCRIMINATED_UNION) {
        throw new IllegalArgumentException(
            String.format(
                "Discriminated union field %s of %s is also a discriminated union (%s); this is"
                    + " disallowed.",
                field.getName(), unionType.getFullName(), messageType.getFullName()));
      }
      String overlappingFields =
          messageType.getFields().stream()
              .filter(f -> baseFieldNames.contains(f.getName()))
              .map(FieldDescriptor::getName)
              .collect(Collectors.joining(","));
      if (!overlappingFields.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Discrimination union field %s of %s has field(s) of the same name in the base"
                    + " message and variant (%s)",
                field.getName(), unionType.getFullName(), overlappingFields));
      }
    }
  }

  @Override
  public Iterable<CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      generateWriteBsonTo(writeBsonToScope);
    }

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReader(this.descriptor, this.builderOutput)) {
      generateMergeBsonFrom(mergeBsonFromScope);
    }

    this.discriminatedUnion.ifPresent(this::generateDiscriminatedUnionHelpers);

    return CodeGeneratorUtils.messageFiles(this.descriptor, this.messageOutput, this.builderOutput);
  }

  /**
   * Generate the contents of the writeBsonTo(BsonWriter) method.
   *
   * <p>Assumes a BsonWriter variable named "writer" is in scope.
   *
   * @param fnScope open scope for the writeBsonTo() method.
   */
  private void generateWriteBsonTo(CodeOutput.Scope fnScope) {
    fnScope.writeLine("writer.writeStartDocument();");
    if (this.discriminatedUnion.isPresent()) {
      // Write the type at the beginning of the stream if any case is set.
      var fieldNamePart = FIELD_NAME_CONVERTER.convert(this.discriminatedUnion.get().getName());
      var unionFieldName =
          CaseFormat.LOWER_UNDERSCORE
              .converterTo(CaseFormat.LOWER_CAMEL)
              .convert(this.discriminatedUnion.get().getName());
      fnScope.writeLine(
          "get%sCaseBsonString(get%sCase()).ifPresent(s -> writer.writeString(\"%s\", s));",
          fieldNamePart, fieldNamePart, unionFieldName);
    }

    for (FieldDescriptor field : this.descriptor.getFields()) {
      fnScope.writeLine("// " + field.getName());
      String fieldNamePart = FIELD_NAME_CONVERTER.convert(field.getName());
      if (field.isMapField()) {
        generateSerializeMapField(field, fieldNamePart, fnScope);
      } else if (field.isRepeated()) {
        generateSerializeRepeatedField(field, fieldNamePart, fnScope);
      } else {
        generateSerializeScalarField(field, fieldNamePart, fnScope);
      }
      fnScope.writeLine("");
    }
    fnScope.writeLine("writer.writeEndDocument();");
  }

  /**
   * Generate code to serialize a scalar (non-repeated non-map) field.
   *
   * <p>Assumes a BsonWriter variable named "writer" is in scope.
   *
   * @param field the field to generate serialization code for.
   * @param fieldNamePart UpperCamelCase name of the field to serialize.
   * @param writeToScope scope to write the serialization code into.
   */
  private void generateSerializeScalarField(
      FieldDescriptor field, String fieldNamePart, CodeOutput.Scope writeToScope) {
    try (var fieldScope =
        field.hasPresence() || field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
            ? writeToScope.openScope("if (has%s())", fieldNamePart)
            : writeToScope.openScope()) {
      fieldScope.writeLine("var value = get%s();", fieldNamePart);
      if (this.discriminatedUnion.isPresent() && field.getRealContainingOneof() != null) {
        // For discriminated union inject a writer that will flatten the variant sub-messaged into
        // the current document. This only needs to be done here as oneof fields may not be repeated
        // or map fields.
        fieldScope.writeLine(
            "value.writeBsonTo(new com.xgen.proto.DiscriminatedUnionVariantBsonWriter(writer));");
      } else {
        fieldScope.writeLine("writer.writeName(\"%s\");", field.getJsonName());
        generateWriteOneValue(field, fieldScope);
      }
    }
  }

  /**
   * Generate code to serialize a repeated (non-map) field.
   *
   * <p>Assumes a BsonWriter variable named "writer" is in scope.
   *
   * @param field the field to generate serialization code for.
   * @param fieldNamePart UpperCamelCase name of the field to serialize.
   * @param writeToScope scope to write the serialization code into.
   */
  private static void generateSerializeRepeatedField(
      FieldDescriptor field, String fieldNamePart, CodeOutput.Scope writeToScope) {
    try (var fieldScope = writeToScope.openScope("if (get%sCount() > 0)", fieldNamePart)) {
      fieldScope.writeLine("writer.writeName(\"%s\");", field.getJsonName());
      if (getFieldOptions(field).getAllowSingleValue()) {
        try (var singleScope = fieldScope.openScope("if (get%sCount() == 1)", fieldNamePart)) {
          singleScope.writeLine("var value = get%s(0);", fieldNamePart);
          generateWriteOneValue(field, singleScope);
        }
        try (var arrayScope = fieldScope.openScope("else")) {
          generateSerializeArray(field, fieldNamePart, arrayScope);
        }
      } else {
        generateSerializeArray(field, fieldNamePart, fieldScope);
      }
    }
  }

  private static void generateSerializeArray(
      FieldDescriptor field, String fieldNamePart, CodeOutput.Scope scope) {
    scope.writeLine("writer.writeStartArray();");
    try (var valueLoopScope = scope.openScope("for (var value : get%sList())", fieldNamePart)) {
      generateWriteOneValue(field, valueLoopScope);
    }
    scope.writeLine("writer.writeEndArray();");
  }

  /**
   * Generate code to serialize a map field.
   *
   * <p>Assumes a BsonWriter variable named "writer" is in scope.
   *
   * @param fieldDescriptor the field to generate serialization code for.
   * @param fieldNamePart UpperCamelCase name of the field to serialize.
   * @param writeToScope scope to write the serialization code into.
   */
  private static void generateSerializeMapField(
      FieldDescriptor fieldDescriptor, String fieldNamePart, CodeOutput.Scope writeToScope) {
    FieldDescriptor valueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
    try (var fieldScope = writeToScope.openScope("if (get%sCount() > 0)", fieldNamePart)) {
      fieldScope.writeLine("writer.writeName(\"%s\");", fieldDescriptor.getJsonName());
      fieldScope.writeLine("writer.writeStartDocument();");
      try (var entryLoopScope =
          fieldScope.openScope("for (var entry : get%sMap().entrySet())", fieldNamePart)) {
        entryLoopScope.writeLine("writer.writeName(entry.getKey());");
        entryLoopScope.writeLine("var value = entry.getValue();");
        generateWriteOneValue(valueDescriptor, entryLoopScope);
      }
      fieldScope.writeLine("writer.writeEndDocument();");
    }
  }

  /**
   * Generate code to implement mergeBsonFrom(BsonReader reader, boolean allowUnknownFields).
   *
   * @param fnScope scope of the mergeBsonFrom() method.
   */
  private void generateMergeBsonFrom(CodeOutput.Scope fnScope) {
    if (this.discriminatedUnion.isEmpty()) {
      generateRegularMergeBsonFrom(fnScope);
    } else {
      generateDiscriminatedUnionMergeBsonFrom(this.discriminatedUnion.get(), fnScope);
    }
    fnScope.writeLine("return this;");
  }

  private void generateRegularMergeBsonFrom(CodeOutput.Scope fnScope) {
    fnScope.writeLine("reader.readStartDocument();");
    try (var decodeLoopScope =
        fnScope.openScope("while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
      try (var fieldSwitchScope = decodeLoopScope.openScope("switch (reader.readName())")) {
        for (FieldDescriptor fieldDescriptor : this.descriptor.getFields()) {
          generateMergeField(fieldDescriptor, fieldSwitchScope);
        }
        try (var defaultScope = fieldSwitchScope.openScope("default:")) {
          try (var allowUnknownFieldsScope = defaultScope.openScope("if (allowUnknownFields)")) {
            allowUnknownFieldsScope.writeLine("reader.skipValue();");
          }
          try (var disallowUnknownFieldsScope = defaultScope.openScope("else")) {
            disallowUnknownFieldsScope.writeLine(
                "throw new com.xgen.proto.BsonProtoParseException(\"Unknown field named \" +"
                    + " reader.getCurrentName());");
          }
        }
      }
    }
    fnScope.writeLine("reader.readEndDocument();");
  }

  private void generateDiscriminatedUnionMergeBsonFrom(
      OneofDescriptor unionType, CodeOutput.Scope fnScope) {
    // Parsing a discriminated union is a two-pass process:
    // 1) Mark the current position.
    // 2) Parse the document and merge any fields for the current message.
    // 3) Return to the mark.
    // 4) Parse the variant message from the current document (if necessary).
    fnScope.writeLine("var mark = reader.getMark();");
    fnScope.writeLine("reader.readStartDocument();");
    fnScope.writeLine("java.util.Optional<String> unknownFieldName = java.util.Optional.empty();");
    fnScope.writeLine("java.util.Optional<String> discriminantName = java.util.Optional.empty();");
    try (var decodeLoopScope =
        fnScope.openScope("while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
      try (var fieldSwitchScope = decodeLoopScope.openScope("switch (reader.readName())")) {
        String discriminantName =
            CaseFormat.LOWER_UNDERSCORE
                .converterTo(CaseFormat.LOWER_CAMEL)
                .convert(unionType.getName());
        try (var discriminantCaseScope =
            fieldSwitchScope.openScope("case \"%s\":", discriminantName)) {
          try (var ifScope =
              discriminantCaseScope.openScope(
                  "if (reader.getCurrentBsonType() != org.bson.BsonType.STRING)")) {
            ifScope.writeLine(
                "throw"
                    + " com.xgen.proto.BsonProtoParseException.createTypeMismatchException(org.bson.BsonType.STRING,"
                    + " reader.getCurrentBsonType(), \"%s.%s\");",
                this.descriptor.getFullName(), discriminantName);
          }
          discriminantCaseScope.writeLine(
              "discriminantName = java.util.Optional.of(reader.readString());");
          discriminantCaseScope.writeLine("break;");
        }
        // Only parse non-variant fields; treat anything else as an unknown field.
        this.descriptor.getFields().stream()
            .filter(f -> f.getRealContainingOneof() == null)
            .forEach(f -> generateMergeField(f, fieldSwitchScope));
        try (var defaultScope = fieldSwitchScope.openScope("default:")) {
          // Record the name of the last seen unknown field and skip the value.
          defaultScope.writeLine(
              "unknownFieldName = java.util.Optional.of(reader.getCurrentName());");
          defaultScope.writeLine("reader.skipValue();");
        }
      }
    }

    // If we've seen unknown fields and there is no variant message, apply the unknown fields policy
    // for the current field.
    try (var noDiscriminantScope = fnScope.openScope("if (discriminantName.isEmpty())")) {
      try (var unknownFieldScope =
          noDiscriminantScope.openScope(
              "if (unknownFieldName.isPresent() && !allowUnknownFields)")) {
        unknownFieldScope.writeLine(
            "throw new com.xgen.proto.BsonProtoParseException(\"Unknown field named \" +"
                + " unknownFieldName.get());");
      }
      noDiscriminantScope.writeLine("reader.readEndDocument();");
      noDiscriminantScope.writeLine("return this;");
    }

    // Check the discriminant name. If it is not known then fail parsing if unknown fields are
    // not allowed.
    try (var variantTypeEmptyScope =
        fnScope.openScope(
            "if (!is%sCaseBsonString(discriminantName.get()))",
            FIELD_NAME_CONVERTER.convert(unionType.getName()))) {
      try (var disallowUnknownFieldsScope =
          variantTypeEmptyScope.openScope("if (!allowUnknownFields)")) {
        disallowUnknownFieldsScope.writeLine(
            "throw new com.xgen.proto.BsonProtoParseException(\"Unknown variant name \" +"
                + " discriminantName.get());");
      }
      variantTypeEmptyScope.writeLine("reader.readEndDocument();");
      variantTypeEmptyScope.writeLine("return this;");
    }

    // Reparse the document but against the variant type with base message fields filtered out.
    // This applies unknown field policy for each sub-message.
    fnScope.writeLine("mark.reset();");
    fnScope.writeLine(
        "reader = com.xgen.proto.DiscriminatedUnionFieldFilteringBsonReader.create(reader,"
            + " FIELD_NAMES);");
    try (var switchScope = fnScope.openScope("switch (discriminantName.get())")) {
      for (var field : this.descriptor.getFields()) {
        if (field.getRealContainingOneof() == null) {
          continue;
        }

        try (var caseScope = switchScope.openScope("case \"%s\":", field.getJsonName())) {
          // Directly generate code to read message and set the field value instead of using the
          // available helpers. The helpers call reader.getCurrentBsonType() which returns null when
          // the reader mark was positioned at a top level document rather than a sub-document.
          // These calls can be safely elided:
          // * readOneValue()'s schema type validation is unnecessary because we already know that
          //   reader is positioned at the beginning of a document.
          // * generateMergeOneOrMany() can accept an array of the schema type but that can't happen
          //   here -- again because we know read is positioned at the beginning of the document.
          caseScope.writeLine(
              "set%s(%s.newBuilder().mergeBsonFrom(reader, %s));",
              FIELD_NAME_CONVERTER.convert(field.getName()),
              getJavaFullName(field.getMessageType()),
              getFieldOptions(field).getAllowUnknownFields());
          caseScope.writeLine("break;");
        }
      }
    }
  }

  /**
   * Generate code to merge the contents of a single field into this.
   *
   * <p>Assumes a BsonReader variable named "reader" is in scope.
   *
   * @param fieldDescriptor the field to generate deserialization code for.
   * @param switchScope scope of the switch over the current field name.
   */
  private static void generateMergeField(
      FieldDescriptor fieldDescriptor, CodeOutput.Scope switchScope) {
    switchScope.writeLine("// " + fieldDescriptor.getName());
    try (var caseScope = switchScope.openScope("case \"%s\":", fieldDescriptor.getJsonName())) {
      if (fieldDescriptor.isMapField()) {
        generateMergeMapStringT(fieldDescriptor, caseScope);
      } else {
        generateMergeOne(fieldDescriptor, caseScope);
      }
      caseScope.writeLine("break;");
    }
  }

  /**
   * Generate code to merge the contents of a scalar value or array of values into a scalar or
   * repeated proto field.
   *
   * <p>Assumes a BsonReader variable named "reader" is in scope.
   *
   * @param field the field to generate deserialization code for.
   * @param caseScope scope of the case for decoding field.
   */
  private static void generateMergeOne(FieldDescriptor field, CodeOutput.Scope caseScope) {
    String fieldNamePart = FIELD_NAME_CONVERTER.convert(field.getName());
    if (field.isRepeated()) {
      if (getFieldOptions(field).getAllowSingleValue()) {
        try (var arrayScope =
            caseScope.openScope("if (reader.getCurrentBsonType() == org.bson.BsonType.ARRAY)")) {
          arrayScope.writeLine("reader.readStartArray();");
          try (var repeatedScope =
              arrayScope.openScope(
                  "while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
            generateReadOneValue(field, repeatedScope);
            generateMergeOneValue(field, fieldNamePart, repeatedScope);
          }
          arrayScope.writeLine("reader.readEndArray();");
        }
        try (var scalarScope = caseScope.openScope("else")) {
          generateReadOneValue(field, scalarScope);
          generateMergeOneValue(field, fieldNamePart, scalarScope);
        }
      } else {
        CodeGeneratorUtils.generateValidateReaderType(
            BsonType.ARRAY, field.getFullName(), caseScope);
        caseScope.writeLine("reader.readStartArray();");
        try (var repeatedScope =
            caseScope.openScope(
                "while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
          generateReadOneValue(field, repeatedScope);
          generateMergeOneValue(field, fieldNamePart, repeatedScope);
        }
        caseScope.writeLine("reader.readEndArray();");
      }
    } else {
      generateReadOneValue(field, caseScope);
      generateMergeOneValue(field, fieldNamePart, caseScope);
    }
  }

  /**
   * Generate code to merge the contents of a document into a string-keyed map proto field.
   *
   * <p>Assumes a BsonReader variable named "reader" is in scope.
   *
   * @param field the field to generate deserialization code for.
   * @param scope scope of the case for decoding field.
   */
  private static void generateMergeMapStringT(FieldDescriptor field, CodeOutput.Scope scope) {
    String fieldNamePart = FIELD_NAME_CONVERTER.convert(field.getName());
    var valueField = field.getMessageType().findFieldByName("value");
    generateValidateReaderType(field, BsonType.DOCUMENT, scope);
    scope.writeLine("reader.readStartDocument();");
    try (var entryScope =
        scope.openScope("while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
      entryScope.writeLine("var name = reader.readName();");
      generateReadOneValue(
          valueField, Optional.of(getFieldOptions(field).getAllowUnknownFields()), entryScope);
      // NB: the last value seen for a given key is used even if the value is a message type.
      // This differs from scalar message where all the values are merged.
      if (valueField.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
        entryScope.writeLine("put%s(name, value.build());", fieldNamePart);
      } else {
        entryScope.writeLine("put%s(name, value);", fieldNamePart);
      }
    }
    scope.writeLine("reader.readEndDocument();");
  }

  /**
   * Generate code to merge a variable named "value" into the current builder.
   *
   * @param field the field to generate deserialization code for.
   * @param fieldNamePart the name of the field in code as UpperCamelCase
   * @param scope scope of the case for decoding field.
   */
  private static void generateMergeOneValue(
      FieldDescriptor field, String fieldNamePart, CodeOutput.Scope scope) {
    if (field.isRepeated()) {
      scope.writeLine("add%s(value);", fieldNamePart);
    } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
      scope.writeLine("get%sBuilder().mergeFrom(value.build());", fieldNamePart);
    } else {
      scope.writeLine("set%s(value);", fieldNamePart);
    }
  }

  /**
   * Generate code to validate that the next value to decode matches the schema imposed by the field
   * and throw a BsonProtoParseException if it does not match.
   *
   * @param fieldDescriptor the field to generate deserialization code for.
   * @param bsonType the expected bson type of the current value.
   * @param parentScope the scope to generate the validation code into.
   */
  private static void generateValidateReaderType(
      FieldDescriptor fieldDescriptor, BsonType bsonType, CodeOutput.Scope parentScope) {
    CodeGeneratorUtils.generateValidateReaderType(
        bsonType, fieldDescriptor.getFullName(), parentScope);
  }

  private void generateDiscriminatedUnionHelpers(OneofDescriptor unionType) {
    String fieldNamePart = FIELD_NAME_CONVERTER.convert(unionType.getName());
    Converter<String, String> caseNameConverter =
        CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.UPPER_UNDERSCORE);

    try (var caseBsonStringScope =
        this.messageOutput.openScope(
            "public static java.util.Optional<String> get%sCaseBsonString(%sCase unionCase)",
            fieldNamePart, fieldNamePart)) {
      try (var switchScope = caseBsonStringScope.openScope("switch (unionCase)")) {
        // Emit a value for each "real" case and omit the NOT_SET case. This maps to empty.
        for (var variant : unionType.getFields()) {
          try (var caseScope =
              switchScope.openScope("case %s:", caseNameConverter.convert(variant.getName()))) {
            caseScope.writeLine("return java.util.Optional.of(\"%s\");", variant.getJsonName());
          }
        }
        try (var defaultCaseScope = switchScope.openScope("default:")) {
          defaultCaseScope.writeLine("return java.util.Optional.empty();");
        }
      }
    }

    try (var isCaseBsonStringScope =
        this.messageOutput.openScope(
            "public static boolean is%sCaseBsonString(java.lang.String variantName)",
            fieldNamePart)) {
      try (var switchScope = isCaseBsonStringScope.openScope("switch (variantName)")) {
        for (var variant : unionType.getFields()) {
          try (var caseScope = switchScope.openScope("case \"%s\":", variant.getJsonName())) {
            caseScope.writeLine("return true;");
          }
        }
        try (var defaultCaseScope = switchScope.openScope("default:")) {
          defaultCaseScope.writeLine("return false;");
        }
      }
    }

    // Write the json names of all the fields (including discriminator field) as a static set.
    // This will be used filter out fields from the stream when parsing the variant message.
    var fieldNames =
        Stream.concat(
                Stream.of(
                    CaseFormat.LOWER_UNDERSCORE
                        .converterTo(CaseFormat.LOWER_CAMEL)
                        .convert(unionType.getName())),
                this.descriptor.getFields().stream()
                    .filter(f -> f.getRealContainingOneof() == null)
                    .map(f -> f.getJsonName()))
            .sorted()
            .map(n -> "\"" + n + "\"")
            .collect(Collectors.joining(", "));
    this.builderOutput
        .getRootScope()
        .writeLine(
            "private static final java.util.Set<String> FIELD_NAMES = java.util.Set.of(%s);",
            fieldNames);
  }
}
