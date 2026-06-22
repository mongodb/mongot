package com.xgen.proto.plugin;

import static com.xgen.proto.plugin.CodeGeneratorUtils.generateReadOneValue;
import static com.xgen.proto.plugin.CodeGeneratorUtils.generateValidateReaderType;
import static com.xgen.proto.plugin.CodeGeneratorUtils.generateWriteOneValue;
import static com.xgen.proto.plugin.CodeGeneratorUtils.getFieldOptions;
import static com.xgen.proto.plugin.CodeGeneratorUtils.getJavaFullName;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.compiler.PluginProtos;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.protobuf.MessageType;

/**
 * Generates code for a value message type that implements {@link com.xgen.proto.BsonValueMessage}.
 *
 * <p>Value messages can be a single repeated value or a union of different types. When serialized
 * to BSON these messages must be wrapped in another message; an attempt to serialize one of these
 * objects by itself will fail as there is no document wrapper.
 */
public class GenericValueMessageCodeGenerator implements CodeGenerator {
  private static final Converter<String, String> FIELD_NAME_CONVERTER =
      CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.UPPER_CAMEL);
  private static final Converter<String, String> ONEOF_CASE_NAME_CONVERTER =
      CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.UPPER_UNDERSCORE);

  private final Descriptor descriptor;
  private final Optional<OneofDescriptor> typeUnion;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public GenericValueMessageCodeGenerator(Descriptor messageDescriptor) {
    validateMessageType(messageDescriptor);
    this.descriptor = messageDescriptor;
    this.typeUnion = this.descriptor.getRealOneofs().stream().findFirst();
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    this.typeUnion.ifPresentOrElse(
        typeUnion -> generateTypeUnion(typeUnion), () -> generateRepeatedField());

    generateMergeBsonValue();

    CodeGeneratorUtils.generateBsonValueTypes(
        getMessageValueTypes(this.descriptor), this.messageOutput.getRootScope());

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }

  private static void validateMessageType(Descriptor messageDescriptor) {
    switch (messageDescriptor.getRealOneofs().size()) {
      case 0 -> {
        if (messageDescriptor.getFields().size() != 1) {
          throw new IllegalArgumentException(
              String.format(
                  "Value message %s may only have one field.", messageDescriptor.getFullName()));
        }
        FieldDescriptor field = messageDescriptor.getFields().get(0);
        if (!field.isRepeated()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value message %s field must be repeated.", messageDescriptor.getFullName()));
        }
        if (field.isMapField()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value message %s field may not be a map field.",
                  messageDescriptor.getFullName()));
        }
        if (getFieldOptions(field).getAllowSingleValue()) {
          throw new IllegalArgumentException(
              String.format(
                  "allow_single_value not supported for repeated value in %s",
                  messageDescriptor.getFullName()));
        }
      }
      case 1 -> {
        if (messageDescriptor.getRealOneofs().get(0).getFields().size()
            != messageDescriptor.getFields().size()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value message %s oneof must cover all fields in the message.",
                  messageDescriptor.getFullName()));
        }
      }
      default ->
          throw new IllegalArgumentException(
              String.format(
                  "Value message %s has multiple oneofs, only one is allowed for type unions.",
                  messageDescriptor.getFullName()));
    }

    var fields =
        messageDescriptor.getRealOneofs().stream()
            .findFirst()
            .map(union -> union.getFields())
            .orElseGet(messageDescriptor::getFields);
    var fieldTypes =
        fields.stream()
            .map(GenericValueMessageCodeGenerator::getFieldValueType)
            .collect(() -> EnumSet.noneOf(BsonType.class), Set::add, Set::addAll);
    if (fields.size() != fieldTypes.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Value message %s has multiple fields that map back to the same type.",
              messageDescriptor.getFullName()));
    }
    if (fields.stream().filter(f -> getFieldOptions(f).getAllowSingleValue()).count() > 0) {
      throw new IllegalArgumentException(
          String.format(
              "No field in %s may have allow_single_value set", messageDescriptor.getFullName()));
    }
  }

  /**
   * Returns the set of BsonTypes this value message may contain as part of a type union.
   *
   * @param messageDescriptor
   * @return the set of bson types.
   */
  public static EnumSet<BsonType> getMessageValueTypes(Descriptor messageDescriptor) {
    if (CodeGeneratorUtils.getMessageType(messageDescriptor) != MessageType.VALUE) {
      return EnumSet.of(BsonType.DOCUMENT);
    }

    Optional<WellKnownTypes> knownType = WellKnownTypes.get(messageDescriptor);
    if (knownType.isPresent()) {
      return knownType.get().getBsonTypes();
    }

    validateMessageType(messageDescriptor);
    return messageDescriptor.getRealOneofs().stream()
        .findFirst()
        .map(union -> union.getFields())
        .orElseGet(messageDescriptor::getFields)
        .stream()
        .map(GenericValueMessageCodeGenerator::getFieldValueType)
        .collect(() -> EnumSet.noneOf(BsonType.class), Set::add, Set::addAll);
  }

  private static BsonType getFieldValueType(FieldDescriptor field) {
    // Repeated fields are all represented as arrays. Fields within a oneof are guaranteed to be
    // scalar so in that case we delegate to the underlying type which may be a message containing
    // a repeated field.
    return field.isRepeated() ? BsonType.ARRAY : getUnderlyingFieldValueType(field);
  }

  // Like getFieldValueType() but we ignore "repeated" and get any underlying type.
  private static BsonType getUnderlyingFieldValueType(FieldDescriptor field) {
    return switch (field.getJavaType()) {
      case BOOLEAN -> BsonType.BOOLEAN;
      case INT -> BsonType.INT32;
      case LONG -> BsonType.INT64;
      case FLOAT, DOUBLE -> BsonType.DOUBLE;
      case ENUM, STRING -> BsonType.STRING;
      case BYTE_STRING -> BsonType.BINARY;
      case MESSAGE -> {
        var bsonTypes = getMessageValueTypes(field.getMessageType());
        if (bsonTypes.size() != 1) {
          throw new IllegalArgumentException(
              String.format(
                  "Values message field %s may not be a type union", field.getFullName()));
        }
        yield bsonTypes.iterator().next();
      }
    };
  }

  private void generateTypeUnion(OneofDescriptor typeUnion) {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      generateWriteBsonToTypeUnion(typeUnion, writeBsonToScope);
    }

    generateToBson(true);

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReader(this.descriptor, this.builderOutput)) {
      generateMergeBsonFromTypeUnion(typeUnion, mergeBsonFromScope);
    }
  }

  private void generateRepeatedField() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      generateWriteBsonToRepeatedField(writeBsonToScope);
    }

    generateToBson(false);

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReader(this.descriptor, this.builderOutput)) {
      generateMergeBsonFromRepeatedField(mergeBsonFromScope);
    }
  }

  private void generateWriteBsonToTypeUnion(OneofDescriptor typeUnion, CodeOutput.Scope fnScope) {
    try (var switchScope =
        fnScope.openScope(
            "switch (get%sCase())", FIELD_NAME_CONVERTER.convert(typeUnion.getName()))) {
      for (var field : typeUnion.getFields()) {
        try (var caseScope =
            switchScope.openScope("case %s:", ONEOF_CASE_NAME_CONVERTER.convert(field.getName()))) {
          caseScope.writeLine(
              "var value = get%s();", FIELD_NAME_CONVERTER.convert(field.getName()));
          generateWriteOneValue(field, caseScope);
          caseScope.writeLine("break;");
        }
      }
      try (var defaultScope = switchScope.openScope("default:")) {
        defaultScope.writeLine(
            "throw new java.lang.IllegalStateException(\"Value must be set in %s before"
                + " serializing\");",
            typeUnion.getFullName());
      }
    }
  }

  private void generateWriteBsonToRepeatedField(CodeOutput.Scope fnScope) {
    // Safety: message validation ensures that there is exactly one repeated field.
    FieldDescriptor field = this.descriptor.getFields().get(0);
    String fieldNamePart = FIELD_NAME_CONVERTER.convert(field.getName());
    fnScope.writeLine("writer.writeStartArray();");
    try (var loopScope = fnScope.openScope("for (var value : get%sList())", fieldNamePart)) {
      generateWriteOneValue(field, loopScope);
    }
    fnScope.writeLine("writer.writeEndArray();");
  }

  private void generateToBson(boolean isTypeUnion) {
    String returnType = (isTypeUnion ? BsonValue.class : BsonArray.class).getCanonicalName();
    try (var toBson = this.messageOutput.openScope("@Override public %s toBson()", returnType)) {
      toBson.writeLine(
          "var writer = new org.bson.BsonDocumentWriter(new org.bson.BsonDocument());");
      toBson.writeLine("writer.writeStartDocument();");
      toBson.writeLine("writer.writeName(\"sentinel\");");
      toBson.writeLine("writeBsonTo(writer);");
      if (isTypeUnion) {
        toBson.writeLine("return writer.getDocument().get(\"sentinel\");");
      } else {
        toBson.writeLine("return (%s) writer.getDocument().get(\"sentinel\");", returnType);
      }
    }
  }

  private void generateMergeBsonFromTypeUnion(OneofDescriptor typeUnion, CodeOutput.Scope fnScope) {
    try (var switchScope = fnScope.openScope("switch (reader.getCurrentBsonType())")) {
      for (FieldDescriptor field : typeUnion.getFields()) {
        String fieldNamePart = FIELD_NAME_CONVERTER.convert(field.getName());
        BsonType bsonType = getFieldValueType(field);
        try (var caseScope = switchScope.openScope("case %s:", bsonType)) {
          generateReadOneValue(field, caseScope);
          caseScope.writeLine("set%s(value);", fieldNamePart);
          caseScope.writeLine("return this;");
        }
      }
      try (var defaultScope = switchScope.openScope("default:")) {
        defaultScope.writeLine(
            "throw"
                + " com.xgen.proto.BsonProtoParseException.createTypeMismatchException(%s.getBsonValueTypes(),"
                + " reader.getCurrentBsonType(), \"%s\");",
            getJavaFullName(this.descriptor), typeUnion.getFullName());
      }
    }
  }

  private void generateMergeBsonFromRepeatedField(CodeOutput.Scope fnScope) {
    // Safety: validation ensures there is only one field in the message and that it is repeated.
    FieldDescriptor field = this.descriptor.getFields().get(0);
    CodeGeneratorUtils.generateValidateReaderType(BsonType.ARRAY, field.getFullName(), fnScope);
    fnScope.writeLine("reader.readStartArray();");
    try (var loopScope =
        fnScope.openScope("while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
      generateValidateReaderType(
          getUnderlyingFieldValueType(field), this.descriptor.getFullName(), loopScope);
      generateReadOneValue(field, loopScope);
      loopScope.writeLine("add%s(value);", FIELD_NAME_CONVERTER.convert(field.getName()));
    }
    fnScope.writeLine("reader.readEndArray();");
    fnScope.writeLine("return this;");
  }

  private void generateMergeBsonValue() {
    try (var mergeValue =
        this.builderOutput.openScope(
            "@Override public Builder mergeBsonFrom(org.bson.BsonValue value) throws"
                + " com.xgen.proto.BsonProtoParseException")) {
      mergeValue.writeLine(
          "var reader = new org.bson.BsonDocumentReader(new org.bson.BsonDocument(\"sentinel\","
              + " value));");
      mergeValue.writeLine("reader.readStartDocument();");
      mergeValue.writeLine("reader.readName();");
      mergeValue.writeLine("return mergeBsonFrom(reader);");
    }

    try (var parseValue =
        this.messageOutput.openScope(
            "public static %s parseBsonFrom(org.bson.BsonValue value) throws"
                + " com.xgen.proto.BsonProtoParseException",
            this.descriptor.getName())) {
      parseValue.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }
  }
}
