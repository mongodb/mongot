package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.EnumDescriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import com.google.errorprone.annotations.MustBeClosed;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.protobuf.FieldOptions;
import org.bson.protobuf.MessageType;
import org.bson.protobuf.Options;
import org.jetbrains.annotations.Nullable;

public class CodeGeneratorUtils {
  private CodeGeneratorUtils() {}

  /** Maps BsonType to the BsonValue subclass that holds that type. */
  private static final EnumMap<BsonType, Class<? extends BsonValue>> BSON_TYPE_TO_VALUE_CLASS;

  static {
    BSON_TYPE_TO_VALUE_CLASS = new EnumMap<>(BsonType.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.ARRAY, BsonArray.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.BINARY, BsonBinary.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.BOOLEAN, BsonBoolean.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.DATE_TIME, BsonDateTime.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.DB_POINTER, BsonDbPointer.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.DECIMAL128, BsonDecimal128.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.DOCUMENT, BsonDocument.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.DOUBLE, BsonDouble.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.INT32, BsonInt32.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.INT64, BsonInt64.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.JAVASCRIPT, BsonJavaScript.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.JAVASCRIPT_WITH_SCOPE, BsonJavaScriptWithScope.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.OBJECT_ID, BsonObjectId.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.REGULAR_EXPRESSION, BsonRegularExpression.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.STRING, BsonString.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.SYMBOL, BsonSymbol.class);
    BSON_TYPE_TO_VALUE_CLASS.put(BsonType.TIMESTAMP, BsonTimestamp.class);
  }

  public static CodeGeneratorResponse.File.Builder initializeFileBuilder(
      Descriptor messageDescriptor, String scope) {
    return CodeGeneratorResponse.File.newBuilder()
        .setName(NameUtils.getJavaFilename(messageDescriptor))
        .setInsertionPoint(String.format("%s:%s", scope, messageDescriptor.getFullName()));
  }

  public static CodeGeneratorResponse.File.Builder initializeFileBuilder(
      EnumDescriptor enumDescriptor) {
    return CodeGeneratorResponse.File.newBuilder()
        .setName(NameUtils.getJavaFilename(enumDescriptor))
        .setInsertionPoint("enum_scope:" + enumDescriptor.getFullName());
  }

  /**
   * Create code generation files for message types.
   *
   * <p>This inserts specified output for message and builder and ensures that the BsonMessage and
   * BsonMessage.BsonBuilder interfaces are implemented.
   *
   * @param messageDescriptor the descriptor of the message to generate code for.
   * @param messageOutput output for message class.
   * @param builderOutput output for builder class.
   * @return an {@link Iterable} over generated code to emit.
   */
  public static Iterable<CodeGeneratorResponse.File> messageFiles(
      Descriptor messageDescriptor, CodeOutput messageOutput, CodeOutput builderOutput) {
    return messageFiles(messageDescriptor, messageOutput, builderOutput, false);
  }

  /**
   * Create code generation files for value message types.
   *
   * <p>This inserts specified output for message and builder and ensures that the BsonValueMessage
   * and BsonValueMessage.BsonBuilder interfaces are implemented.
   *
   * @param messageDescriptor the descriptor of the message to generate code for.
   * @param messageOutput output for message class.
   * @param builderOutput output for builder class.
   * @return an {@link Iterable} over generated code to emit.
   */
  public static Iterable<CodeGeneratorResponse.File> valueMessageFiles(
      Descriptor messageDescriptor, CodeOutput messageOutput, CodeOutput builderOutput) {
    return messageFiles(messageDescriptor, messageOutput, builderOutput, true);
  }

  private static Iterable<CodeGeneratorResponse.File> messageFiles(
      Descriptor messageDescriptor,
      CodeOutput messageOutput,
      CodeOutput builderOutput,
      boolean isValueMessage) {
    // Value messages can't be parsed from a BsonDocument or byte stream.
    if (!isValueMessage) {
      CodeGeneratorUtils.addParseFromHelpers(messageDescriptor, messageOutput);
      CodeGeneratorUtils.addMergeFromHelpers(messageDescriptor, builderOutput);
    }
    return List.of(
        messageOutput.buildResponse(messageDescriptor, "class_scope"),
        builderOutput.buildResponse(messageDescriptor, "builder_scope"),
        initializeFileBuilder(messageDescriptor, "message_implements")
            .setContent(
                String.format(
                    "com.xgen.proto.%s,", isValueMessage ? "BsonValueMessage" : "BsonMessage"))
            .build(),
        initializeFileBuilder(messageDescriptor, "builder_implements")
            .setContent(
                String.format(
                    "com.xgen.proto.%s,",
                    isValueMessage
                        ? "BsonValueMessage.BsonValueBuilder"
                        : "BsonMessage.BsonBuilder"))
            .build());
  }

  @MustBeClosed
  public static CodeOutput.Scope openWriteBsonToScope(CodeOutput output) {
    return output.openScope("@Override public void writeBsonTo(org.bson.BsonWriter writer)");
  }

  @MustBeClosed
  public static CodeOutput.Scope openMergeBsonFromReader(
      Descriptor messageDescriptor, CodeOutput output) {
    return output.openScope(
        "@Override public Builder mergeBsonFrom(org.bson.BsonReader reader%s) throws"
            + " com.xgen.proto.BsonProtoParseException",
        getMessageType(messageDescriptor) != MessageType.VALUE
            ? ", boolean allowUnknownFields"
            : "");
  }

  @MustBeClosed
  public static CodeOutput.Scope openMergeBsonFromReaderValidatedType(
      Descriptor messageDescriptor, BsonType valueType, CodeOutput output) {
    // First emit a mergeBsonFrom(BsonReader) implementation that may throw and checks valueType,
    // then open a second package-private method that does not validate the type. This is useful
    // for well-known types to call each other (mostly for ProtoValue).
    try (var mergeFromReader = openMergeBsonFromReader(messageDescriptor, output)) {
      generateValidateReaderType(valueType, messageDescriptor.getFullName(), mergeFromReader);
      mergeFromReader.writeLine("return mergeBsonFromTypeValidated(reader);");
    }

    return output.openScope("Builder mergeBsonFromTypeValidated(org.bson.BsonReader reader)");
  }

  @MustBeClosed
  public static CodeOutput.Scope openBsonValueMergeScope(
      Descriptor messageDescriptor,
      BsonType expectedType,
      CodeOutput builderOutput,
      CodeOutput messageOutput) {
    generateBsonValueMerge(expectedType, messageDescriptor.getFullName(), builderOutput);
    return openBsonValueMergeWithoutHelpersScope(
        messageDescriptor,
        BSON_TYPE_TO_VALUE_CLASS.get(expectedType),
        builderOutput,
        messageOutput);
  }

  @MustBeClosed
  public static CodeOutput.Scope openBsonValueMergeWithoutHelpersScope(
      Descriptor messageDescriptor,
      Class<? extends BsonValue> valueClass,
      CodeOutput builderOutput,
      CodeOutput messageOutput) {
    String valueClassName = valueClass.getCanonicalName();
    try (var parseFrom =
        messageOutput.openScope(
            "public static %s parseBsonFrom(%s value)",
            messageDescriptor.getName(), valueClassName)) {
      parseFrom.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }

    return builderOutput.openScope("public Builder mergeBsonFrom(%s value)", valueClassName);
  }

  private static void generateBsonValueMerge(
      BsonType expectedType, String messageName, CodeOutput builderOutput) {
    String valueClassName = getBsonValueClassName(expectedType);
    try (var mergeFrom =
        builderOutput.openScope(
            "@Override public Builder mergeBsonFrom(org.bson.BsonValue value) throws"
                + " com.xgen.proto.BsonProtoParseException")) {
      generateValidateType(expectedType, "value.getBsonType()", messageName, mergeFrom);
      mergeFrom.writeLine("return mergeBsonFrom((%s) value);", valueClassName);
    }
  }

  private static void addMergeFromHelpers(Descriptor messageDescriptor, CodeOutput output) {
    try (var mergeFromReaderScope =
        output.openScope(
            "@Override public Builder mergeBsonFrom(org.bson.BsonReader reader) throws"
                + " com.xgen.proto.BsonProtoParseException")) {
      mergeFromReaderScope.writeLine("return mergeBsonFrom(reader, false);");
    }

    if (messageDescriptor.getFullName().equals(WellKnownTypes.DOCUMENT.getMessageClassName())) {
      try (var mergeFromDocScope =
          output.openScope("@Override public Builder mergeBsonFrom(org.bson.BsonDocument doc)")) {
        mergeFromDocScope.writeLine(
            "return mergeBsonFromTypeValidated(new org.bson.BsonDocumentReader(doc));");
      }
    } else {
      try (var mergeFromDocScope =
          output.openScope(
              "@Override public Builder mergeBsonFrom(org.bson.BsonDocument doc) throws"
                  + " com.xgen.proto.BsonProtoParseException")) {
        mergeFromDocScope.writeLine("return mergeBsonFrom(new org.bson.BsonDocumentReader(doc));");
      }
    }

    try (var mergeFromValueScope =
        output.openScope(
            "@Override public Builder mergeBsonFrom(org.bson.BsonValue value) throws"
                + " com.xgen.proto.BsonProtoParseException")) {
      generateValidateType(
          BsonType.DOCUMENT,
          "value.getBsonType()",
          messageDescriptor.getFullName(),
          mergeFromValueScope);
      mergeFromValueScope.writeLine("return mergeBsonFrom(value.asDocument());");
    }

    try (var mergeFromBufScope =
        output.openScope(
            "@Override public Builder mergeBsonFrom(java.nio.ByteBuffer buf) throws"
                + " com.xgen.proto.BsonProtoParseException")) {
      mergeFromBufScope.writeLine("return mergeBsonFrom(new org.bson.BsonBinaryReader(buf));");
    }
  }

  private static void addParseFromHelpers(Descriptor messageDescriptor, CodeOutput output) {
    try (var parseFromReaderScope =
        output.openScope(
            "public static %s parseBsonFrom(org.bson.BsonReader reader) throws"
                + " com.xgen.proto.BsonProtoParseException",
            messageDescriptor.getName())) {
      parseFromReaderScope.writeLine("return parseBsonFrom(reader, false);");
    }

    try (var parseFromReaderUnknownScope =
        output.openScope(
            "public static %s parseBsonFrom(org.bson.BsonReader reader, boolean allowUnknownFields)"
                + " throws com.xgen.proto.BsonProtoParseException",
            messageDescriptor.getName())) {
      parseFromReaderUnknownScope.writeLine(
          "return newBuilder().mergeBsonFrom(reader, allowUnknownFields).build();");
    }

    if (messageDescriptor.getFullName().equals(WellKnownTypes.DOCUMENT.getMessageClassName())) {
      try (var parseFromDocScope =
          output.openScope(
              "public static %s parseBsonFrom(org.bson.BsonDocument doc)",
              messageDescriptor.getName())) {
        parseFromDocScope.writeLine("return newBuilder().mergeBsonFrom(doc).build();");
      }
    } else {
      try (var parseFromDocScope =
          output.openScope(
              "public static %s parseBsonFrom(org.bson.BsonDocument doc) throws"
                  + " com.xgen.proto.BsonProtoParseException",
              messageDescriptor.getName())) {
        parseFromDocScope.writeLine("return parseBsonFrom(new org.bson.BsonDocumentReader(doc));");
      }
    }

    try (var parseFromBinScope =
        output.openScope(
            "public static %s parseBsonFrom(java.nio.ByteBuffer buf) throws"
                + " com.xgen.proto.BsonProtoParseException",
            messageDescriptor.getName())) {
      parseFromBinScope.writeLine("return parseBsonFrom(new org.bson.BsonBinaryReader(buf));");
    }
  }

  /**
   * Get the protobuf BSON {@link MessageType} assigned to the message type.
   *
   * @param messageDescriptor descriptor to get the type from.
   * @return the {@link MessageType} of the message.
   */
  public static MessageType getMessageType(Descriptor messageDescriptor) {
    return messageDescriptor.getOptions().getExtension(Options.messageOptions).getMessageType();
  }

  /**
   * Get the protobuf BSON options for the field.
   *
   * @param field the field to get the options for
   * @return FieldOptions, either set explicitly for the field or defaults.
   */
  public static FieldOptions getFieldOptions(FieldDescriptor field) {
    return field.getOptions().getExtension(Options.fieldOptions);
  }

  /**
   * Generate code to write a single value.
   *
   * <p>Assumes "var value" and "BsonWriter writer" are in scope.
   *
   * @param field the field to generate serialization code for.
   * @param scope scope to write the serialization code into.
   */
  public static void generateWriteOneValue(FieldDescriptor field, CodeOutput.Scope scope) {
    switch (field.getJavaType()) {
      case MESSAGE -> scope.writeLine("value.writeBsonTo(writer);");
      case STRING -> scope.writeLine("writer.writeString(value);");
      case BYTE_STRING ->
          scope.writeLine("writer.writeBinaryData(new org.bson.BsonBinary(value.toByteArray()));");
      case ENUM -> scope.writeLine("writer.writeString(value.toBsonString());");
      case BOOLEAN -> scope.writeLine("writer.writeBoolean(value);");
      case INT -> scope.writeLine("writer.writeInt32(value);");
      case LONG -> scope.writeLine("writer.writeInt64(value);");
      case FLOAT, DOUBLE -> scope.writeLine("writer.writeDouble(value);");
    }
  }

  /**
   * Generate code to create a variable named "value" from the reader stream that conforms to the
   * schema imposed by field.
   *
   * <p>Assumes a BsonReader variable named "reader" is in scope.
   *
   * @param field the field to generate deserialization code for.
   * @param scope scope of the case for decoding field.
   */
  public static void generateReadOneValue(FieldDescriptor field, CodeOutput.Scope scope) {
    generateReadOneValue(field, Optional.empty(), scope);
  }

  /**
   * Generate code to create a variable named "value" from the reader stream that conforms to the
   * schema imposed by field.
   *
   * <p>Assumes a BsonReader variable named "reader" is in scope.
   *
   * @param field the field to generate deserialization code for.
   * @param allowUnknownFields to allow unknown fields. If unset, this is derived from field.
   * @param scope scope of the case for decoding field.
   */
  public static void generateReadOneValue(
      FieldDescriptor field, Optional<Boolean> allowUnknownFields, CodeOutput.Scope scope) {
    // If this is a field in a value message then we are choosing the destination field by BsonType
    // there is no need to do type validation or numeric type coercion.
    final boolean inValueMessage = getMessageType(field.getContainingType()) == MessageType.VALUE;
    switch (field.getJavaType()) {
      case MESSAGE -> {
        // Messages are typically documents but if they are marked 'as_value' they could be any
        // other type and operate through slightly different interfaces.
        boolean isValueMessage = getMessageType(field.getMessageType()) == MessageType.VALUE;
        String javaName = getJavaFullName(field.getMessageType());
        if (!isValueMessage) {
          if (!inValueMessage) {
            generateValidateReaderType(BsonType.DOCUMENT, field.getFullName(), scope);
          }
          scope.writeLine(
              "var value = %s.newBuilder().mergeBsonFrom(reader, %s);",
              javaName, allowUnknownFields.orElse(getFieldOptions(field).getAllowUnknownFields()));
        } else {
          // Validate based on the types presented by getBsonValueTypes().
          if (!inValueMessage) {
            generateValidateValueReaderType(javaName, field.getFullName(), scope);
          }
          scope.writeLine("var value = %s.newBuilder().mergeBsonFrom(reader);", javaName);
        }
      }
      case STRING -> {
        if (!inValueMessage) {
          generateValidateReaderType(BsonType.STRING, field.getFullName(), scope);
        }
        scope.writeLine("var value = reader.readString();");
      }
      case BYTE_STRING -> {
        if (!inValueMessage) {
          generateValidateReaderType(BsonType.BINARY, field.getFullName(), scope);
        }
        scope.writeLine("var binValue = reader.readBinaryData();");
        try (var ifScope = scope.openScope("if (binValue.getType() != 0)")) {
          ifScope.writeLine(
              "throw new com.xgen.proto.BsonProtoParseException(\"non-zero binary sub-type for"
                  + " bytes field %s\");",
              field.getFullName());
        }
        scope.writeLine("var value = com.google.protobuf.ByteString.copyFrom(binValue.getData());");
      }
      case ENUM -> {
        // TODO: accept integer values as well; map invalid values to UNRECOGNIZED for open enums.
        if (!inValueMessage) {
          generateValidateReaderType(BsonType.STRING, field.getFullName(), scope);
        }
        scope.writeLine(
            "var value = %s.fromBsonString(reader.readString());",
            getJavaFullName(field.getEnumType()));
      }
      case BOOLEAN -> {
        if (!inValueMessage) {
          generateValidateReaderType(BsonType.BOOLEAN, field.getFullName(), scope);
        }
        scope.writeLine("var value = reader.readBoolean();");
      }
      case INT -> {
        if (inValueMessage) {
          scope.writeLine("int value = reader.readInt32();");
        } else {
          scope.writeLine(
              "int value = com.xgen.proto.NumericUtils.readIntValue(\"%s\", reader);",
              field.getJsonName());
        }
      }
      case LONG -> {
        if (inValueMessage) {
          scope.writeLine("long value = reader.readInt64();");
        } else {
          scope.writeLine(
              "long value = com.xgen.proto.NumericUtils.readLongValue(\"%s\", reader);",
              field.getJsonName());
        }
      }
      case FLOAT -> {
        if (inValueMessage) {
          scope.writeLine("float value = (float)reader.readDouble();");
        } else {
          scope.writeLine(
              "float value = com.xgen.proto.NumericUtils.readFloatValue(\"%s\", reader);",
              field.getJsonName());
        }
      }
      case DOUBLE -> {
        if (inValueMessage) {
          scope.writeLine("double value = reader.readDouble();");
        } else {
          scope.writeLine(
              "double value = com.xgen.proto.NumericUtils.readDoubleValue(\"%s\", reader);",
              field.getJsonName());
        }
      }
    }
  }

  /**
   * Generate code to validate that the next value to decode matches the schema imposed by the field
   * and throw a BsonProtoParseException if it does not match.
   *
   * @param bsonType the expected bson type of the current value.
   * @param message message to provide if there is a type mismatch.
   * @param parentScope the scope to generate the validation code into.
   */
  public static void generateValidateReaderType(
      BsonType bsonType, String message, CodeOutput.Scope parentScope) {
    // For DOCUMENT getCurrentBsonType() may be null if we are at the beginning of the stream.
    // In that case prime the stream by reading the first type.
    if (bsonType == BsonType.DOCUMENT) {
      try (var initialScope = parentScope.openScope("if (reader.getCurrentBsonType() == null)")) {
        initialScope.writeLine("reader.readBsonType();");
      }
    }
    generateValidateType(bsonType, "reader.getCurrentBsonType()", message, parentScope);
  }

  /**
   * Generate code to ensure that the BsonType returned by actualTypeExpr matches expectedType, and
   * throw a BsonProtoParseException if it does not match.
   *
   * @param expectedType the expected bson type of the current value.
   * @param actualTypeExpr an expression that evaluates to the actual type.
   * @param message message to provide if there is a type mismatch.
   * @param parentScope the scope to generate the validation code into.
   */
  public static void generateValidateType(
      BsonType expectedType, String actualTypeExpr, String message, CodeOutput.Scope parentScope) {
    String expectedTypeExpr = "org.bson.BsonType." + expectedType;
    try (var scope = parentScope.openScope("if (%s != %s)", actualTypeExpr, expectedTypeExpr)) {
      scope.writeLine(
          "com.xgen.proto.BsonProtoParseException.throwTypeMismatchException(%s, %s, \"%s\");",
          expectedTypeExpr, actualTypeExpr, message);
    }
  }

  /**
   * Generate code to validate that the next value is one of the types accepted by messageJavaType
   * during parsing and throw a BsonProtoParseException if it does not match.
   *
   * @param messageJavaType Java class name for the as_value message type.
   * @param message message to provide if there is a type mismatch.
   * @param parentScope the scope to generate the validation code into.
   */
  public static void generateValidateValueReaderType(
      String messageJavaType, String message, CodeOutput.Scope parentScope) {
    try (var validateScope =
        parentScope.openScope(
            "if (!%s.getBsonValueTypes().contains(reader.getCurrentBsonType()))",
            messageJavaType)) {
      validateScope.writeLine(
          "com.xgen.proto.BsonProtoParseException.throwTypeMismatchException(%s.getBsonValueTypes(),"
              + " reader.getCurrentBsonType(), \"%s\");",
          messageJavaType, message);
    }
  }

  /**
   * Generate an implementation of getBsonValueTypes() for as_value messages.
   *
   * @param allowedType the type that this message can parse.
   * @param messageScope message class to write the implementation into.
   */
  public static void generateBsonValueTypes(BsonType allowedType, CodeOutput.Scope messageScope) {
    generateBsonValueTypes(EnumSet.of(allowedType), messageScope);
  }

  /**
   * Generate an implementation of getBsonValueTypes() for as_value messages.
   *
   * @param allowedTypes list of types to accept.
   * @param messageScope message class to write the implementation into.
   */
  public static void generateBsonValueTypes(
      EnumSet<BsonType> allowedTypes, CodeOutput.Scope messageScope) {
    messageScope.writeLine(
        String.format(
            "private static java.util.EnumSet<org.bson.BsonType> BSON_VALUE_TYPES ="
                + " java.util.EnumSet.of(%s);",
            allowedTypes.stream()
                .map(t -> String.format("org.bson.BsonType.%s", t.name()))
                .collect(Collectors.joining(", "))));
    try (var fnScope =
        messageScope.openScope(
            "public static java.util.EnumSet<org.bson.BsonType> getBsonValueTypes()")) {
      fnScope.writeLine("return BSON_VALUE_TYPES;");
    }
  }

  /**
   * Get the fully qualified java symbol name.
   *
   * @param enumDescriptor enum type to get the symbol name for.
   * @return the fully qualified java symbol name.
   */
  public static String getJavaFullName(EnumDescriptor enumDescriptor) {
    var components = getNestedPath(enumDescriptor.getName(), enumDescriptor.getContainingType());
    components.addFirst(NameUtils.getJavaPackagePath(enumDescriptor.getFile()));
    return String.join(".", components);
  }

  /**
   * Get the fully qualified java symbol name.
   *
   * @param messageDescriptor message type to get the symbol name for.
   * @return the fully qualified java symbol name.
   */
  public static String getJavaFullName(Descriptor messageDescriptor) {
    var components =
        getNestedPath(messageDescriptor.getName(), messageDescriptor.getContainingType());
    components.addFirst(NameUtils.getJavaPackagePath(messageDescriptor.getFile()));
    return String.join(".", components);
  }

  /**
   * Returns the canonical BsonValue class names for the given type. For example:
   *
   * <p>getBsonValueClassName(BsonType.STRING) => "org.bson.BsonString"
   *
   * @param bsonType
   * @return canonical class name
   */
  public static String getBsonValueClassName(BsonType bsonType) {
    return BSON_TYPE_TO_VALUE_CLASS.get(bsonType).getCanonicalName();
  }

  /**
   * Given a name and containing message, get the full path up to the level of the containing file
   * by traversing all parent message type.
   *
   * @param name name of the initial symbol
   * @param containingMessage message that contains the named symbol.
   * @return a list of strings that enumerates the full path.
   */
  private static ArrayDeque<String> getNestedPath(
      String name, @Nullable Descriptor containingMessage) {
    var components = new ArrayDeque<String>();
    components.addLast(name);
    while (containingMessage != null) {
      components.addFirst(containingMessage.getName());
      containingMessage = containingMessage.getContainingType();
    }
    return components;
  }
}
