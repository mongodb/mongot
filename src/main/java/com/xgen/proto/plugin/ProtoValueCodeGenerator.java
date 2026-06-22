package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;
import org.bson.BsonValue;

public class ProtoValueCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoValueCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      generateWriteBsonTo(writeBsonToScope);
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonValue toBson()")) {
      generateToBson(toBsonScope);
    }

    try (var mergeBsonValueScope =
        CodeGeneratorUtils.openBsonValueMergeWithoutHelpersScope(
            this.descriptor, BsonValue.class, this.builderOutput, this.messageOutput)) {
      generateMergeBsonValue(mergeBsonValueScope);
    }

    // Allow parsing and merging from a raw ObjectId.
    try (var mergeFromObjectId =
        this.builderOutput.openScope(
            "public Builder mergeBsonFrom(org.bson.types.ObjectId value)")) {
      mergeFromObjectId.writeLine(
          "return setObjectId(%s.parseBsonFrom(value).getRep());",
          WellKnownTypes.OBJECT_ID.getMessageClassName());
    }
    try (var parseFromObjectId =
        this.messageOutput.openScope(
            "public static %s parseBsonFrom(org.bson.types.ObjectId value)",
            this.descriptor.getName())) {
      parseFromObjectId.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }

    // Allow parsing and merging from a raw Decimal128.
    try (var mergeFromDecimal128 =
        this.builderOutput.openScope(
            "public Builder mergeBsonFrom(org.bson.types.Decimal128 value)")) {
      mergeFromDecimal128.writeLine(
          "return setDecimal128(%s.parseBsonFrom(value).getRep());",
          WellKnownTypes.DECIMAL128.getMessageClassName());
    }
    try (var parseFromDecimal128 =
        this.messageOutput.openScope(
            "public static %s parseBsonFrom(org.bson.types.Decimal128 value)",
            this.descriptor.getName())) {
      parseFromDecimal128.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }

    try (var mergeBsonFromReader =
        this.builderOutput.openScope(
            "@Override public Builder mergeBsonFrom(org.bson.BsonReader reader)")) {
      generateMergeBsonFrom(mergeBsonFromReader);
    }

    this.messageOutput
        .getRootScope()
        .writeLine(
            "private static java.util.EnumSet<org.bson.BsonType> BSON_VALUE_TYPES ="
                + " java.util.EnumSet.allOf(org.bson.BsonType.class);");
    try (var getBsonValueTypesScope =
        this.messageOutput.openScope(
            "public static java.util.EnumSet<org.bson.BsonType> getBsonValueTypes()")) {
      getBsonValueTypesScope.writeLine("return BSON_VALUE_TYPES;");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }

  private static void generateWriteBsonTo(CodeOutput.Scope fnScope) {
    try (var switchScope = fnScope.openScope("switch (getTypeCase())")) {
      generateWriteMessageCase("DOCUMENT", "getDocument", switchScope);
      generateWriteMessageCase("ARRAY", "getArray", switchScope);
      generateWriteSimpleCase("STRING", "String", switchScope);
      generateWriteMessageCase("BINARY", "getBinary", switchScope);
      generateWriteSimpleCase("BOOL", "getBool()", "writeBoolean", switchScope);
      generateWriteSimpleCase("INT32", "Int32", switchScope);
      generateWriteSimpleCase("INT64", "Int64", switchScope);
      generateWriteSimpleCase("DOUBLE", "Double", switchScope);
      generateWriteSimpleCase("UTC_DATETIME", "getUtcDatetime()", "writeDateTime", switchScope);
      generateWriteSimpleCase(
          "TIMESTAMP", "new org.bson.BsonTimestamp(getTimestamp())", "writeTimestamp", switchScope);

      try (var objectIdScope = switchScope.openScope("case OBJECT_ID:")) {
        // Share the ProtoObjectId WKT implementation. This could be improved by sharing a runtime
        // library rather than allocating a new object.
        objectIdScope.writeLine(
            "%s.newBuilder().setRep(getObjectId()).build().writeBsonTo(writer);",
            WellKnownTypes.OBJECT_ID.getMessageClassName());
        objectIdScope.writeLine("break;");
      }

      try (var caseScope = switchScope.openScope("case VALUELESS:")) {
        try (var valuelessSwitchScope = caseScope.openScope("switch (getValueless())")) {
          generateOneValuelessWriteCase("NULL", "Null", valuelessSwitchScope);
          generateOneValuelessWriteCase("MIN_KEY", "MinKey", valuelessSwitchScope);
          generateOneValuelessWriteCase("MAX_KEY", "MaxKey", valuelessSwitchScope);
          generateOneValuelessWriteCase("UNDEFINED", "Undefined", valuelessSwitchScope);
          try (var unrecognized = valuelessSwitchScope.openScope("case UNRECOGNIZED:")) {
            unrecognized.writeLine(
                "throw new IllegalArgumentException(\"Unknown valueless field type\");");
          }
        }
        caseScope.writeLine("break;");
      }

      try (var decimal128Scope = switchScope.openScope("case DECIMAL128:")) {
        // Share the ProtoDecimal128 WKT implementation. This could be improved by sharing a runtime
        // library rather than allocating a new object.
        decimal128Scope.writeLine(
            "%s.newBuilder().setRep(getDecimal128()).build().writeBsonTo(writer);",
            WellKnownTypes.DECIMAL128.getMessageClassName());
        decimal128Scope.writeLine("break;");
      }

      generateWriteSimpleCase("JAVA_SCRIPT", "JavaScript", switchScope);
      generateWriteSimpleCase("SYMBOL", "Symbol", switchScope);
      generateWriteMessageCase("REGULAR_EXPRESSION", "getRegularExpression", switchScope);
      generateWriteMessageCase("DB_POINTER", "getDbPointer", switchScope);
      generateWriteMessageCase("JAVA_SCRIPT_WITH_SCOPE", "getJavaScriptWithScope", switchScope);

      try (var typeNotSetScope = switchScope.openScope("case TYPE_NOT_SET:")) {
        typeNotSetScope.writeLine(
            "throw new IllegalStateException(\"ProtoValue without any set member.\");");
      }
    }
  }

  private static void generateWriteMessageCase(
      String enumName, String accessor, CodeOutput.Scope switchScope) {
    try (var caseScope = switchScope.openScope("case %s:", enumName)) {
      caseScope.writeLine("%s().writeBsonTo(writer);", accessor);
      caseScope.writeLine("break;");
    }
  }

  private static void generateWriteSimpleCase(
      String enumName, String fieldType, CodeOutput.Scope switchScope) {
    generateWriteSimpleCase(
        enumName,
        String.format("get%s()", fieldType),
        String.format("write%s", fieldType),
        switchScope);
  }

  private static void generateWriteSimpleCase(
      String enumName, String readExpr, String writeMethod, CodeOutput.Scope switchScope) {
    try (var caseScope = switchScope.openScope("case %s:", enumName)) {
      caseScope.writeLine("writer.%s(%s);", writeMethod, readExpr);
      caseScope.writeLine("break;");
    }
  }

  private static void generateOneValuelessWriteCase(
      String caseName, String writeType, CodeOutput.Scope valuelessSwitchScope) {
    try (var caseScope = valuelessSwitchScope.openScope("case %s:", caseName)) {
      caseScope.writeLine("writer.write%s();", writeType);
      caseScope.writeLine("break;");
    }
  }

  private static void generateToBson(CodeOutput.Scope fnScope) {
    fnScope.writeLine("var writer = new org.bson.BsonDocumentWriter(new org.bson.BsonDocument());");
    fnScope.writeLine("writer.writeStartDocument();");
    fnScope.writeLine("writer.writeName(\"sentinel\");");
    fnScope.writeLine("writeBsonTo(writer);");
    fnScope.writeLine("return writer.getDocument().get(\"sentinel\");");
  }

  private static void generateMergeBsonFrom(CodeOutput.Scope fnScope) {
    try (var switchScope = fnScope.openScope("switch (reader.getCurrentBsonType())")) {
      generateMergeMessageCase(BsonType.DOCUMENT, "Document", switchScope);
      generateMergeMessageCase(BsonType.ARRAY, "Array", switchScope);
      generateMergeSimpleCase(BsonType.STRING, "String", switchScope);
      generateMergeMessageCase(BsonType.BINARY, "Binary", switchScope);
      generateMergeSimpleCase(BsonType.BOOLEAN, "Bool", "reader.readBoolean()", switchScope);
      generateMergeSimpleCase(BsonType.INT32, "Int32", switchScope);
      generateMergeSimpleCase(BsonType.INT64, "Int64", switchScope);
      generateMergeSimpleCase(BsonType.DOUBLE, "Double", switchScope);
      generateMergeSimpleCase(
          BsonType.DATE_TIME, "UtcDatetime", "reader.readDateTime()", switchScope);
      generateMergeSimpleCase(
          BsonType.TIMESTAMP, "Timestamp", "reader.readTimestamp().getValue()", switchScope);

      try (var objectIdScope = switchScope.openScope("case %s:", BsonType.OBJECT_ID)) {
        // Share the ProtoObjectId WKT implementation. This could be improved by sharing a runtime
        // library rather than allocating a new object.
        objectIdScope.writeLine(
            "setObjectId(%s.parseBsonFrom(reader.readObjectId()).getRep());",
            WellKnownTypes.OBJECT_ID.getMessageClassName());
        objectIdScope.writeLine("return this;");
      }

      generateMergeValuelessCase(BsonType.NULL, "Null", switchScope);
      generateMergeValuelessCase(BsonType.MIN_KEY, "MinKey", switchScope);
      generateMergeValuelessCase(BsonType.MAX_KEY, "MaxKey", switchScope);
      generateMergeValuelessCase(BsonType.UNDEFINED, "Undefined", switchScope);

      try (var decimal128Scope = switchScope.openScope("case %s:", BsonType.DECIMAL128)) {
        // Share the ProtoDecimal128 WKT implementation. This could be improved by sharing a runtime
        // library rather than allocating a new object.
        decimal128Scope.writeLine(
            "setDecimal128(%s.parseBsonFrom(reader.readDecimal128()).getRep());",
            WellKnownTypes.DECIMAL128.getMessageClassName());
        decimal128Scope.writeLine("return this;");
      }

      generateMergeMessageCase(BsonType.REGULAR_EXPRESSION, "RegularExpression", switchScope);
      generateMergeSimpleCase(BsonType.JAVASCRIPT, "JavaScript", switchScope);
      generateMergeSimpleCase(BsonType.SYMBOL, "Symbol", switchScope);
      generateMergeMessageCase(BsonType.DB_POINTER, "DbPointer", switchScope);
      generateMergeMessageCase(BsonType.JAVASCRIPT_WITH_SCOPE, "JavaScriptWithScope", switchScope);

      try (var endOfDocument = switchScope.openScope("case %s:", BsonType.END_OF_DOCUMENT)) {
        endOfDocument.writeLine(
            "throw new java.lang.IllegalStateException(\"reading bson value but received"
                + " END_OF_DOCUMENT\");");
      }
    }
    fnScope.writeLine("throw new AssertionError(\"unreachable\");");
  }

  private static void generateMergeMessageCase(
      BsonType bsonType, String fieldType, CodeOutput.Scope switchScope) {
    try (var caseScope = switchScope.openScope("case %s:", bsonType)) {
      caseScope.writeLine("get%sBuilder().mergeBsonFromTypeValidated(reader);", fieldType);
      caseScope.writeLine("return this;");
    }
  }

  private static void generateMergeSimpleCase(
      BsonType bsonType, String fieldName, CodeOutput.Scope switchScope) {
    generateMergeSimpleCase(
        bsonType, fieldName, String.format("reader.read%s()", fieldName), switchScope);
  }

  private static void generateMergeSimpleCase(
      BsonType bsonType, String fieldName, String readExpr, CodeOutput.Scope switchScope) {
    try (var caseScope = switchScope.openScope("case %s:", bsonType)) {
      caseScope.writeLine("set%s(%s);", fieldName, readExpr);
      caseScope.writeLine("return this;");
    }
  }

  private static void generateMergeValuelessCase(
      BsonType bsonType, String fieldName, CodeOutput.Scope switchScope) {
    try (var caseScope = switchScope.openScope("case %s:", bsonType)) {
      caseScope.writeLine("reader.read%s();", fieldName);
      caseScope.writeLine("setValueless(ProtoValueless.%s);", bsonType);
      caseScope.writeLine("return this;");
    }
  }

  private static void generateMergeBsonValue(CodeOutput.Scope fnScope) {
    fnScope.writeLine("var doc = new org.bson.BsonDocument(\"dummy\", value);");
    fnScope.writeLine("var reader = new org.bson.BsonDocumentReader(doc);");
    fnScope.writeLine("reader.readStartDocument();");
    fnScope.writeLine("reader.readName();");
    fnScope.writeLine("return mergeBsonFrom(reader);");
  }
}
