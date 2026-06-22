package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoDecimal128CodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoDecimal128CodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var bytesToDecimal128Scope =
        this.messageOutput.openScope(
            "static org.bson.types.Decimal128 bytesToDecimal128(com.google.protobuf.ByteString"
                + " bytes)")) {
      bytesToDecimal128Scope.writeLine(
          "var buf = bytes.asReadOnlyByteBuffer().order(java.nio.ByteOrder.LITTLE_ENDIAN);");
      bytesToDecimal128Scope.writeLine("var lo = buf.getLong();");
      bytesToDecimal128Scope.writeLine("var hi = buf.getLong();");
      bytesToDecimal128Scope.writeLine(
          "return org.bson.types.Decimal128.fromIEEE754BIDEncoding(hi, lo);");
    }

    try (var toDecimal128Scope =
        this.messageOutput.openScope("public org.bson.types.Decimal128 toDecimal128()")) {
      toDecimal128Scope.writeLine("return bytesToDecimal128(getRep());");
    }

    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeDecimal128(toDecimal128());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonDecimal128 toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonDecimal128(toDecimal128());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.DECIMAL128, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.DECIMAL128, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return mergeBsonFrom(reader.readDecimal128());");
    }

    try (var mergeBsonValueScope =
        this.builderOutput.openScope(
            "public Builder mergeBsonFrom(org.bson.types.Decimal128 value)")) {
      mergeBsonValueScope.writeLine(
          "var buf = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);");
      mergeBsonValueScope.writeLine(
          "var encoded = buf.putLong(value.getLow()).putLong(value.getHigh()).rewind();");
      mergeBsonValueScope.writeLine(
          "return setRep(com.google.protobuf.ByteString.copyFrom(encoded));");
    }

    try (var parseBsonValue =
        this.messageOutput.openScope(
            "public static %s parseBsonFrom(org.bson.types.Decimal128 value)",
            this.descriptor.getName())) {
      parseBsonValue.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.DECIMAL128, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return mergeBsonFrom(value.getValue());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
