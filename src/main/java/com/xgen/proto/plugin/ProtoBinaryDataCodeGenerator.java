package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoBinaryDataCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoBinaryDataCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeBinaryData(toBson());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonBinary toBson()")) {
      toBsonScope.writeLine(
          "return new org.bson.BsonBinary((byte)(getSubTypeValue() & 0xff),"
              + " getData().toByteArray());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(BsonType.BINARY, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.BINARY, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return mergeBsonFrom(reader.readBinaryData());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.BINARY, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("setSubTypeValue(java.lang.Byte.toUnsignedInt(value.getType()));");
      mergeBsonValue.writeLine(
          "return setData(com.google.protobuf.ByteString.copyFrom(value.getData()));");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
