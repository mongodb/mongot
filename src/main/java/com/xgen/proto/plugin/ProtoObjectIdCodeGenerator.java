package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoObjectIdCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoObjectIdCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var toObjectIdScope =
        this.messageOutput.openScope("public org.bson.types.ObjectId toObjectId()")) {
      toObjectIdScope.writeLine("return new org.bson.types.ObjectId(getRep().toByteArray());");
    }

    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeObjectId(toObjectId());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonObjectId toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonObjectId(toObjectId());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.OBJECT_ID, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.OBJECT_ID, this.builderOutput)) {
      mergeBsonFromScope.writeLine(
          "return"
              + " setRep(com.google.protobuf.ByteString.copyFrom(reader.readObjectId().toByteArray()));");
    }

    try (var mergeBsonValue =
        this.builderOutput.openScope(
            "public Builder mergeBsonFrom(org.bson.types.ObjectId value)")) {
      mergeBsonValue.writeLine(
          "return setRep(com.google.protobuf.ByteString.copyFrom(value.toByteArray()));");
    }

    try (var parseBsonValue =
        this.messageOutput.openScope(
            "public static %s parseBsonFrom(org.bson.types.ObjectId value)",
            this.descriptor.getName())) {
      parseBsonValue.writeLine("return newBuilder().mergeBsonFrom(value).build();");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.OBJECT_ID, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return mergeBsonFrom(value.getValue());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
