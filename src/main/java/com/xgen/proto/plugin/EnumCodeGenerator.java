package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.EnumDescriptor;
import static com.google.protobuf.Descriptors.EnumValueDescriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import java.util.List;

public class EnumCodeGenerator implements CodeGenerator {
  private static final Converter<String, String> BSON_ENUM_CONVERTER =
      CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL);

  private final EnumDescriptor descriptor;
  private final CodeOutput output = new CodeOutput();

  public EnumCodeGenerator(EnumDescriptor enumDescriptor) {
    this.descriptor = enumDescriptor;
  }

  @Override
  public Iterable<CodeGeneratorResponse.File> generate() {
    try (var fnScope = this.output.openScope("public String toBsonString()")) {
      generateToBsonString(fnScope);
    }
    try (var fnScope =
        this.output.openScope(
            "public static %s fromBsonString(String value) throws"
                + " com.xgen.proto.BsonProtoParseException",
            this.descriptor.getName())) {
      generateFromBsonString(fnScope);
    }
    return List.of(this.output.buildResponse(this.descriptor));
  }

  private void generateToBsonString(CodeOutput.Scope fnScope) {
    try (var switchScope = fnScope.openScope("switch (this)")) {
      for (EnumValueDescriptor valueDescriptor : this.descriptor.getValues()) {
        String enumName = valueDescriptor.getName();
        try (var caseScope = switchScope.openScope("case %s:", enumName)) {
          caseScope.writeLine("return \"%s\";", BSON_ENUM_CONVERTER.convert(enumName));
        }
      }
      try (var defaultCase = switchScope.openScope("default:")) {
        defaultCase.writeLine(
            "throw new java.lang.IllegalStateException(\"enum with unrecognized value\");");
      }
    }
  }

  private void generateFromBsonString(CodeOutput.Scope fnScope) {
    try (var switchScope = fnScope.openScope("switch(value)")) {
      for (EnumValueDescriptor valueDescriptor : this.descriptor.getValues()) {
        String enumName = valueDescriptor.getName();
        try (var caseScope =
            switchScope.openScope("case \"%s\":", BSON_ENUM_CONVERTER.convert(enumName))) {
          caseScope.writeLine("return %s;", enumName);
        }
      }
      try (var defaultCase = switchScope.openScope("default:")) {
        defaultCase.writeLine(
            "throw new com.xgen.proto.BsonProtoParseException(String.format(\"unknown enum value"
                + " %s\", value));");
      }
    }
  }
}
