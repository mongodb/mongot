load("@rules_java//java:java_binary.bzl", "java_binary")
load("@rules_java//java:java_library.bzl", "java_library")
load("@rules_jvm_external//:defs.bzl", "artifact")
load("//bazel/java:proto_bson.bzl", "java_proto_bson_toolchain")

java_binary(
    name = "JavaBsonProtoPlugin",
    srcs = glob(["*.java"]),
    deps = [
        artifact("org.mongodb:bson"),
        "//src/main/java/com/xgen/proto:options_java_proto",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_protobuf//:protobuf_java_util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_jetbrains_annotations",
    ],
)

java_library(
    name = "bson_proto_runtime",
    exports = [
        artifact("org.mongodb:bson"),
        "//src/main/java/com/xgen/proto",
        "@com_google_protobuf//:protobuf_java",
    ],
)

java_proto_bson_toolchain(
    name = "java_proto_bson_library_toolchain",
    plugin = ":JavaBsonProtoPlugin",
    runtime = [":bson_proto_runtime"],
    visibility = ["//visibility:public"],
)
