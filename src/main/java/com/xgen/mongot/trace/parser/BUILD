load("@rules_java//java:java_binary.bzl", "java_binary")
load("//bazel/java:package.bzl", "mongot_java_package")

mongot_java_package(
    name = "lib",
    testonly = True,
    deps = [
        "com.fasterxml.jackson.core:jackson-annotations",
        "com.fasterxml.jackson.core:jackson-databind",
        "com.google.errorprone:error_prone_annotations",
        "com.google.guava:guava",
        "com.googlecode.json-simple:json-simple",
    ],
)

java_binary(
    name = "parser",
    testonly = True,
    main_class = "com.xgen.mongot.trace.parser.JsonConvertor",
    visibility = ["//visibility:public"],
    runtime_deps = ["//src/main/java/com/xgen/mongot/trace/parser:lib"],
)
