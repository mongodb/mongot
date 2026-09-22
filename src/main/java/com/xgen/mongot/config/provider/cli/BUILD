load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "cli",
    srcs = ["CliOptionRegistry.java"],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
