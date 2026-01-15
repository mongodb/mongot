load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "config",
    srcs = [
        "InitialSyncConfig.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
