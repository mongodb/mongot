load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "dynamic",
    srcs = [
        "DynamicFeatureFlagConfig.java",
        "DynamicFeatureFlagRegistry.java",
        "DynamicFeatureFlags.java",
        "EvaluateDynamicFeatureFlagContext.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/explainers",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_flogger_flogger",
        "@maven//:org_mongodb_bson",
    ],
)
