load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "request",
    srcs = [
        "BatchOptionsDefinition.java",
        "CursorOptionsDefinition.java",
        "ExplainDefinition.java",
        "GetMoreCommandDefinition.java",
        "KillCursorsCommandDefinition.java",
        "MaterializeVectorSearchCommandDefinition.java",
        "OptimizationFlagsDefinition.java",
        "PlanShardedSearchCommandDefinition.java",
        "SearchCommandDefinition.java",
        "VectorSearchCommandDefinition.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/cursor/batch",
        "//src/main/java/com/xgen/mongot/index/query",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor/batch",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
