load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "explainers",
    srcs = [
        "AggregationFeatureExplainer.java",
        "CollectorTimingFeatureExplainer.java",
        "DynamicFeatureFlagFeatureExplainer.java",
        "FacetFeatureExplainer.java",
        "HighlightFeatureExplainer.java",
        "MetadataFeatureExplainer.java",
        "OptimizerFeatureExplainer.java",
        "ResultMaterializationFeatureExplainer.java",
        "SortFeatureExplainer.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/profiler",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/util",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information/creator",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/profiler",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/query/highlights",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/server/command/search/definition/request",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger_system_backend",  # keep
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
        "@maven//:org_apache_lucene_lucene_highlighter",
    ],
)
