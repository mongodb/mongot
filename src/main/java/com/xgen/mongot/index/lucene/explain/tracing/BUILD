load("//bazel/java:package.bzl", "java_library")

"""
   Since `Explain.java` is designed to be used anywhere in the codebase, be very stingy with
   defining dependencies here in order to not create dependency cycles.
"""

java_library(
    name = "tracing",
    srcs = [
        "Explain.java",
        "ExplainQueryState.java",
        "ExplainTooLargeException.java",
        "FeatureExplainer.java",
        "FeatureExplainerEmitter.java",
        "ResourceUsageFeatureExplainer.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "@maven//:io_opentelemetry_opentelemetry_context",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_github_oshi_oshi_common",
        "@maven//:com_github_oshi_oshi_core",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_opentelemetry_opentelemetry_context",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
