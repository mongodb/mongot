load("//bazel/java:package.bzl", "java_library")

# TODO: remove ignores and exclusions to fully adopt gazelle.
# gazelle:exclude parser

java_library(
    name = "trace",
    srcs = [
        "LatencySamplingSpanProcessor.java",
        "ParentScopeGuard.java",
        "Slf4jExporter.java",
        "SpanGuard.java",
        "ToggleSampler.java",
        "Tracing.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:io_opentelemetry_opentelemetry_api",
        "@maven//:io_opentelemetry_opentelemetry_sdk_common",
        "@maven//:io_opentelemetry_opentelemetry_sdk_trace",
    ],
    deps = [
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_opentelemetry_opentelemetry_api",
        "@maven//:io_opentelemetry_opentelemetry_context",
        "@maven//:io_opentelemetry_opentelemetry_exporter_otlp_common",
        "@maven//:io_opentelemetry_opentelemetry_sdk",
        "@maven//:io_opentelemetry_opentelemetry_sdk_common",
        "@maven//:io_opentelemetry_opentelemetry_sdk_trace",
        "@maven//:io_opentelemetry_opentelemetry_semconv",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
