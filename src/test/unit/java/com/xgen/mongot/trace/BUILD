load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "trace",
    srcs = [
        "ExporterTest.java",
        "GatedSpanExporterTest.java",
        "LatencySamplingSpanProcessorTest.java",
        "TracingTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/trace",
        "//src/main/java/com/xgen/testing",
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_truth_truth",
        "@maven//:io_opentelemetry_opentelemetry_api",
        "@maven//:io_opentelemetry_opentelemetry_context",
        "@maven//:io_opentelemetry_opentelemetry_sdk_common",
        "@maven//:io_opentelemetry_opentelemetry_sdk_trace",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
