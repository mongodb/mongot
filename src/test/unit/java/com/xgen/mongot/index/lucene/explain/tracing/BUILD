load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "tracing",
    srcs = [
        "ExplainQueryStateTest.java",
        "ExplainTest.java",
        "ResourceUsageFeatureExplainerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/explain/tracing",
        "@maven//:com_github_oshi_oshi_common",
        "@maven//:com_github_oshi_oshi_core",
        "@maven//:com_google_truth_truth",
        "@maven//:io_opentelemetry_opentelemetry_context",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
