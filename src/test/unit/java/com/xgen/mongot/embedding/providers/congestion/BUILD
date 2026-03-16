load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "congestion",
    srcs = [
        "AimdCongestionControlTest.java",
        "DynamicSemaphoreTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding/providers/congestion",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:junit_junit",
    ],
)
