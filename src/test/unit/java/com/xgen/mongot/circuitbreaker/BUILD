load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "circuitbreaker",
    srcs = [
        "CircuitBreakerModeTest.java",
        "HeapLimitTrackerTest.java",
        "QueryFootprintTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/circuitbreaker",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
    ],
)
