load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "cache",
    srcs = [
        "IntervalThrottleTest.java",
        "ScrapeCacheTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics/cache",
        "//src/main/java/com/xgen/testing/util",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
        "@maven//:junit_junit",
    ],
)
