load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "monitor",
    srcs = [
        "PeriodicDiskMonitorTest.java",
        "ReplicationStateMonitorTest.java",
        "SizeBasedDiskMonitorTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/monitor",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
