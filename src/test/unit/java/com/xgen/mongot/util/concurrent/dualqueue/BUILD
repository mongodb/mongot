load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "dualqueue",
    srcs = [
        "DualBlockingQueueTest.java",
        "DualQueueExecutorTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/concurrent/dualqueue",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
    ],
)
