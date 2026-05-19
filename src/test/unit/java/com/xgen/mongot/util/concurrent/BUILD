load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "concurrent",
    srcs = [
        "ExecutorsTest.java",
        "FileLockGuardTest.java",
        "LiveThreadIdsRegistryTest.java",
        "LockGuardTest.java",
        "MeteredCallerRunsPolicyTest.java",
        "OneShotSingleThreadExecutorTest.java",
        "RecurringTimerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
    ],
)
