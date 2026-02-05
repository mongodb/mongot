load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "timers",
    srcs = [
        "CpuTickerTest.java",
        "SamplingInvocationCountingTimerTest.java",
        "ThreadSafeInvocationCountingTimerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/timers",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
    ],
)
