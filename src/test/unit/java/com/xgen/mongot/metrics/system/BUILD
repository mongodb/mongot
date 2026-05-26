load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "system",
    srcs = [
        "CpuInfoMetricsTest.java",
        "MemoryMetricsTest.java",
        "NumaInfoMetricsTest.java",
        "ProcessMetricsTest.java",
        "SystemMetricsInstrumentationTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics/system",
        "@maven//:com_github_oshi_oshi_common",
        "@maven//:com_google_flogger_flogger_system_backend",  # keep
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
