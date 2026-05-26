load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "system",
    srcs = [
        "CpuInfoMetrics.java",
        "DiskMetrics.java",
        "MemoryMetrics.java",
        "NetstatMetrics.java",
        "NumaInfoMetrics.java",
        "ProcessMetrics.java",
        "SystemMetricsInstrumentation.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_github_oshi_oshi_common",
        "@maven//:com_github_oshi_oshi_core",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
