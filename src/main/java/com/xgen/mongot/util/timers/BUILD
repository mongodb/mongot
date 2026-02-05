load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "timers",
    srcs = [
        "CpuTicker.java",
        "InvocationCountingTimer.java",
        "SamplingInvocationCountingTimer.java",
        "ThreadSafeInvocationCountingTimer.java",
        "TimingData.java",
    ],
    javacopts = ["--enable-preview"],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
    ],
)
