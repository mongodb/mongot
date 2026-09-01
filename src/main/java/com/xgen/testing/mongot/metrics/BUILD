load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "metrics",
    testonly = True,
    srcs = [
        "CounterSubjects.java",
        "MeterRef.java",
        "MeterSubject.java",
        "MultiCounterDefinedTags.java",
        "MultiCounterEmptyTags.java",
        "MultiCounterSubject.java",
        "SimpleMetricsFactory.java",
        "SingleCounterDefinedTags.java",
        "SingleCounterEmptyTags.java",
        "SingleCounterSubject.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:io_micrometer_micrometer_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
    ],
)
