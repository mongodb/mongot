load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "metrics",
    srcs = [
        "BuildInfoMetrics.java",
        "CachedGauge.java",
        "Instrumentation.java",
        "MeterAndFtdcRegistry.java",
        "MetricsFactory.java",
        "PerIndexMetricsFactory.java",
        "ServerStatusDataExtractor.java",
        "SubsystemResourceHistory.java",
        "ThreadPoolResourceMetrics.java",
        "Timed.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:io_micrometer_micrometer_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/metrics/micrometer",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
