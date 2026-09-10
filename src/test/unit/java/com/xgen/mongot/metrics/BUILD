load("//bazel/java:package.bzl", "java_library")
load("//bazel/java:test.bzl", "java_test_suite")

# Library target for tests that need to use ServerStatusDataExtractorTest utilities
java_library(
    name = "lib",
    testonly = True,
    srcs = [
        "CachedGaugeTest.java",
        "MetricsFactoryTest.java",
        "PerIndexMetricsFactoryTest.java",
        "ServerStatusDataExtractorTest.java",
        "TimerTest.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/metrics/micrometer",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test_suite(
    name = "metrics",
    srcs = [
        "BuildInfoMetricsTest.java",
        "CachedGaugeTest.java",
        "MetricsFactoryTest.java",
        "PerIndexMetricsFactoryTest.java",
        "ServerStatusDataExtractorTest.java",
        "SubsystemResourceHistoryTest.java",
        "ThreadPoolResourceMetricsTest.java",
        "TimerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/metrics/micrometer",
        "@maven//:com_google_flogger_flogger_system_backend",  # keep
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
    ],
)
