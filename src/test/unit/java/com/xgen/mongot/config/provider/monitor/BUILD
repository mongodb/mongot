load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "monitor",
    srcs = ["PeriodicConfigMonitorTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/provider/monitor",
        "//src/main/java/com/xgen/mongot/config/updater",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
