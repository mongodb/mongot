load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "logging",
    srcs = [
        "DefaultKeyValueLoggerTest.java",
        "FloggerTest.java",
        "LoggingConfigTest.java",
        "LoggingServerContextTest.java",
        "OmitEmptyFieldJsonDecoratorTest.java",
        "ProductionLogbackConfigTest.java",
        "SafeKeyValuePairsJsonProviderTest.java",
    ],
    data = ["//conf/mongot/production:conf"],
    tags = ["unit"],
    runtime_deps = [
        "@maven//:com_google_flogger_flogger_slf4j_backend",  #keep
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/logging",
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:ch_qos_logback_logback_core",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:net_logstash_logback_logstash_logback_encoder",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
