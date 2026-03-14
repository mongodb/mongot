load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "featureflag",
    srcs = [
        "DynamicFeatureFlagConfigTest.java",
        "DynamicFeatureFlagRegistryTest.java",
    ],
    data = ["//src/test/unit/resources/featureflag:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
