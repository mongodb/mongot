load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "featureflag",
    srcs = [
        "FeatureFlagsTest.java",
    ],
    data = ["//src/test/unit/resources/featureflag:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
    ],
)
