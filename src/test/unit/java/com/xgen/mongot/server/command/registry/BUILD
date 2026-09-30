load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "registry",
    srcs = ["CommandRegistryTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command/builtin",
        "//src/main/java/com/xgen/mongot/server/command/registry",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
    ],
)
