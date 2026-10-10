load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# This macro owns its test targets; Gazelle must not generate a second suite for the same sources.
# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "//src/main/java/com/xgen/mongot/util/regex",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
    ],
)
