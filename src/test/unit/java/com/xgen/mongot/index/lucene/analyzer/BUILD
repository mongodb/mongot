load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# This macro owns its test targets; Gazelle must not generate a second suite for the same sources.
# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/analyzer",
        "//src/main/java/com/xgen/mongot/util/regex",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
    ],
)
