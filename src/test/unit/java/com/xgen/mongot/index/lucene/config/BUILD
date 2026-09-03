load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "config",
    srcs = ["LuceneConfigTest.java"],
    data = ["//src/test/unit/resources/index/lucene/config:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/config",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/util",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
    ],
)
