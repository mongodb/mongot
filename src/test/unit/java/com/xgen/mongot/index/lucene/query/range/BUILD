load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "range",
    srcs = [
        "RangeQueryUtilsTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/query/range",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
