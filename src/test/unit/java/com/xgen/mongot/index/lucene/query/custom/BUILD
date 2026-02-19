load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "custom",
    srcs = [
        "ExactVectorSearchQueryTest.java",
        "MongotKnnQueryMetricsTest.java",
        "WrappedKnnQueryTest.java",
        "WrappedQueryTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/query/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/query/util",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/version",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
