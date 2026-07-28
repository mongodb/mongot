load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "match",
    srcs = [
        "DocumentMatcherTest.java",
        "MatchStageTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/pushdown/match",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
