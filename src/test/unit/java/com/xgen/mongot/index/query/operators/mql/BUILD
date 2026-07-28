load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "mql",
    srcs = [
        "ClauseTest.java",
        "ClauseTests.java",
        "ClosedRangeBoundComparisonOperatorTest.java",
        "ComparisonOperatorTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/operators/mql:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/operators/value",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
