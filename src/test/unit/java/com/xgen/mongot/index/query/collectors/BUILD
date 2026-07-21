load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "collectors",
    srcs = [
        "AggregationCollectorDeserializationTest.java",
        "AggregationCollectorParseTest.java",
        "AggregationSpecBuilderTest.java",
        "CollectorTest.java",
        "DrillSidewaysInfoBuilderTest.java",
        "FacetCollectorTest.java",
        "GenericDrillSidewaysFacetOperatorBuilderTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/collectors:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/aggregation",
        "//src/main/java/com/xgen/mongot/index/query/collectors",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/value",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/index/query/scores",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/collectors",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "//src/test/unit/java/com/xgen/mongot/index/query/aggregation:planner_test_support",  # keep
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
