load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "rules",
    srcs = [
        "DefaultRuleRegistryTest.java",
        "RangeBoundsMergeMqlAndClauseRuleTest.java",
        "RangeBoundsMergeMqlSimpleClauseRuleTest.java",
        "RangeBoundsMergeOperatorRuleTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/query/context",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/operators/type",
        "//src/main/java/com/xgen/mongot/index/query/optimizer",
        "//src/main/java/com/xgen/mongot/index/query/optimizer/rules",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/index/query/scores",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
