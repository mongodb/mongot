load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "aggregation",
    srcs = [
        "AccumulatorTest.java",
        "AggregationRegistryTest.java",
        "AggregationTestSupport.java",
        "PrimitiveArrayTest.java",
        "ResultTest.java",
        "ShardMergePipelineBuilderTest.java",
        "SpecTest.java",
        "TermsAccumulatorTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/query/aggregation",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
