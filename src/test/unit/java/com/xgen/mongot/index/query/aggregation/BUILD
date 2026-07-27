load("//bazel/java:package.bzl", "java_library")
load("//bazel/java:test.bzl", "java_test_suite")

# gazelle:exclude TestOperatorSpecs.java

java_library(
    name = "planner_test_support",
    testonly = True,
    srcs = ["TestOperatorSpecs.java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/aggregation",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)

java_test_suite(
    name = "aggregation",
    srcs = [
        "AccumulatorTest.java",
        "AggregationIntegrationTest.java",
        "AggregationRegistryTest.java",
        "AggregationTestSupport.java",
        "ApplyLimitTest.java",
        "AvgAccumulatorTest.java",
        "AvgAggregationResultTest.java",
        "BoundaryAggregationTest.java",
        "BoundaryEncoderTest.java",
        "IntermediateBsonTest.java",
        "NumericMetricValuesTest.java",
        "PrimitiveArrayTest.java",
        "ResultTest.java",
        "SearchAggregationBootstrapTest.java",
        "ShardMergePipelineBuilderTest.java",
        "SpecTest.java",
        "SumAccumulatorTest.java",
        "SumAvgPartitionMergeTest.java",
        "TermsAccumulatorTest.java",
    ],
    tags = ["unit"],
    deps = [
        ":planner_test_support",  # keep
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/aggregation",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
