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
        "CountAccumulatorTest.java",
        "DocValuesFieldReaderTest.java",
        "FieldValueFetcherTest.java",
        "FirstLastAccumulatorTest.java",
        "FirstLastShardMergeTest.java",
        "IntermediateBsonTest.java",
        "MetricFieldPathsTest.java",
        "MinMaxAccumulatorTest.java",
        "MinMaxSelectionTest.java",
        "NumericMetricValuesTest.java",
        "PrimitiveArrayTest.java",
        "ResultTest.java",
        "SearchAggregationBootstrapTest.java",
        "ShardMergeAliasPlanTest.java",
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
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort",
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort/mixed",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/aggregation",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
