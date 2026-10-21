load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "comparator",
    srcs = [
        "DoubleFieldConverterTest.java",
        "InstrumentedChildBitSetProducerTest.java",
        "MqlLongComparatorTest.java",
        "ScopeFiltersTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort/comparator",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_join",
        "@maven//:org_mongodb_bson",
    ],
)
