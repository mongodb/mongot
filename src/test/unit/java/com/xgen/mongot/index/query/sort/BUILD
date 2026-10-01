load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "sort",
    srcs = [
        "MongotSortFieldTest.java",
        "NullEmptySortPositionTest.java",
        "SequenceTokenTest.java",
        "SortBetaV1Test.java",
        "SortOptionsTest.java",
        "SortTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/sort:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/sort",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_math3",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
