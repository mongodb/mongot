load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "mixed",
    srcs = [
        "CompositeComparatorTest.java",
        "CompositeLeafComparatorTest.java",
        "MixedBooleanLeafComparatorTest.java",
        "MixedNullLeafComparatorTest.java",
        "MixedNumericLeafComparatorTest.java",
        "MixedOrdinalLeafComparatorTest.java",
        "MqlMixedSortTest.java",
        "ScopedMqlMixedSortTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort/mixed",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/util",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_join",
        "@maven//:org_apache_lucene_lucene_test_framework",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
