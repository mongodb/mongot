load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "facet",
    srcs = [
        "ParentDedupedLongRangeFacetCountsTest.java",
        "ParentDedupedSortedSetDocValuesFacetCountsTest.java",
        "TokenFacetOrdinalMapMismatchTest.java",
        "TokenSsdvFacetStateTest.java",
        "TokensFacetStateCacheTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/facet",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
        "@maven//:org_apache_lucene_lucene_join",
    ],
)
