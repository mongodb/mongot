load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "vector",
    testonly = True,
    srcs = [
        "LucenePinCanary.java",
        "MongotBaseKnnVectorsFormatTestCase.java",
    ],
    # LucenePinCanary reads the pinned Lucene version out of this file at runtime.
    data = ["//bazel/java:maven_search_query.MODULE.bazel"],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
    ],
)
