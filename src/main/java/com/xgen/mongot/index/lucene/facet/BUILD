load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "facet",
    srcs = [
        "AbstractSortedSetDocValuesFacetCounts.java",
        "ConcurrentSortedSetDocValuesFacetCounts.java",
        "ParentDedupedSortedSetDocValuesFacetCounts.java",
        "SortedSetDocValuesFacetCounts.java",
        "TokenFacetsCardinalityLimitExceededException.java",
        "TokenFacetsStateCache.java",
        "TokenSsdvFacetState.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
        "@maven//:org_apache_lucene_lucene_join",
        "@maven//:org_jetbrains_annotations",
    ],
)
