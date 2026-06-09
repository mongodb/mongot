load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "searcher",
    srcs = [
        "ConcurrentIndexSearcher.java",
        "FieldToSortableTypesMapping.java",
        "LuceneIndexSearcher.java",
        "LuceneSearcherFactory.java",
        "LuceneSearcherManager.java",
        "QueryCacheProvider.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/facet",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/codec/bloom",
        "//src/main/java/com/xgen/mongot/index/lucene/facet",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/similarity",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
