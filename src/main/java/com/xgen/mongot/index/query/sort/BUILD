load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "sort",
    srcs = [
        "EmbeddedSortScope.java",
        "MetaSortField.java",
        "MetaSortOptions.java",
        "MongotSortField.java",
        "NullEmptySortPosition.java",
        "SequenceToken.java",
        "Sort.java",
        "SortBetaV1.java",
        "SortOptions.java",
        "SortOrder.java",
        "SortSelector.java",
        "SortSpec.java",
        "UserFieldSortOptions.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/proto/bson:types_java_proto",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
