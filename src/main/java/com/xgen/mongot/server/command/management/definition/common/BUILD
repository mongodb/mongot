load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "common",
    srcs = [
        "CommonDefinitions.java",
        "NamedSearchIndex.java",
        "NamedSearchIndexId.java",
        "UserIndexDefinition.java",
        "UserSearchIndexDefinition.java",
        "UserVectorIndexDefinition.java",
        "UserViewDefinition.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
