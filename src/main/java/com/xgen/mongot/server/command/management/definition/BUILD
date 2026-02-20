load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "definition",
    srcs = [
        "CreateSearchIndexesCommandDefinition.java",
        "DropSearchIndexCommandDefinition.java",
        "ListSearchIndexesCommandDefinition.java",
        "ListSearchIndexesResponseDefinition.java",
        "ManageSearchIndexCommandDefinition.java",
        "SearchIndexCommandDefinition.java",
        "UpdateSearchIndexCommandDefinition.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor/serialization",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mongodb_bson",
    ],
)
