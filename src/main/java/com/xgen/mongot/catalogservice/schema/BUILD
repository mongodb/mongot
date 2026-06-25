load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "schema",
    srcs = [
        "AuthoritativeIndexKey.java",
        "IndexEntry.java",
        "ServerStateEntry.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
