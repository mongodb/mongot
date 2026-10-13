load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "parser",
    srcs = [
        "PathField.java",
        "TokenEndpointField.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
