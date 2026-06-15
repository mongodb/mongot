load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "featureflag",
    srcs = [
        "Feature.java",
        "FeatureFlags.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
