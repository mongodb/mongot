load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "index",
    srcs = ["OnDiskIndexSizeSampler.java"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "@maven//:com_google_errorprone_error_prone_annotations",
    ],
)
