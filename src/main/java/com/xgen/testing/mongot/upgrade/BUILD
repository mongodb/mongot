load("//bazel/java:package.bzl", "java_library")

# gazelle:ignore
# This test-only library depends on the test serialization package.

java_library(
    name = "upgrade_index_names",
    testonly = True,
    srcs = ["UpgradeTestIndexNames.java"],
    visibility = ["//visibility:public"],
    deps = ["//src/main/java/com/xgen/testing/mongot/integration/index/serialization"],
)
