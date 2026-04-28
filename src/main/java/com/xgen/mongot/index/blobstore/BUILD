load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "blobstore",
    srcs = [
        "BlobstoreSnapshotterManager.java",
        "IndexBlobstoreSnapshotter.java",
        "IndexSnapshotStatus.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/blobstore",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
