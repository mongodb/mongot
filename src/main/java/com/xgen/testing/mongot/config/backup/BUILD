load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "backup",
    testonly = True,
    srcs = [
        "ConfigJournalV1Builder.java",
        "QuarantinedIndexBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/config/backup",
        "//src/main/java/com/xgen/mongot/config/manager",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/config/backup",
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/index/definition",
        "@maven//:org_mongodb_bson",
    ],
)
