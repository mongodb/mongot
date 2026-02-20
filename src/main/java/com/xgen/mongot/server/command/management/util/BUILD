load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "util",
    srcs = [
        "IndexEntryMapper.java",
        "IndexMapper.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/catalogservice",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
